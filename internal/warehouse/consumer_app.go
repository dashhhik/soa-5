package warehouse

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
)

type ConsumerConfig struct {
	HTTPAddr          string
	KafkaBrokers      []string
	KafkaTopic        string
	DLQTopic          string
	GroupID           string
	SchemaRegistryURL string
	CassandraHosts    []string
	CassandraKeyspace string
}

type ConsumerApp struct {
	cfg              ConsumerConfig
	reader           *kafka.Reader
	codec            *AvroCodec
	repo             *Repository
	processor        *Processor
	dlq              *DLQPublisher
	metrics          *ConsumerMetrics
	server           *http.Server
	logger           *slog.Logger
	committedMu      sync.RWMutex
	committedOffsets map[int]int64
}

func NewConsumerApp(ctx context.Context, cfg ConsumerConfig, logger *slog.Logger) (*ConsumerApp, error) {
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8081"
	}
	if cfg.GroupID == "" {
		cfg.GroupID = "warehouse-state-consumer"
	}
	if cfg.DLQTopic == "" {
		cfg.DLQTopic = "warehouse-events-dlq"
	}
	if cfg.CassandraKeyspace == "" {
		cfg.CassandraKeyspace = "warehouse"
	}
	registry := NewSchemaRegistryClient(cfg.SchemaRegistryURL)
	codec, err := NewAvroCodec(registry, cfg.KafkaTopic+"-value")
	if err != nil {
		return nil, err
	}
	if err := retry(ctx, 30, 2*time.Second, codec.EnsureRegistered); err != nil {
		return nil, err
	}

	var repo *Repository
	if err := retry(ctx, 60, 5*time.Second, func(context.Context) error {
		var connectErr error
		repo, connectErr = NewRepository(cfg.CassandraHosts, cfg.CassandraKeyspace)
		return connectErr
	}); err != nil {
		return nil, err
	}

	metrics := NewConsumerMetrics()
	app := &ConsumerApp{
		cfg: cfg,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        cfg.KafkaBrokers,
			Topic:          cfg.KafkaTopic,
			GroupID:        cfg.GroupID,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,
		}),
		codec:            codec,
		repo:             repo,
		processor:        NewProcessor(repo),
		dlq:              NewDLQPublisher(cfg.KafkaBrokers, cfg.DLQTopic),
		metrics:          metrics,
		logger:           logger,
		committedOffsets: make(map[int]int64),
	}
	app.server = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return app, nil
}

func (a *ConsumerApp) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 3)
	go func() {
		a.logger.Info("wms-consumer listening", "addr", a.cfg.HTTPAddr)
		if err := a.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		errCh <- a.consume(runCtx)
	}()
	go a.updateLag(runCtx)

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil && !errors.Is(err, context.Canceled) {
			cancel()
			return err
		}
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = a.server.Shutdown(shutdownCtx)
	_ = a.reader.Close()
	_ = a.dlq.Close()
	a.repo.Close()
	return nil
}

func (a *ConsumerApp) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.Handle("/metrics", promhttp.HandlerFor(a.metrics.Registry, promhttp.HandlerOpts{}))
	return mux
}

func (a *ConsumerApp) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if err := a.repo.Ping(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy", "cassandra": err.Error()})
		return
	}
	if err := a.checkKafka(ctx); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unhealthy", "kafka": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *ConsumerApp) consume(ctx context.Context) error {
	for {
		msg, err := a.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			a.logger.Error("kafka fetch failed", "error", err)
			time.Sleep(time.Second)
			continue
		}
		a.handleMessage(ctx, msg)
	}
}

func (a *ConsumerApp) handleMessage(ctx context.Context, msg kafka.Message) {
	started := time.Now()
	meta := KafkaMeta{Partition: msg.Partition, Offset: msg.Offset}
	event, _, err := a.codec.Decode(ctx, msg.Value)
	if err != nil {
		reason := err.Error()
		if dlqErr := a.dlq.Publish(ctx, nil, msg.Value, reason, err, meta); dlqErr != nil {
			a.logger.Error("dlq publish failed", "partition", msg.Partition, "offset", msg.Offset, "error", dlqErr)
			return
		}
		a.commit(ctx, msg)
		return
	}

	result, err := a.processor.Process(ctx, event)
	if err != nil {
		var validation ValidationError
		if !errors.As(err, &validation) {
			a.metrics.CassandraWriteErrors.Inc()
		}
		if dlqErr := a.dlq.Publish(ctx, &event, msg.Value, err.Error(), err, meta); dlqErr != nil {
			a.logger.Error("dlq publish failed", "event_id", event.EventID, "partition", msg.Partition, "offset", msg.Offset, "error", dlqErr)
			return
		}
		a.logger.Error("event sent to dlq",
			"event_id", event.EventID,
			"event_type", event.EventType,
			"partition", msg.Partition,
			"offset", msg.Offset,
			"error", err,
		)
		a.commit(ctx, msg)
		return
	}

	a.metrics.EventsProcessed.WithLabelValues(string(event.EventType)).Inc()
	a.metrics.ProcessingDuration.Observe(time.Since(started).Seconds())
	a.logger.Info("event processed",
		"event_id", event.EventID,
		"event_type", event.EventType,
		"status", result.Status,
		"reason", result.Reason,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)
	a.commit(ctx, msg)
}

func (a *ConsumerApp) commit(ctx context.Context, msg kafka.Message) {
	if err := a.reader.CommitMessages(ctx, msg); err != nil {
		a.logger.Error("offset commit failed", "partition", msg.Partition, "offset", msg.Offset, "error", err)
		return
	}
	a.committedMu.Lock()
	a.committedOffsets[msg.Partition] = msg.Offset + 1
	a.committedMu.Unlock()
}

func (a *ConsumerApp) updateLag(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		a.refreshLag(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (a *ConsumerApp) refreshLag(ctx context.Context) {
	if len(a.cfg.KafkaBrokers) == 0 {
		return
	}
	conn, err := kafka.DialContext(ctx, "tcp", a.cfg.KafkaBrokers[0])
	if err != nil {
		return
	}
	partitions, err := conn.ReadPartitions(a.cfg.KafkaTopic)
	_ = conn.Close()
	if err != nil {
		return
	}
	for _, partition := range partitions {
		leader, err := kafka.DialLeader(ctx, "tcp", a.cfg.KafkaBrokers[0], a.cfg.KafkaTopic, partition.ID)
		if err != nil {
			continue
		}
		latest, err := leader.ReadLastOffset()
		_ = leader.Close()
		if err != nil {
			continue
		}
		a.committedMu.RLock()
		committed := a.committedOffsets[partition.ID]
		a.committedMu.RUnlock()
		lag := latest - committed
		if lag < 0 {
			lag = 0
		}
		a.metrics.SetLag(a.cfg.KafkaTopic, partition.ID, float64(lag))
	}
}

func (a *ConsumerApp) checkKafka(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", a.cfg.KafkaBrokers[0])
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = conn.ReadPartitions(a.cfg.KafkaTopic)
	return err
}

func splitAndTrim(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func intEnv(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}
