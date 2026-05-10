package warehouse

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

type ProducerConfig struct {
	HTTPAddr          string
	KafkaBrokers      []string
	KafkaTopic        string
	SchemaRegistryURL string
}

type ProducerApp struct {
	cfg       ProducerConfig
	publisher *Publisher
	server    *http.Server
	logger    *slog.Logger
}

func NewProducerApp(ctx context.Context, cfg ProducerConfig, logger *slog.Logger) (*ProducerApp, error) {
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
	}
	publisher, err := NewPublisher(ctx, PublisherConfig{
		Brokers:           cfg.KafkaBrokers,
		Topic:             cfg.KafkaTopic,
		SchemaRegistryURL: cfg.SchemaRegistryURL,
	}, logger)
	if err != nil {
		return nil, err
	}
	app := &ProducerApp{cfg: cfg, publisher: publisher, logger: logger}
	app.server = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return app, nil
}

func (a *ProducerApp) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		a.logger.Info("wms-producer listening", "addr", a.cfg.HTTPAddr)
		errCh <- a.server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.server.Shutdown(shutdownCtx)
		_ = a.publisher.Close()
		err := <-errCh
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case err := <-errCh:
		_ = a.publisher.Close()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (a *ProducerApp) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", a.handleHealth)
	mux.HandleFunc("GET /healthz", a.handleHealth)
	mux.HandleFunc("POST /events", a.handlePublish)
	mux.HandleFunc("POST /events/unsafe", a.handlePublishUnsafe)
	return mux
}

func (a *ProducerApp) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *ProducerApp) handlePublish(w http.ResponseWriter, r *http.Request) {
	a.publishHTTP(w, r, false)
}

func (a *ProducerApp) handlePublishUnsafe(w http.ResponseWriter, r *http.Request) {
	a.publishHTTP(w, r, true)
}

func (a *ProducerApp) publishHTTP(w http.ResponseWriter, r *http.Request, unsafe bool) {
	var event Event
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	var (
		published Event
		err       error
	)
	if unsafe {
		published, err = a.publisher.PublishUnsafe(r.Context(), event)
	} else {
		published, err = a.publisher.Publish(r.Context(), event)
	}
	if err != nil {
		a.logger.Error("publish failed", "event_id", event.EventID, "event_type", event.EventType, "error", err)
		status := http.StatusBadGateway
		var validation ValidationError
		if errors.As(err, &validation) {
			status = http.StatusBadRequest
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, published)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
