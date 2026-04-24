package producer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"
)

type Config struct {
	HTTPAddr          string
	KafkaBrokers      []string
	KafkaTopic        string
	SchemaRegistryURL string
	Generator         GeneratorConfig
}

type App struct {
	cfg        Config
	logger     *slog.Logger
	publisher  *Publisher
	generator  *Generator
	httpServer *http.Server
}

func New(cfg Config, logger *slog.Logger) (*App, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if len(cfg.KafkaBrokers) == 0 {
		return nil, errors.New("kafka brokers must not be empty")
	}
	if cfg.KafkaTopic == "" {
		return nil, errors.New("kafka topic must not be empty")
	}
	if cfg.SchemaRegistryURL == "" {
		return nil, errors.New("schema registry url must not be empty")
	}
	if cfg.HTTPAddr == "" {
		cfg.HTTPAddr = ":8080"
	}
	if cfg.Generator.Interval <= 0 {
		cfg.Generator.Interval = 750 * time.Millisecond
	}
	if cfg.Generator.BatchSize <= 0 {
		cfg.Generator.BatchSize = 2
	}
	if cfg.Generator.MaxSessions <= 0 {
		cfg.Generator.MaxSessions = 32
	}
	if cfg.Generator.Seed == 0 {
		cfg.Generator.Seed = time.Now().UnixNano()
	}

	publisher, err := NewPublisher(PublisherConfig{
		Brokers:           cfg.KafkaBrokers,
		Topic:             cfg.KafkaTopic,
		SchemaRegistryURL: cfg.SchemaRegistryURL,
	}, logger)
	if err != nil {
		return nil, err
	}

	app := &App{
		cfg:       cfg,
		logger:    logger,
		publisher: publisher,
	}
	app.generator = NewGenerator(cfg.Generator, publisher, logger)
	app.httpServer = &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           app.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	return app, nil
}

func (a *App) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		a.logger.Info("movie-producer listening", "addr", a.cfg.HTTPAddr)
		errCh <- a.httpServer.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.generator.Stop()
		_ = a.httpServer.Shutdown(shutdownCtx)
		_ = a.publisher.Close()
		err := <-errCh
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		if err != nil {
			return err
		}
		return nil
	case err := <-errCh:
		_ = a.publisher.Close()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (a *App) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.handleHealthz)
	mux.HandleFunc("POST /events", a.handlePublishEvent)
	mux.HandleFunc("POST /generate/start", a.handleGeneratorStart)
	mux.HandleFunc("POST /generate/stop", a.handleGeneratorStop)
	return mux
}

func (a *App) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *App) handlePublishEvent(w http.ResponseWriter, r *http.Request) {
	var event MovieEvent
	if err := decodeJSON(r, &event); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := a.publisher.Publish(r.Context(), event); err != nil {
		a.logger.Error("event publish failed", "event_id", event.EventID, "user_id", event.UserID, "error", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"event_id": event.EventID})
}

func (a *App) handleGeneratorStart(w http.ResponseWriter, r *http.Request) {
	var req GeneratorStartRequest
	if err := decodeOptionalJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	if err := a.generator.ApplyStartRequest(req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	started, err := a.generator.Start(r.Context())
	if err != nil {
		a.logger.Error("generator start failed", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	status := "already_running"
	if started {
		status = "started"
	}
	a.logger.Info("generator lifecycle", "action", "start", "status", status)
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  status,
		"running": true,
	})
}

func (a *App) handleGeneratorStop(w http.ResponseWriter, _ *http.Request) {
	stopped := a.generator.Stop()
	status := "not_running"
	if stopped {
		status = "stopped"
	}
	a.logger.Info("generator lifecycle", "action", "stop", "status", status)
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  status,
		"running": false,
	})
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	return nil
}

func decodeOptionalJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
	return nil
}
