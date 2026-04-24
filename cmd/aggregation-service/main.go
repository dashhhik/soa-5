package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	aggexporter "movie-analytics/internal/aggregation/exporter"
	aggs3 "movie-analytics/internal/aggregation/s3"

	_ "github.com/lib/pq"
	"github.com/robfig/cron/v3"
)

func main() {
	cfg := loadConfig()
	logger := newJSONLogger(os.Stdout)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pgDB, err := sql.Open("postgres", cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("postgres db: %v", err)
	}
	defer pgDB.Close()

	chClient := newClickHouseClient(cfg.ClickHouseURL)
	svc := newAggregationService(cfg, pgDB, chClient, logger)
	s3Bucket := env("S3_BUCKET", "movie-analytics")
	s3Client, err := aggs3.New(
		env("S3_ENDPOINT", "http://minio:9000"),
		env("S3_ACCESS_KEY", "minioadmin"),
		env("S3_SECRET_KEY", "minioadmin"),
		boolEnv("S3_USE_SSL", false),
	)
	if err != nil {
		log.Fatalf("s3 client: %v", err)
	}
	exporter := aggexporter.New(pgDB, s3Client, env("S3_BUCKET", "movie-analytics"), logger)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, apiResponse{Status: "ok"})
	})
	mux.HandleFunc("POST /aggregate", svc.handleAggregate)
	mux.HandleFunc("POST /export", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, apiResponse{Error: "method not allowed"})
			return
		}

		date, err := parseDateParam(r.URL.Query().Get("date"))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, apiResponse{Error: err.Error()})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), cfg.QueryTimeout)
		defer cancel()

		key := fmt.Sprintf("daily/%s/aggregates.json", date.Format("2006-01-02"))
		result, err := exporter.ExportDate(ctx, date)
		if err != nil {
			logger.Error("export failed", map[string]any{
				"date":   date.Format("2006-01-02"),
				"bucket": s3Bucket,
				"key":    key,
				"error":  err.Error(),
			})
			writeJSON(w, http.StatusInternalServerError, apiResponse{Error: err.Error()})
			return
		}

		writeJSON(w, http.StatusOK, result)
	})

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: mux,
	}

	cronExpr := cfg.AggregationCron
	scheduler := cron.New(
		cron.WithLocation(time.UTC),
		cron.WithParser(cron.NewParser(cron.Minute|cron.Hour|cron.Dom|cron.Month|cron.Dow)),
	)
	if cronExpr != "" {
		if _, err := scheduler.AddFunc(cronExpr, func() {
			runCtx, cancel := context.WithTimeout(context.Background(), cfg.QueryTimeout)
			defer cancel()
			date := time.Now().UTC().AddDate(0, 0, -1)
			if _, err := svc.aggregateDate(runCtx, date); err != nil {
				svc.logger.Error("scheduler aggregate failed", map[string]any{
					"date":  date.Format("2006-01-02"),
					"error": err.Error(),
				})
			}
		}); err != nil {
			log.Fatalf("invalid AGGREGATION_CRON %q: %v", cronExpr, err)
		}
	}

	exportCron := strings.TrimSpace(env("EXPORT_CRON", "0 3 * * *"))
	if exportCron != "" {
		if _, err := scheduler.AddFunc(exportCron, func() {
			runCtx, cancel := context.WithTimeout(context.Background(), cfg.QueryTimeout)
			defer cancel()
			date := time.Now().UTC().AddDate(0, 0, -1)
			if _, err := exporter.ExportDate(runCtx, date); err != nil {
				logger.Error("scheduler export failed", map[string]any{
					"date":   date.Format("2006-01-02"),
					"bucket": s3Bucket,
					"key":    fmt.Sprintf("daily/%s/aggregates.json", date.Format("2006-01-02")),
					"error":  err.Error(),
				})
			}
		}); err != nil {
			log.Fatalf("invalid EXPORT_CRON %q: %v", exportCron, err)
		}
	}

	scheduler.Start()
	defer scheduler.Stop()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("aggregation-service listening", map[string]any{"addr": cfg.HTTPAddr})
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			log.Printf("aggregation-service server error: %v", err)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
}

type apiResponse struct {
	Status string `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func notImplemented(feature string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusNotImplemented, apiResponse{Error: feature + " is not implemented in the baseline"})
	}
}

func boolEnv(key string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return value
}
