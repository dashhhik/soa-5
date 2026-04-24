package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type config struct {
	HTTPAddr        string
	ClickHouseURL   string
	PostgresDSN     string
	AggregationCron string
	QueryTimeout    time.Duration
	TopMoviesLimit  int
}

func loadConfig() config {
	return config{
		HTTPAddr:        env("HTTP_ADDR", ":8082"),
		ClickHouseURL:   strings.TrimRight(env("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123"), "/"),
		PostgresDSN:     env("POSTGRES_DSN", "postgres://analytics:analytics@postgres:5432/analytics?sslmode=disable"),
		AggregationCron: env("AGGREGATION_CRON", "0 2 * * *"),
		QueryTimeout:    durationEnv("QUERY_TIMEOUT", 30*time.Second),
		TopMoviesLimit:  intEnv("TOP_MOVIES_LIMIT", 10),
	}
}

type aggregationService struct {
	cfg    config
	pg     *sql.DB
	ch     *clickHouseClient
	logger *jsonLogger
	mu     sync.Mutex
}

func newAggregationService(cfg config, pg *sql.DB, ch *clickHouseClient, logger *jsonLogger) *aggregationService {
	return &aggregationService{cfg: cfg, pg: pg, ch: ch, logger: logger}
}

func (s *aggregationService) handleAggregate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, apiResponse{Error: "method not allowed"})
		return
	}

	date, err := parseDateParam(r.URL.Query().Get("date"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()

	summary, err := s.aggregateDate(ctx, date)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, apiResponse{Error: err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, summary)
}

func (s *aggregationService) aggregateDate(ctx context.Context, date time.Time) (aggregateSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	startedAt := time.Now().UTC()
	date = date.UTC().Truncate(24 * time.Hour)

	s.logger.Info("aggregation started", map[string]any{
		"date":          date.Format("2006-01-02"),
		"started_at":    startedAt,
		"query_timeout": s.cfg.QueryTimeout.String(),
	})

	rowsProcessed, err := s.ch.rowsProcessed(ctx, date)
	if err != nil {
		return aggregateSummary{}, err
	}

	metrics, err := s.buildMetrics(ctx, date)
	if err != nil {
		return aggregateSummary{}, err
	}

	metricsWritten, err := s.upsertMetrics(ctx, metrics)
	if err != nil {
		return aggregateSummary{}, err
	}

	summary := aggregateSummary{
		Date:           date.Format("2006-01-02"),
		RowsProcessed:  rowsProcessed,
		MetricsWritten: metricsWritten,
		DurationMillis: time.Since(startedAt).Milliseconds(),
	}

	s.logger.Info("aggregation complete", map[string]any{
		"date":            summary.Date,
		"rows_processed":  summary.RowsProcessed,
		"metrics_written": summary.MetricsWritten,
		"duration_ms":     summary.DurationMillis,
	})

	return summary, nil
}

func (s *aggregationService) buildMetrics(ctx context.Context, date time.Time) ([]metricRow, error) {
	start, end := dayBounds(date)

	dau, err := s.ch.dailyActiveUsers(ctx, start, end)
	if err != nil {
		return nil, err
	}

	avgWatchTime, err := s.ch.avgWatchTime(ctx, start, end)
	if err != nil {
		return nil, err
	}

	conversion, err := s.ch.conversion(ctx, start, end)
	if err != nil {
		return nil, err
	}

	topMovies, err := s.ch.topMovies(ctx, start, end, s.cfg.TopMoviesLimit)
	if err != nil {
		return nil, err
	}

	d1, err := s.ch.retention(ctx, date, 1)
	if err != nil {
		return nil, err
	}

	d7, err := s.ch.retention(ctx, date, 7)
	if err != nil {
		return nil, err
	}

	metrics := make([]metricRow, 0, 4+len(topMovies)+2)
	metrics = append(metrics,
		newMetric(date, "dau", "", float64(dau)),
		newMetric(date, "avg_watch_time", "", avgWatchTime),
		newMetric(date, "conversion", "", conversion),
		newMetric(date, "retention", "D1", d1),
		newMetric(date, "retention", "D7", d7),
	)

	for _, movie := range topMovies {
		metrics = append(metrics, newMetric(date, "top_movies", movie.MovieID, float64(movie.Views)))
	}

	return metrics, nil
}

func parseDateParam(raw string) (time.Time, error) {
	if raw == "" {
		return time.Time{}, errors.New("missing date query parameter")
	}

	parsed, err := time.Parse("2006-01-02", raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid date %q: %w", raw, err)
	}
	return parsed.UTC(), nil
}

func dayBounds(date time.Time) (time.Time, time.Time) {
	start := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	return start, start.Add(24 * time.Hour)
}

func newMetric(date time.Time, name, dimension string, value float64) metricRow {
	return metricRow{
		MetricDate:  date.UTC().Format("2006-01-02"),
		MetricName:  name,
		Dimension:   dimension,
		MetricValue: value,
		ComputedAt:  time.Now().UTC(),
	}
}

type aggregateSummary struct {
	Date           string `json:"date"`
	RowsProcessed  int64  `json:"rows_processed"`
	MetricsWritten int64  `json:"metrics_written"`
	DurationMillis int64  `json:"duration_ms"`
}

type metricRow struct {
	MetricDate  string
	MetricName  string
	Dimension   string
	MetricValue float64
	ComputedAt  time.Time
}

func intEnv(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func durationEnv(key string, fallback time.Duration) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
