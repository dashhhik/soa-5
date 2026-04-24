package exporter

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type Logger interface {
	Info(msg string, fields map[string]any)
	Error(msg string, fields map[string]any)
}

type ObjectStore interface {
	PutObject(ctx context.Context, bucket, key, contentType string, payload []byte) error
}

type Service struct {
	db     *sql.DB
	store  ObjectStore
	bucket string
	logger Logger
}

type Result struct {
	Date   string `json:"date"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Status string `json:"status"`
}

type Payload struct {
	Date       string        `json:"date"`
	ExportedAt string        `json:"exported_at"`
	Metrics    []MetricEntry `json:"metrics"`
}

type MetricEntry struct {
	MetricDate  string  `json:"metric_date"`
	MetricName  string  `json:"metric_name"`
	Dimension   string  `json:"dimension"`
	MetricValue float64 `json:"metric_value"`
	ComputedAt  string  `json:"computed_at"`
}

func New(db *sql.DB, store ObjectStore, bucket string, logger Logger) *Service {
	return &Service{db: db, store: store, bucket: bucket, logger: logger}
}

func (s *Service) ExportDate(ctx context.Context, date time.Time) (Result, error) {
	startedAt := time.Now().UTC()
	date = date.UTC().Truncate(24 * time.Hour)

	metrics, err := s.loadMetrics(ctx, date)
	if err != nil {
		return Result{}, err
	}

	payload := Payload{
		Date:       date.Format("2006-01-02"),
		ExportedAt: startedAt.Format(time.RFC3339),
		Metrics:    metrics,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Result{}, fmt.Errorf("marshal export payload: %w", err)
	}

	key := fmt.Sprintf("daily/%s/aggregates.json", payload.Date)
	if err := s.store.PutObject(ctx, s.bucket, key, "application/json", body); err != nil {
		return Result{}, fmt.Errorf("upload export object: %w", err)
	}

	duration := time.Since(startedAt)
	if s.logger != nil {
		s.logger.Info("export complete", map[string]any{
			"date":          payload.Date,
			"bucket":        s.bucket,
			"key":           key,
			"metrics_count": len(metrics),
			"duration_ms":   duration.Milliseconds(),
		})
	}

	return Result{
		Date:   payload.Date,
		Bucket: s.bucket,
		Key:    key,
		Status: "ok",
	}, nil
}

func (s *Service) loadMetrics(ctx context.Context, date time.Time) ([]MetricEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT metric_date, metric_name, dimension, metric_value, computed_at
FROM analytics_metrics
WHERE metric_date = $1::date
ORDER BY metric_name, dimension`,
		date.Format("2006-01-02"),
	)
	if err != nil {
		return nil, fmt.Errorf("query analytics_metrics: %w", err)
	}
	defer rows.Close()

	metrics := make([]MetricEntry, 0)
	for rows.Next() {
		var metricDate time.Time
		var metricName string
		var dimension string
		var metricValue float64
		var computedAt time.Time
		if err := rows.Scan(&metricDate, &metricName, &dimension, &metricValue, &computedAt); err != nil {
			return nil, fmt.Errorf("scan analytics_metrics: %w", err)
		}
		metrics = append(metrics, MetricEntry{
			MetricDate:  metricDate.UTC().Format("2006-01-02"),
			MetricName:  metricName,
			Dimension:   dimension,
			MetricValue: metricValue,
			ComputedAt:  computedAt.UTC().Format(time.RFC3339),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate analytics_metrics: %w", err)
	}
	return metrics, nil
}
