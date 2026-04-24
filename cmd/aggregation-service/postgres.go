package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *aggregationService) upsertMetrics(ctx context.Context, metrics []metricRow) (int64, error) {
	if len(metrics) == 0 {
		return 0, nil
	}

	tx, err := s.pg.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	const stmt = `
INSERT INTO analytics_metrics (
    metric_date,
    metric_name,
    dimension,
    metric_value,
    computed_at
)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (metric_date, metric_name, dimension)
DO UPDATE SET
    metric_value = EXCLUDED.metric_value,
    computed_at = EXCLUDED.computed_at`

	now := s.now().UTC()
	var written int64
	for _, metric := range metrics {
		if _, err := tx.ExecContext(ctx, stmt,
			metric.MetricDate,
			metric.MetricName,
			metric.Dimension,
			metric.MetricValue,
			now,
		); err != nil {
			return 0, fmt.Errorf("upsert metric %s/%s: %w", metric.MetricName, metric.Dimension, err)
		}
		written++
	}

	if err := tx.Commit(); err != nil {
		return 0, err
	}

	return written, nil
}

func (s *aggregationService) now() time.Time {
	return time.Now().UTC()
}
