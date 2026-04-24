CREATE TABLE IF NOT EXISTS analytics_metrics (
    metric_date DATE NOT NULL,
    metric_name TEXT NOT NULL,
    dimension TEXT NOT NULL DEFAULT '',
    metric_value DOUBLE PRECISION NOT NULL,
    computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (metric_date, metric_name, dimension)
);

CREATE INDEX IF NOT EXISTS idx_analytics_metrics_name
    ON analytics_metrics (metric_name);
