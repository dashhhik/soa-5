package tests

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type testConfig struct {
	ProducerURL    string
	AggregationURL string
	ClickHouseURL  string
	PostgresDSN    string
	S3Endpoint     string
	S3AccessKey    string
	S3SecretKey    string
	S3Bucket       string
	S3UseSSL       bool
}

func loadTestConfig() testConfig {
	return testConfig{
		ProducerURL:    env("PRODUCER_URL", "http://movie-producer:8080"),
		AggregationURL: env("AGGREGATION_URL", "http://aggregation-service:8082"),
		ClickHouseURL:  env("CLICKHOUSE_HTTP_URL", "http://clickhouse:8123"),
		PostgresDSN:    env("POSTGRES_DSN", "postgres://analytics:analytics@postgres:5432/analytics?sslmode=disable"),
		S3Endpoint:     env("S3_ENDPOINT", "http://minio:9000"),
		S3AccessKey:    env("S3_ACCESS_KEY", "minioadmin"),
		S3SecretKey:    env("S3_SECRET_KEY", "minioadmin"),
		S3Bucket:       env("S3_BUCKET", "movie-analytics"),
		S3UseSSL:       boolEnv("S3_USE_SSL", false),
	}
}

func env(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func boolEnv(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func newHTTPClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second}
}

func waitForHTTP(t *testing.T, name, rawURL string, timeout time.Duration) {
	t.Helper()

	client := newHTTPClient()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		req, _ := http.NewRequest(http.MethodGet, rawURL, nil)
		resp, err := client.Do(req)
		if err == nil && resp != nil {
			_ = resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return
			}
			lastErr = fmt.Errorf("%s returned %s", name, resp.Status)
		} else if err != nil {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("timeout waiting for %s at %s: %v", name, rawURL, lastErr)
}

func waitForHTTPPost(t *testing.T, rawURL string, body []byte, timeout time.Duration) (*http.Response, []byte) {
	t.Helper()

	client := newHTTPClient()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodPost, rawURL, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		if len(body) > 0 {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		if err == nil && resp != nil {
			respBody, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr != nil {
				lastErr = readErr
				time.Sleep(2 * time.Second)
				continue
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return resp, respBody
			}
			lastErr = fmt.Errorf("POST %s returned %s: %s", rawURL, resp.Status, strings.TrimSpace(string(respBody)))
		} else if err != nil {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("timeout waiting for POST %s: %v", rawURL, lastErr)
	return nil, nil
}

func waitForClickHouseRows(t *testing.T, cfg testConfig, sqlQuery string, timeout time.Duration) []map[string]any {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		rows, err := queryClickHouse(cfg, sqlQuery)
		if err != nil {
			lastErr = err
			time.Sleep(2 * time.Second)
			continue
		}
		if len(rows) > 0 {
			return rows
		}
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("timeout waiting for ClickHouse rows for query %q: %v", sqlQuery, lastErr)
	return nil
}

func waitForPostgresMetrics(t *testing.T, db *sql.DB, date string, timeout time.Duration) []metricRow {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		metrics, err := loadMetricsForDate(db, date)
		if err != nil {
			lastErr = err
			time.Sleep(2 * time.Second)
			continue
		}
		if len(metrics) > 0 {
			return metrics
		}
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("timeout waiting for postgres metrics on %s: %v", date, lastErr)
	return nil
}

func newMinIOClient(cfg testConfig) (*minio.Client, error) {
	endpoint, err := normalizeEndpoint(cfg.S3Endpoint)
	if err != nil {
		return nil, err
	}
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.S3AccessKey, cfg.S3SecretKey, ""),
		Secure: cfg.S3UseSSL,
	})
}

func normalizeEndpoint(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty endpoint")
	}
	if strings.Contains(raw, "://") {
		parsed, err := url.Parse(raw)
		if err != nil {
			return "", err
		}
		if parsed.Host != "" {
			return parsed.Host, nil
		}
		if parsed.Path != "" {
			return parsed.Path, nil
		}
		return "", fmt.Errorf("invalid endpoint %q", raw)
	}
	return raw, nil
}

func waitForS3Object(t *testing.T, client *minio.Client, bucket, key string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		_, err := client.StatObject(context.Background(), bucket, key, minio.StatObjectOptions{})
		if err == nil {
			return
		}
		lastErr = err
		time.Sleep(2 * time.Second)
	}

	t.Fatalf("timeout waiting for s3 object %s/%s: %v", bucket, key, lastErr)
}

func queryClickHouse(cfg testConfig, sqlQuery string) ([]map[string]any, error) {
	req, err := http.NewRequest(http.MethodPost, cfg.ClickHouseURL, strings.NewReader(sqlQuery+"\nFORMAT JSON"))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := newHTTPClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("clickhouse returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var decoded struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, err
	}
	return decoded.Data, nil
}

func loadMetricsForDate(db *sql.DB, date string) ([]metricRow, error) {
	rows, err := db.Query(`
SELECT metric_date, metric_name, dimension, metric_value, computed_at
FROM analytics_metrics
WHERE metric_date = $1::date
ORDER BY metric_name, dimension`, date)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var metrics []metricRow
	for rows.Next() {
		var metricDate time.Time
		var metricName string
		var dimension string
		var metricValue float64
		var computedAt time.Time
		if err := rows.Scan(&metricDate, &metricName, &dimension, &metricValue, &computedAt); err != nil {
			return nil, err
		}
		metrics = append(metrics, metricRow{
			MetricDate:  metricDate.UTC().Format("2006-01-02"),
			MetricName:  metricName,
			Dimension:   dimension,
			MetricValue: metricValue,
			ComputedAt:  computedAt.UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return metrics, nil
}

type metricRow struct {
	MetricDate  string
	MetricName  string
	Dimension   string
	MetricValue float64
	ComputedAt  time.Time
}
