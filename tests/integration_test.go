package tests

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/minio/minio-go/v7"
)

func TestIntegrationSuite(t *testing.T) {
	cfg := loadTestConfig()

	healthChecks(t, cfg)

	pipelineEvent := uniqueEvent(time.Now().UTC())
	pipelineTest(t, cfg, pipelineEvent)

	aggDate := time.Now().UTC().AddDate(0, 0, -2).Truncate(24 * time.Hour)
	aggregationIdempotencyTest(t, cfg, aggDate)
	s3ExportIdempotencyTest(t, cfg, aggDate)

	generatorConsistencyTest(t, cfg)
}

func healthChecks(t *testing.T, cfg testConfig) {
	t.Helper()

	waitForHTTP(t, "producer health", cfg.ProducerURL+"/healthz", 3*time.Minute)
	waitForHTTP(t, "aggregation health", cfg.AggregationURL+"/healthz", 3*time.Minute)
}

func pipelineTest(t *testing.T, cfg testConfig, event movieEvent) {
	t.Helper()

	postEvent(t, cfg.ProducerURL, event)

	query := fmt.Sprintf(`
SELECT
  event_id,
  user_id,
  movie_id,
  event_type,
  toString(timestamp) AS timestamp,
  device_type,
  session_id,
  progress_seconds
FROM analytics.raw_movie_events
WHERE event_id = '%s'`, event.EventID)
	rows := waitForClickHouseRows(t, cfg, query, 5*time.Minute)
	if len(rows) != 1 {
		t.Fatalf("expected 1 ClickHouse row, got %d", len(rows))
	}

	row := rows[0]
	assertStringField(t, row, "event_id", event.EventID)
	assertStringField(t, row, "user_id", event.UserID)
	assertStringField(t, row, "movie_id", event.MovieID)
	assertStringField(t, row, "event_type", event.EventType)
	assertStringField(t, row, "device_type", event.DeviceType)
	assertStringField(t, row, "session_id", event.SessionID)
	assertIntField(t, row, "progress_seconds", event.ProgressSeconds)

	gotTs := mustParseClickHouseTime(t, row["timestamp"])
	wantTs := time.UnixMilli(event.Timestamp).UTC()
	if gotTs.UnixMilli() != wantTs.UnixMilli() {
		t.Fatalf("timestamp mismatch: got %s want %s", gotTs.UTC().Format(time.RFC3339Nano), wantTs.Format(time.RFC3339Nano))
	}
}

func aggregationIdempotencyTest(t *testing.T, cfg testConfig, date time.Time) {
	t.Helper()

	started := uniqueEventAt(date.Add(10 * time.Hour))
	finished := uniqueEventAt(date.Add(11 * time.Hour))
	finished.EventType = "VIEW_FINISHED"
	finished.ProgressSeconds = started.ProgressSeconds + 90
	finished.SessionID = started.SessionID
	finished.MovieID = started.MovieID
	finished.UserID = started.UserID
	finished.DeviceType = started.DeviceType
	finished.EventID = uuid.NewString()

	started.EventType = "VIEW_STARTED"
	started.ProgressSeconds = 0
	started.Timestamp = movieTimestamp(date.Add(10 * time.Hour))
	finished.Timestamp = movieTimestamp(date.Add(11 * time.Hour))

	postEvent(t, cfg.ProducerURL, started)
	postEvent(t, cfg.ProducerURL, finished)

	idsQuery := fmt.Sprintf(`
SELECT count() AS value
FROM analytics.raw_movie_events
WHERE event_id IN ('%s', '%s')
HAVING count() = 2`,
		started.EventID, finished.EventID)
	waitForClickHouseRows(t, cfg, idsQuery, 5*time.Minute)

	runAggregate(t, cfg.AggregationURL, date)
	runAggregate(t, cfg.AggregationURL, date)

	db := openPostgres(t, cfg.PostgresDSN)
	defer db.Close()

	metrics := waitForPostgresMetrics(t, db, date.Format("2006-01-02"), 5*time.Minute)
	assertNoDuplicateMetrics(t, metrics)
	assertMetricNames(t, metrics, []string{"avg_watch_time", "conversion", "dau", "retention", "top_movies"})
	assertRetentionMetrics(t, metrics)
}

func s3ExportIdempotencyTest(t *testing.T, cfg testConfig, date time.Time) {
	t.Helper()

	runExport(t, cfg.AggregationURL, date)
	runExport(t, cfg.AggregationURL, date)

	client, err := newMinIOClient(cfg)
	if err != nil {
		t.Fatalf("new minio client: %v", err)
	}

	key := fmt.Sprintf("daily/%s/aggregates.json", date.Format("2006-01-02"))
	waitForS3Object(t, client, cfg.S3Bucket, key, 5*time.Minute)

	obj, err := client.GetObject(context.Background(), cfg.S3Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		t.Fatalf("get s3 object: %v", err)
	}
	defer obj.Close()

	body, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("read s3 object: %v", err)
	}

	var payload struct {
		Date    string `json:"date"`
		Metrics []struct {
			MetricName string `json:"metric_name"`
		} `json:"metrics"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal s3 payload: %v", err)
	}
	if payload.Date != date.Format("2006-01-02") {
		t.Fatalf("s3 payload date mismatch: got %s want %s", payload.Date, date.Format("2006-01-02"))
	}
	if len(payload.Metrics) == 0 {
		t.Fatalf("expected exported metrics, got none")
	}

	names := make([]string, 0, len(payload.Metrics))
	for _, metric := range payload.Metrics {
		names = append(names, metric.MetricName)
	}
	sort.Strings(names)
	for _, want := range []string{"avg_watch_time", "conversion", "dau", "retention", "top_movies"} {
		if !containsString(names, want) {
			t.Fatalf("exported metrics missing %s", want)
		}
	}

	list := client.ListObjects(context.Background(), cfg.S3Bucket, minio.ListObjectsOptions{
		Prefix:    fmt.Sprintf("daily/%s/", date.Format("2006-01-02")),
		Recursive: true,
	})
	count := 0
	for object := range list {
		if object.Err != nil {
			t.Fatalf("list object: %v", object.Err)
		}
		count++
		if object.Key != key {
			t.Fatalf("unexpected s3 key: got %s want %s", object.Key, key)
		}
	}
	if count != 1 {
		t.Fatalf("expected one S3 object under prefix, got %d", count)
	}
}

func generatorConsistencyTest(t *testing.T, cfg testConfig) {
	t.Helper()

	startReq := map[string]any{
		"interval_ms":  200,
		"batch_size":   3,
		"max_sessions": 4,
		"seed":         time.Now().UnixNano(),
	}
	body, _ := json.Marshal(startReq)
	resp, respBody := postRaw(t, cfg.ProducerURL+"/generate/start", body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("generator start failed: %s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
	defer func() {
		stopResp, stopBody := postRaw(t, cfg.ProducerURL+"/generate/stop", nil)
		if stopResp.StatusCode != http.StatusOK {
			t.Fatalf("generator stop failed: %s: %s", stopResp.Status, strings.TrimSpace(string(stopBody)))
		}
	}()

	query := `
SELECT session_id
FROM analytics.raw_movie_events
GROUP BY session_id
HAVING countIf(event_type = 'VIEW_STARTED') > 0
   AND countIf(event_type = 'VIEW_FINISHED') > 0
LIMIT 1`
	rows := waitForClickHouseRows(t, cfg, query, 7*time.Minute)
	sessionID := asString(t, rows[0]["session_id"])

	events := waitForClickHouseRows(t, cfg, fmt.Sprintf(`
SELECT
  event_type,
  toString(timestamp) AS timestamp,
  progress_seconds
FROM analytics.raw_movie_events
WHERE session_id = '%s'
ORDER BY timestamp, event_id`, sessionID), 2*time.Minute)

	if len(events) < 2 {
		t.Fatalf("expected at least 2 events for session %s, got %d", sessionID, len(events))
	}

	var prevTs time.Time
	var prevProgress int
	for i, event := range events {
		ts := mustParseClickHouseTime(t, event["timestamp"])
		progress := asInt(t, event["progress_seconds"])
		if i == 0 {
			prevTs = ts
			prevProgress = progress
			if asString(t, event["event_type"]) != "VIEW_STARTED" {
				t.Fatalf("first event for session %s should be VIEW_STARTED, got %s", sessionID, asString(t, event["event_type"]))
			}
			continue
		}
		if ts.Before(prevTs) {
			t.Fatalf("timestamps decreased for session %s", sessionID)
		}
		if progress < prevProgress {
			t.Fatalf("progress_seconds decreased for session %s", sessionID)
		}
		prevTs = ts
		prevProgress = progress
	}

	lastType := asString(t, events[len(events)-1]["event_type"])
	if lastType != "VIEW_FINISHED" {
		t.Fatalf("last event for session %s should be VIEW_FINISHED, got %s", sessionID, lastType)
	}
}

type movieEvent struct {
	EventID         string `json:"event_id"`
	UserID          string `json:"user_id"`
	MovieID         string `json:"movie_id"`
	EventType       string `json:"event_type"`
	Timestamp       int64  `json:"timestamp"`
	DeviceType      string `json:"device_type"`
	SessionID       string `json:"session_id"`
	ProgressSeconds int    `json:"progress_seconds"`
}

func uniqueEvent(ts time.Time) movieEvent {
	return movieEvent{
		EventID:         uuid.NewString(),
		UserID:          "user-" + uuid.NewString()[:8],
		MovieID:         "movie-" + uuid.NewString()[:8],
		EventType:       "VIEW_STARTED",
		Timestamp:       ts.UnixMilli(),
		DeviceType:      "DESKTOP",
		SessionID:       uuid.NewString(),
		ProgressSeconds: 0,
	}
}

func uniqueEventAt(ts time.Time) movieEvent {
	event := uniqueEvent(ts)
	event.Timestamp = movieTimestamp(ts)
	return event
}

func movieTimestamp(ts time.Time) int64 {
	return ts.UTC().UnixMilli()
}

func postEvent(t *testing.T, producerURL string, event movieEvent) {
	t.Helper()

	body, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	resp, respBody := postRaw(t, producerURL+"/events", body)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("post event failed: %s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}
}

func runAggregate(t *testing.T, aggregationURL string, date time.Time) {
	t.Helper()
	resp, respBody := postRaw(t, aggregationURL+"/aggregate?date="+date.Format("2006-01-02"), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("aggregate failed for %s: %s: %s", date.Format("2006-01-02"), resp.Status, strings.TrimSpace(string(respBody)))
	}
}

func runExport(t *testing.T, aggregationURL string, date time.Time) {
	t.Helper()
	resp, respBody := postRaw(t, aggregationURL+"/export?date="+date.Format("2006-01-02"), nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("export failed for %s: %s: %s", date.Format("2006-01-02"), resp.Status, strings.TrimSpace(string(respBody)))
	}
}

func postRaw(t *testing.T, rawURL string, body []byte) (*http.Response, []byte) {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := newHTTPClient().Do(req)
	if err != nil {
		t.Fatalf("POST %s failed: %v", rawURL, err)
	}
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		_ = resp.Body.Close()
		t.Fatalf("read response body: %v", err)
	}
	_ = resp.Body.Close()
	return resp, respBody
}

func openPostgres(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetMaxOpenConns(2)
	return db
}

func assertStringField(t *testing.T, row map[string]any, key, want string) {
	t.Helper()
	if got := asString(t, row[key]); got != want {
		t.Fatalf("%s mismatch: got %s want %s", key, got, want)
	}
}

func assertIntField(t *testing.T, row map[string]any, key string, want int) {
	t.Helper()
	if got := asInt(t, row[key]); got != want {
		t.Fatalf("%s mismatch: got %d want %d", key, got, want)
	}
}

func asString(t *testing.T, value any) string {
	t.Helper()
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		t.Fatalf("expected string, got %T (%v)", value, value)
		return ""
	}
}

func asInt(t *testing.T, value any) int {
	t.Helper()
	switch v := value.(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	case json.Number:
		n, err := v.Int64()
		if err != nil {
			t.Fatalf("parse number: %v", err)
		}
		return int(n)
	default:
		t.Fatalf("expected numeric value, got %T (%v)", value, value)
		return 0
	}
}

func mustParseClickHouseTime(t *testing.T, value any) time.Time {
	t.Helper()
	raw := asString(t, value)
	layouts := []string{
		"2006-01-02 15:04:05.000",
		time.RFC3339Nano,
		time.RFC3339,
	}
	for _, layout := range layouts {
		if parsed, err := time.ParseInLocation(layout, raw, time.UTC); err == nil {
			return parsed.UTC()
		}
	}
	t.Fatalf("unable to parse ClickHouse timestamp %q", raw)
	return time.Time{}
}

func assertNoDuplicateMetrics(t *testing.T, metrics []metricRow) {
	t.Helper()
	seen := make(map[string]struct{}, len(metrics))
	for _, metric := range metrics {
		key := metric.MetricName + "|" + metric.Dimension
		if _, ok := seen[key]; ok {
			t.Fatalf("duplicate metric row for %s", key)
		}
		seen[key] = struct{}{}
	}
}

func assertMetricNames(t *testing.T, metrics []metricRow, expected []string) {
	t.Helper()
	found := make(map[string]struct{}, len(metrics))
	for _, metric := range metrics {
		found[metric.MetricName] = struct{}{}
	}
	for _, name := range expected {
		if _, ok := found[name]; !ok {
			t.Fatalf("missing metric_name %s", name)
		}
	}
}

func assertRetentionMetrics(t *testing.T, metrics []metricRow) {
	t.Helper()
	var dims []string
	for _, metric := range metrics {
		if metric.MetricName == "retention" {
			dims = append(dims, metric.Dimension)
		}
	}
	sort.Strings(dims)
	if len(dims) == 0 {
		t.Fatalf("expected retention metrics")
	}
	if !containsString(dims, "D1") || !containsString(dims, "D7") {
		t.Fatalf("expected retention dimensions D1 and D7, got %v", dims)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
