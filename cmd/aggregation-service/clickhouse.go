package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type clickHouseClient struct {
	baseURL string
	client  *http.Client
}

func newClickHouseClient(baseURL string) *clickHouseClient {
	return &clickHouseClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

type chJSONResponse struct {
	Data []json.RawMessage `json:"data"`
}

func (c *clickHouseClient) query(ctx context.Context, sql string, dst any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL, bytes.NewBufferString(sql+"\nFORMAT JSON"))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("clickhouse query failed: %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}

	var parsed chJSONResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return err
	}

	return decodeCHData(parsed.Data, dst)
}

func decodeCHData(rows []json.RawMessage, dst any) error {
	blob, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	return json.Unmarshal(blob, dst)
}

func (c *clickHouseClient) rowsProcessed(ctx context.Context, date time.Time) (int64, error) {
	var rows []struct {
		Value int64 `json:"value"`
	}
	if err := c.query(ctx, fmt.Sprintf(`
SELECT count() AS value
FROM analytics.raw_movie_events
WHERE timestamp >= toDateTime64('%s', 3, 'UTC')
  AND timestamp < toDateTime64('%s', 3, 'UTC')`,
		date.Format("2006-01-02 15:04:05"),
		date.Add(24*time.Hour).Format("2006-01-02 15:04:05"),
	), &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Value, nil
}

func (c *clickHouseClient) dailyActiveUsers(ctx context.Context, start, end time.Time) (int64, error) {
	var rows []struct {
		Value int64 `json:"value"`
	}
	if err := c.query(ctx, fmt.Sprintf(`
SELECT uniqExact(user_id) AS value
FROM analytics.raw_movie_events
WHERE timestamp >= toDateTime64('%s', 3, 'UTC')
  AND timestamp < toDateTime64('%s', 3, 'UTC')`,
		start.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"),
	), &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Value, nil
}

func (c *clickHouseClient) avgWatchTime(ctx context.Context, start, end time.Time) (float64, error) {
	var rows []struct {
		Value float64 `json:"value"`
	}
	if err := c.query(ctx, fmt.Sprintf(`
SELECT coalesce(avgIf(progress_seconds, event_type = 'VIEW_FINISHED'), 0) AS value
FROM analytics.raw_movie_events
WHERE timestamp >= toDateTime64('%s', 3, 'UTC')
  AND timestamp < toDateTime64('%s', 3, 'UTC')`,
		start.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"),
	), &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Value, nil
}

func (c *clickHouseClient) conversion(ctx context.Context, start, end time.Time) (float64, error) {
	var rows []struct {
		Finished int64 `json:"finished"`
		Started  int64 `json:"started"`
	}
	if err := c.query(ctx, fmt.Sprintf(`
SELECT
    countIf(event_type = 'VIEW_FINISHED') AS finished,
    countIf(event_type = 'VIEW_STARTED') AS started
FROM analytics.raw_movie_events
WHERE timestamp >= toDateTime64('%s', 3, 'UTC')
  AND timestamp < toDateTime64('%s', 3, 'UTC')`,
		start.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"),
	), &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 || rows[0].Started == 0 {
		return 0, nil
	}
	return float64(rows[0].Finished) / float64(rows[0].Started), nil
}

type topMovieRow struct {
	MovieID string `json:"movie_id"`
	Views   int64  `json:"views"`
}

func (c *clickHouseClient) topMovies(ctx context.Context, start, end time.Time, limit int) ([]topMovieRow, error) {
	var rows []topMovieRow
	if err := c.query(ctx, fmt.Sprintf(`
SELECT
    movie_id,
    countIf(event_type = 'VIEW_STARTED') AS views
FROM analytics.raw_movie_events
WHERE timestamp >= toDateTime64('%s', 3, 'UTC')
  AND timestamp < toDateTime64('%s', 3, 'UTC')
GROUP BY movie_id
ORDER BY views DESC, movie_id ASC
LIMIT %d`,
		start.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"),
		limit,
	), &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func (c *clickHouseClient) retention(ctx context.Context, cohortDate time.Time, offset int) (float64, error) {
	var rows []struct {
		Value int64 `json:"value"`
	}
	if err := c.query(ctx, retentionQuery(cohortDate, offset), &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	cohortSize, err := c.cohortSize(ctx, cohortDate)
	if err != nil {
		return 0, err
	}
	if cohortSize == 0 {
		return 0, nil
	}
	return float64(rows[0].Value) / float64(cohortSize), nil
}

func (c *clickHouseClient) cohortSize(ctx context.Context, cohortDate time.Time) (int64, error) {
	var rows []struct {
		Value int64 `json:"value"`
	}
	if err := c.query(ctx, fmt.Sprintf(`
SELECT count() AS value
FROM (
    SELECT user_id
    FROM analytics.raw_movie_events
    GROUP BY user_id
    HAVING min(toDate(timestamp)) = toDate('%s')
)`,
		cohortDate.Format("2006-01-02"),
	), &rows); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Value, nil
}

func retentionQuery(cohortDate time.Time, offset int) string {
	return fmt.Sprintf(`
SELECT count() AS value
FROM (
    SELECT user_id
    FROM analytics.raw_movie_events
    GROUP BY user_id
    HAVING min(toDate(timestamp)) = toDate('%s')
) AS cohort
INNER JOIN (
    SELECT DISTINCT user_id
    FROM analytics.raw_movie_events
    WHERE toDate(timestamp) = addDays(toDate('%s'), %d)
) AS active USING (user_id)`,
		cohortDate.Format("2006-01-02"),
		cohortDate.Format("2006-01-02"),
		offset,
	)
}
