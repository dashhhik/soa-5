CREATE VIEW IF NOT EXISTS analytics.v_dau_movie_events AS
SELECT
    toDate(timestamp) AS event_date,
    uniqExact(user_id) AS dau
FROM analytics.raw_movie_events
GROUP BY toDate(timestamp);

CREATE VIEW IF NOT EXISTS analytics.v_avg_watch_time_movie_events AS
SELECT
    toDate(timestamp) AS event_date,
    if(
        countIf(event_type = 'VIEW_FINISHED') = 0,
        toFloat64(0),
        avgIf(progress_seconds, event_type = 'VIEW_FINISHED')
    ) AS avg_watch_time_seconds,
    countIf(event_type = 'VIEW_FINISHED') AS finished_views
FROM analytics.raw_movie_events
GROUP BY toDate(timestamp);

CREATE VIEW IF NOT EXISTS analytics.v_conversion_movie_events AS
SELECT
    toDate(timestamp) AS event_date,
    countIf(event_type = 'VIEW_STARTED') AS started_views,
    countIf(event_type = 'VIEW_FINISHED') AS finished_views,
    countIf(event_type = 'VIEW_FINISHED') / nullIf(countIf(event_type = 'VIEW_STARTED'), 0) AS conversion_rate
FROM analytics.raw_movie_events
GROUP BY toDate(timestamp);

CREATE VIEW IF NOT EXISTS analytics.v_top_movies_movie_events AS
SELECT
    event_date,
    movie_id,
    view_count,
    movie_rank
FROM
(
    SELECT
        event_date,
        movie_id,
        view_count,
        row_number() OVER (PARTITION BY event_date ORDER BY view_count DESC, movie_id) AS movie_rank
    FROM
    (
        SELECT
            toDate(timestamp) AS event_date,
            movie_id,
            count() AS view_count
        FROM analytics.raw_movie_events
        WHERE event_type = 'VIEW_STARTED'
        GROUP BY toDate(timestamp), movie_id
    ) AS view_counts
) AS ranked_movies
WHERE movie_rank <= 10;

CREATE VIEW IF NOT EXISTS analytics.v_retention_movie_events AS
WITH
    first_view AS
    (
        SELECT
            user_id,
            min(toDate(timestamp)) AS cohort_date
        FROM analytics.raw_movie_events
        WHERE event_type = 'VIEW_STARTED'
        GROUP BY user_id
    ),
    cohort_sizes AS
    (
        SELECT
            cohort_date,
            count() AS cohort_size
        FROM first_view
        GROUP BY cohort_date
    ),
    daily_activity AS
    (
        SELECT DISTINCT
            e.user_id,
            f.cohort_date,
            dateDiff('day', f.cohort_date, toDate(e.timestamp)) AS day_number
        FROM analytics.raw_movie_events AS e
        INNER JOIN first_view AS f USING (user_id)
        WHERE dateDiff('day', f.cohort_date, toDate(e.timestamp)) BETWEEN 0 AND 7
    ),
    retained AS
    (
        SELECT
            cohort_date,
            day_number,
            count() AS retained_users
        FROM daily_activity
        GROUP BY
            cohort_date,
            day_number
    )
SELECT
    c.cohort_date,
    d.day_number,
    coalesce(r.retained_users, 0) AS retained_users,
    c.cohort_size,
    coalesce(r.retained_users, 0) / c.cohort_size AS retention_rate
FROM cohort_sizes AS c
CROSS JOIN
(
    SELECT arrayJoin([0, 1, 2, 3, 4, 5, 6, 7]) AS day_number
) AS d
LEFT JOIN retained AS r
    ON r.cohort_date = c.cohort_date
   AND r.day_number = d.day_number
ORDER BY
    c.cohort_date,
    d.day_number;
