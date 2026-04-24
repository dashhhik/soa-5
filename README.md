# Online Cinema Analytics Pipeline

Pipeline for event ingestion, daily analytics, S3 export, and Grafana reporting.

## Architecture

- `movie-producer` accepts events and synthetic generation requests.
- Kafka topic `movie-events` carries Avro Confluent messages keyed by `user_id`.
- ClickHouse ingests raw events into `analytics.raw_movie_events` and exposes analytics views.
- `aggregation-service` reads ClickHouse for daily aggregates and writes `analytics_metrics` in PostgreSQL.
- The same service exports daily metrics to MinIO as JSON objects.
- Grafana reads ClickHouse through the ClickHouse datasource and provisions dashboards automatically.

## Run

Run the integration suite against the live compose stack:

```bash
docker compose up --build tests
```

Build and start the full stack:

```bash
docker compose up --build
```

Validate compose only:

```bash
docker compose config
```

## Endpoints

### Producer

- `GET /healthz`
- `POST /events`
- `POST /generate/start`
- `POST /generate/stop`

### Aggregation

- `GET /healthz`
- `POST /aggregate?date=YYYY-MM-DD`
- `POST /export?date=YYYY-MM-DD`

### Grafana

- [http://localhost:3000](http://localhost:3000)
- `admin / admin`

### MinIO

- Console: [http://localhost:9001](http://localhost:9001)
- Credentials: `minioadmin / minioadmin`

## Kafka

- Topic: `movie-events`
- Partitions: `3`
- Replication factor: `2`
- Minimum in-sync replicas: `1`
- Partition key: `user_id`

The key ensures all events for a user stay ordered on the same partition.

## Metrics

The aggregation service writes one row per metric key into PostgreSQL:

- `dau`
- `avg_watch_time`
- `conversion`
- `retention` with dimensions `D1` and `D7`
- `top_movies` with movie IDs in `dimension`

Exports write all rows for a date to:

- `movie-analytics`
- `daily/YYYY-MM-DD/aggregates.json`

## Checklist

- Kafka brokers: `kafka-1`, `kafka-2`
- Schema Registry: `schema-registry`
- ClickHouse raw ingestion and views
- PostgreSQL `analytics_metrics`
- MinIO bucket `movie-analytics`
- Grafana provisioning and dashboard
- Integration tests under `tests/`
- S3 export idempotency
- Aggregation idempotency

## Notes

- The canonical contract lives in `contracts.md`.
- The integration tests use real services only, no mocks.
