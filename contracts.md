# Online Cinema Analytics Pipeline

## Baseline contract

This repository starts with infrastructure only. The goal of the first step is to make the stack boot to the point where the core services come up under Docker Compose:

- Kafka brokers
- Schema Registry
- ClickHouse
- PostgreSQL
- MinIO

The application services are present as stubs only:

- `movie-producer`
- `aggregation-service`
- `tests`

## Service names

| Service | Docker host | Port |
| --- | --- | --- |
| Kafka broker 1 | `kafka-1` | `9092` |
| Kafka broker 2 | `kafka-2` | `9092` |
| Schema Registry | `schema-registry` | `8081` |
| ClickHouse HTTP | `clickhouse` | `8123` |
| ClickHouse native | `clickhouse` | `9000` |
| PostgreSQL | `postgres` | `5432` |
| Movie Producer | `movie-producer` | `8080` |
| Aggregation Service | `aggregation-service` | `8082` |
| Grafana | `grafana` | `3000` |
| MinIO S3 API | `minio` | `9000` |
| MinIO Console | `minio` | `9001` |

## Kafka

- Topic: `movie-events`
- Partitions: `3`
- Replication factor: `2`
- Minimum in-sync replicas: `1`
- Partition key: `user_id`

## Movie event schema

Canonical schema file:

- `schemas/movie_event.avsc`

Required fields:

- `event_id`
- `user_id`
- `movie_id`
- `event_type`
- `timestamp`
- `device_type`
- `session_id`
- `progress_seconds`

## ClickHouse

Raw ingestion tables are created from:

- `infra/clickhouse/init/001_movie_events.sql`
- `infra/clickhouse/init/002_analytics_views.sql`

Table names:

- `kafka_movie_events`
- `raw_movie_events`
- `mv_kafka_movie_events_to_raw`

## PostgreSQL

Metrics table:

- `analytics_metrics`

Init script:

- `infra/postgres/init/001_metrics.sql`

## Grafana

Provisioning lives under:

- `infra/grafana/provisioning/...`

The baseline includes a ClickHouse datasource and a placeholder dashboard definition.

## Stubs

The following services are intentionally minimal for now:

- `cmd/producer/main.go`
- `cmd/aggregation-service/main.go`
- `tests/Dockerfile`

They only provide startup health endpoints and placeholder responses.
