Agent S3 owns:
- internal/aggregation/exporter/**
- internal/aggregation/s3/**
- internal/aggregation/http/** only if needed to wire /export
- cmd/aggregation-service/main.go only for /export wiring and scheduler
- go.mod/go.sum only if an S3 SDK dependency is needed

Agent Grafana owns:
- infra/grafana/**

Agent Tests owns:
- tests/**
- tests/go.mod/go.sum, if tests become a separate Go module
- does not touch production code

Agent Reliability owns only after other agents finish:
- README.md
- docker-compose.yml
- healthcheck/retry small fixes
- .ai/parallel/final-report.md
- does not rewrite business logic unless necessary

Common files:
- contracts.md is read-only. Do not change it without an explicit final decision.
- schemas/movie_event.avsc is read-only.
- docker-compose.yml is read-only for S3/Grafana/Tests unless a request file explicitly asks for a change.
- infra/postgres/init/** is read-only.
- infra/clickhouse/init/** is read-only for S3/Tests/Grafana.
- internal/event/** is read-only.
- internal/producer/** is read-only.
- cmd/movie-producer/** is read-only.

Conflict protocol:
- If an agent needs a file outside its ownership, it must not edit it directly.
- Instead, create a request file in .ai/parallel/requests/ with the minimum required change and rationale.

Status protocol:
- Each agent writes a status file in .ai/parallel/status/ when done.
