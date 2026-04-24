# Tests Status

Done:
- Created a separate Go module under `tests/`.
- Implemented live integration tests for health checks, event ingestion, aggregation idempotency, S3 export idempotency, and generator consistency.
- Added helper utilities for HTTP, ClickHouse, PostgreSQL, and MinIO polling.
- Wired explicit compose env variables for the tests service.
- Tests use the real pipeline only, no mocks.

Files changed:
- `tests/Dockerfile`
- `tests/go.mod`
- `tests/go.sum`
- `tests/helpers_test.go`
- `tests/integration_test.go`
- `docker-compose.yml`

Commands run:
- `go test -run '^$' ./...` in `tests/`
- `go mod tidy -go=1.23.0 -compat=1.23.0` in `tests/`

Passed:
- Tests module compiles with `go test -run '^$' ./...`.

Not passed:
- Full suite execution against a running compose stack was not completed here.

Blockers:
- Docker daemon is not available in this environment, so `docker compose up --build tests` could not be executed.

Request files created:
- None.
