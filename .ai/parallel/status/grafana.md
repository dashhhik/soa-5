# Grafana Status

Done:
- Added ClickHouse datasource provisioning in `infra/grafana/provisioning/datasources/clickhouse.yml`.
- Enabled dashboard provisioning editing in `infra/grafana/provisioning/dashboards/dashboards.yml`.
- Added dashboard `infra/grafana/dashboards/online-cinema.json`.
- Dashboard title is `Online Cinema Analytics`.
- Included panels for retention heatmap, DAU, conversion, top movies, and average watch time.

Files changed:
- `infra/grafana/provisioning/datasources/clickhouse.yml`
- `infra/grafana/provisioning/dashboards/dashboards.yml`
- `infra/grafana/dashboards/online-cinema.json`
- `README.md`

Commands run:
- `docker compose config`
- `python3 -m json.tool infra/grafana/dashboards/online-cinema.json >/dev/null`

Passed:
- Dashboard JSON syntax validation.
- Compose file parses with Grafana provisioning bind mounts in place.

Not passed:
- Live Grafana startup and provisioning were not executed here.

Blockers:
- Docker daemon is not available in this environment, so Grafana could not be launched for end-to-end verification.

Request files created:
- None.
