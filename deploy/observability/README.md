# Observability bundle

Drop-in Prometheus scrape config, alert rules, and a Grafana dashboard
for the OI platform.

## Files

| File                       | Purpose                                           |
|----------------------------|---------------------------------------------------|
| `prometheus.yml`           | Scrape config for `oi-collector` + `oi-api`.      |
| `prometheus-alerts.yml`    | Alert rules — ingest stalls, WS storms, HA, API.  |
| `grafana-dashboard.json`   | Import into Grafana (datasource variable DS_PROMETHEUS). |

## Wiring with the existing compose stack

Append to `deploy/docker-compose.yml`:

```yaml
  prometheus:
    image: prom/prometheus:v2.55.0
    volumes:
      - ./observability/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./observability/prometheus-alerts.yml:/etc/prometheus/rules/oi.yml:ro
    ports: ["127.0.0.1:9096:9090"]

  grafana:
    image: grafana/grafana:11.4.0
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: "Admin"
    ports: ["127.0.0.1:3000:3000"]
    depends_on: [prometheus]
```

Then `curl http://localhost:9096/-/reload` after editing rules.

## Alert severities

* **critical** — paging: `OiCollectorStalled` (nothing writing 3m),
  `OiNoLeader` (writes halted).
* **warning** — non-paging but visible: per-exchange stalls, WS
  reconnect storms, lease flipping, API error rate / p95 breach.
