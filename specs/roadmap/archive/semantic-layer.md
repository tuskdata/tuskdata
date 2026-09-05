# Later: Agentic Semantic Layer

## Why later (not now)

This is the **highest-impact** feature we have on the radar — semantic layer is *the* breakout category for 2026 per the industry research (Snowflake's OSI announcement, Databricks reporting 80% of new DBs created by AI agents). But:

1. **OSI just dropped (Mar 2026)**. The spec will change 1-2 times this year. Building an exporter against a moving target burns cycles.
2. **Pre-requisite**: MCP integration in the AI Copilot should land first — it's the upstream that lets the semantic layer ground agent calls.
3. **Pre-requisite**: stable Data Contracts (0.5.x) — many semantic-layer constructs (entity definitions, valid metric ranges) overlap with contract concepts. Building contracts first means the semantic layer can re-use the storage and UI.

## Shape when we build it

YAML in a `metrics/` directory, scanned at startup or on dashboard edit:

```yaml
- name: mrr
  description: Monthly recurring revenue, deferred-aware
  type: gauge
  sql: |
    SELECT SUM(amount) / 100.0
    FROM subscriptions
    WHERE status = 'active' AND deferred = FALSE
  entities: [tenant_id]
  unit: usd
```

The AI Copilot prompt always begins with the registry of available metrics. Charts/dashboards can reference `metric: mrr` instead of writing SQL. OSI export available as `tusk metrics export --format osi`.

## When this becomes "Next"

- MCP integration shipped in core
- 0.6.x done
- OSI spec stabilized (v1.0)
