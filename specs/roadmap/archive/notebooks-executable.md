# Later: Notebooks executable as pipelines

## Why later

The mockup has a Notebooks tab. We have the engine pieces (Polars, DuckDB, SQL, AI Copilot) but **the notebook UI itself is 80% of the work** — easily 4-6 weeks for a decent first version.

The buyer persona (data analyst) is also narrower than SMB engineering teams. Embedded SDK, Alerts, Data Contracts all serve SMB engineering directly; notebooks serve a specialist seat.

## Shape when we build it

Hex/Deepnote-style:
- Cells: SQL, Python (Polars), Markdown, chart.
- Outputs cached, re-runnable per cell.
- Same execution engine as dashboards.
- "Schedule this notebook" → runs as a job, outputs go to a dashboard or a webhook.

## When this becomes "Next"

- 0.7.x ships.
- A user actively requests notebooks (or two adjacent users do — signals real demand).
- We've decided to add the data-analyst persona to the SMB-engineering focus.
