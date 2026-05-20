# Later: OSI (Open Semantic Interchange) export

## Why later

Strictly downstream of the semantic layer feature. OSI is the **format** Tusk would speak on the wire to interop with Snowflake / dbt / Sigma / Hex / others.

OSI was announced March 2026. The spec will likely change 1-2 times before stabilizing. Building an exporter now is wasted effort.

## Shape when we build it

`tusk metrics export --format osi > metrics.osi.json` — emits the OSI-spec'd manifest of all defined metrics + entities + their SQL bodies. Importable by other OSI-aware tools.

Bonus: `tusk metrics import metrics.osi.json` to ingest a manifest from another tool.

## When this becomes "Next"

- Semantic layer feature is in production.
- OSI spec has shipped a 1.0 (not just announcement).
- We have a customer or design partner who actually wants Tusk-to-OtherTool interop.
