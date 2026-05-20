# Later: Agentic anomaly investigation

## Why later

Cool but depends on **semantic-layer + MCP** as prerequisites. Without those, the agent doesn't have grounded context and just hallucinates. Same reason ThoughtSpot/Cube/Sigma are racing on this in 2026 — the underlying semantic foundation is what makes it useful, not the agent loop itself.

## Shape when we build it

When an alert fires (per `Alerts & Actions in widgets`), the Copilot agent kicks off a "what changed?" investigation:

1. Pulls schema + recent commits via lineage/contracts.
2. Runs candidate diagnostic queries (compare to last week, group by attribute, etc.).
3. Posts to Slack: "MRR dropped 5% — appears localized to `tenant_id IN (...)` due to 3 cancellations on 2026-05-19. Linked queries: ..."

Human approves the explanation → close incident. Or thumbs-down → agent learns.

## When this becomes "Next"

- Semantic layer shipped.
- MCP integration shipped.
- Alerts & Actions has been in use for 1-2 releases (so we have real alert patterns to investigate).
