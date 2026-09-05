"""Alert rules API + the HTMX panel embedded in Notifications settings."""

from __future__ import annotations

from litestar import Controller, Request, delete, get, post
from litestar.params import Body
from litestar.response import Response, Template

from tusk.core import alerts
from tusk.core.connection import list_connections
from tusk.studio.htmx import htmx_toast, is_htmx
from tusk.studio.routes.base import _current_user_id


def _panel_context() -> dict:
    from tusk.core.history import QueryHistory

    saved = []
    try:
        saved = [
            {"id": q.id, "name": q.name, "connection_id": q.connection_id}
            for q in QueryHistory().get_saved_queries()
        ]
    except Exception:  # noqa: BLE001 — the panel must render without history
        saved = []
    widgets = []
    try:
        from tusk.bi.db import get_dashboards, get_widgets

        for d in get_dashboards():
            for w in get_widgets(d["id"]):
                if w.get("query_id"):
                    widgets.append({"id": w["id"], "label": f"{d['name']} · {w.get('title') or w.get('query_name') or 'widget'}"})
    except Exception:  # noqa: BLE001 — Analytics may be absent
        widgets = []
    return {
        "rules": [r.to_dict() for r in alerts.list_rules()],
        "metrics": [{"key": k, "label": v[0], "unit": v[1]} for k, v in alerts.METRICS.items()],
        "ops": [{"key": k, "label": v} for k, v in alerts.OP_LABELS.items()],
        "connections": [{"id": c.id, "name": c.name, "type": c.type} for c in list_connections()],
        "saved_queries": saved,
        "widgets": widgets,
    }


class AlertsController(Controller):
    path = "/api/alerts"
    tags = ["alerts"]

    @get("/panel")
    async def panel(self, request: Request) -> Template:
        return Template("partials/alerts_panel.html", context=_panel_context())

    @get("/")
    async def list_rules(self) -> dict:
        return {"rules": [r.to_dict() for r in alerts.list_rules()], "summary": alerts.rules_summary()}

    @post("/")
    async def create(self, request: Request, data: dict = Body()) -> Response:
        try:
            rule = alerts.create_rule(
                name=data.get("name", ""),
                source_kind=data.get("source_kind", ""),
                source_ref=str(data.get("source_ref", "")),
                op=data.get("op", "gt"),
                threshold=float(data.get("threshold", 0) or 0),
                connection_id=data.get("connection_id") or None,
                for_seconds=int(data.get("for_seconds", 0) or 0),
                enabled=bool(data.get("enabled", True)),
                owner_id=_current_user_id(request) or "",
            )
        except (ValueError, TypeError) as exc:
            return Response(content={"error": str(exc)}, status_code=400)
        _audit(request, "alert.create", rule.id, rule.name)
        return Response(content={"rule": rule.to_dict()}, status_code=201, headers=htmx_toast(f"Alert '{rule.name}' created"))

    @post("/{rule_id:str}")
    async def update(self, request: Request, rule_id: str, data: dict = Body()) -> Response:
        if not alerts.get_rule(rule_id):
            return Response(content={"error": "not found"}, status_code=404)
        try:
            rule = alerts.update_rule(rule_id, **{k: data[k] for k in ("name", "op", "threshold", "for_seconds", "enabled", "source_ref", "connection_id") if k in data})
        except (ValueError, TypeError) as exc:
            return Response(content={"error": str(exc)}, status_code=400)
        return Response(content={"rule": rule.to_dict() if rule else None}, headers=htmx_toast("Alert updated"))

    @post("/{rule_id:str}/toggle")
    async def toggle(self, request: Request, rule_id: str) -> Response:
        rule = alerts.get_rule(rule_id)
        if not rule:
            return Response(content={"error": "not found"}, status_code=404)
        rule = alerts.update_rule(rule_id, enabled=not rule.enabled)
        msg = "Alert resumed" if rule and rule.enabled else "Alert paused"
        if is_htmx(request):
            return Template("partials/alerts_panel.html", context=_panel_context(), headers=htmx_toast(msg))
        return Response(content={"rule": rule.to_dict() if rule else None}, headers=htmx_toast(msg))

    @post("/{rule_id:str}/check")
    async def check_now(self, request: Request, rule_id: str) -> Response:
        rule = alerts.get_rule(rule_id)
        if not rule:
            return Response(content={"error": "not found"}, status_code=404)
        out = await alerts.check_rule(rule, notify=True)
        if out.get("transition") == "error":
            msg, variant = f"Check failed: {out.get('error')}", "error"
        else:
            msg, variant = f"{rule.name}: value {out.get('value'):g} → {out.get('state')}", "info"
        if is_htmx(request):
            return Template("partials/alerts_panel.html", context=_panel_context(), headers=htmx_toast(msg, variant))
        return Response(content=out, headers=htmx_toast(msg, variant))

    @delete("/{rule_id:str}", status_code=200)
    async def remove(self, request: Request, rule_id: str) -> Response:
        rule = alerts.get_rule(rule_id)
        if not rule:
            return Response(content={"error": "not found"}, status_code=404)
        alerts.delete_rule(rule_id)
        _audit(request, "alert.delete", rule.id, rule.name)
        if is_htmx(request):
            return Template("partials/alerts_panel.html", context=_panel_context(), headers=htmx_toast("Alert deleted"))
        return Response(content={"deleted": True}, headers=htmx_toast("Alert deleted"))


def _audit(request: Request, action: str, rule_id: str, name: str) -> None:
    try:
        from tusk.core.auth import log_audit

        log_audit(_current_user_id(request) or "", action, f"{name} ({rule_id})", request.client.host if request.client else "")
    except Exception:  # noqa: BLE001 — audit must never break the request
        pass
