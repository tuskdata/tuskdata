"""Data Contracts API: freeze the current schema as a contract, read its
status and violations, export it as YAML."""

from __future__ import annotations

from litestar import Controller, Request, delete, get, post
from litestar.response import Response

from tusk.core import contracts as ct
from tusk.core import schema_watch as sw
from tusk.core.connection import get_connection
from tusk.studio.routes.base import _current_user_id


def _audit(request: Request, action: str, resource: str, details: str | None = None) -> None:
    from tusk.core.auth import log_audit

    log_audit(
        action,
        user_id=_current_user_id(request) or None,
        resource=resource,
        details=details,
        ip_address=request.client.host if request.client else None,
    )


class ContractsController(Controller):
    path = "/api/contracts"
    tags = ["contracts"]

    @post("/{connection_id:str}/freeze")
    async def freeze(self, request: Request, connection_id: str) -> dict:
        """Take a fresh snapshot and freeze it (or the chosen tables) as the
        connection's contract. Replaces the previous active contract.

        Body is optional JSON ``{"name": ..., "tables": [...]}``; the panel
        button posts nothing (HTMX would send a form, not JSON)."""
        data: dict = {}
        raw = await request.body()
        if raw:
            try:
                data = await request.json()
            except Exception:  # noqa: BLE001 — form-encoded or garbage → defaults
                data = {}
        conn = get_connection(connection_id)
        if conn is None:
            return {"error": "Connection not found"}
        if conn.type != "postgres":
            return {"error": "Contracts only support PostgreSQL connections"}
        try:
            run = await sw.run_watch(connection_id, notify=False)
        except (ValueError, RuntimeError) as e:
            return {"error": str(e)}
        latest = sw.latest_snapshot(connection_id)
        tables = data.get("tables") or None
        try:
            contract = ct.freeze_contract(
                connection_id,
                latest["catalog"],
                name=data.get("name"),
                tables=tables,
                created_by=_current_user_id(request) or None,
                snapshot_id=run["snapshot_id"],
            )
        except ValueError as e:
            return {"error": str(e)}
        _audit(request, "contract.freeze", connection_id, f"#{contract['id']} {contract['name']} ({contract['table_count']} tables)")
        contract.pop("expected", None)
        return {"contract": contract}

    @get("/{connection_id:str}")
    async def status(self, connection_id: str) -> dict:
        """Active contract + open violation for a connection."""
        contract = ct.active_contract(connection_id)
        if not contract:
            return {"connection_id": connection_id, "contract": None, "violation": None}
        open_v = ct.open_violation(contract["id"])
        contract.pop("expected", None)
        return {"connection_id": connection_id, "contract": contract, "violation": open_v}

    @get("/{connection_id:str}/violations")
    async def violations(self, connection_id: str, limit: int = 50) -> dict:
        contract = ct.active_contract(connection_id)
        if not contract:
            return {"violations": []}
        return {"contract_id": contract["id"], "violations": ct.list_violations(contract["id"], limit=limit)}

    @get("/{connection_id:str}/export.yaml")
    async def export_yaml(self, connection_id: str) -> Response:
        contract = ct.active_contract(connection_id)
        if not contract:
            return Response(content=b"# no active contract\n", media_type="text/yaml", status_code=404)
        body = ct.to_yaml(contract).encode("utf-8")
        return Response(
            content=body,
            media_type="text/yaml",
            headers={"Content-Disposition": f'attachment; filename="contract-{connection_id}.yaml"'},
        )

    @delete("/{connection_id:str}", status_code=200)
    async def release(self, request: Request, connection_id: str) -> dict:
        """Deactivate the connection's contract."""
        contract = ct.active_contract(connection_id)
        if not contract:
            return {"error": "No active contract"}
        ct.deactivate_contract(contract["id"])
        _audit(request, "contract.release", connection_id, f"#{contract['id']} {contract['name']}")
        return {"success": True}
