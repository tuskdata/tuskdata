"""Workspace persistence for Tusk Data Studio

Saves and loads workspace state (datasets, transforms, join sources) to ~/.tusk/workspaces/
"""

from pathlib import Path
from datetime import datetime
import msgspec
import structlog

log = structlog.get_logger()

TUSK_DIR = Path.home() / ".tusk"
WORKSPACES_DIR = TUSK_DIR / "workspaces"
DEFAULT_WORKSPACE = "default"


class DatasetState(msgspec.Struct):
    """Persisted state for a dataset"""
    id: str
    name: str
    source_type: str
    path: str | None = None
    connection_id: str | None = None
    query: str | None = None
    osm_layer: str | None = None
    transforms: list[dict] = msgspec.field(default_factory=list)
    join_sources: list[dict] = msgspec.field(default_factory=list)


class WorkspaceState(msgspec.Struct):
    """Full workspace state"""
    name: str
    datasets: list[DatasetState] = msgspec.field(default_factory=list)
    active_dataset_id: str | None = None
    updated_at: str | None = None
    version: int = 1


def _workspace_path(name: str) -> Path:
    """Get path to workspace file"""
    return WORKSPACES_DIR / f"{name}.json"


def save_workspace(state: WorkspaceState) -> dict:
    """Save workspace state to file"""
    try:
        WORKSPACES_DIR.mkdir(parents=True, exist_ok=True)
        state.updated_at = datetime.now().isoformat()

        path = _workspace_path(state.name)
        with open(path, "wb") as f:
            f.write(msgspec.json.encode(state))

        log.info("Workspace saved", name=state.name, datasets=len(state.datasets))
        return {"success": True, "path": str(path)}
    except Exception as e:
        log.error("Failed to save workspace", name=state.name, error=str(e))
        return {"error": str(e)}


def load_workspace(name: str = DEFAULT_WORKSPACE) -> WorkspaceState | None:
    """Load workspace state from file"""
    path = _workspace_path(name)
    if not path.exists():
        log.debug("Workspace not found", name=name)
        return None

    try:
        with open(path, "rb") as f:
            state = msgspec.json.decode(f.read(), type=WorkspaceState)
        log.info("Workspace loaded", name=name, datasets=len(state.datasets))
        return state
    except Exception as e:
        log.error("Failed to load workspace", name=name, error=str(e))
        return None


def delete_workspace(name: str) -> bool:
    """Delete a workspace file"""
    path = _workspace_path(name)
    if path.exists():
        path.unlink()
        log.info("Workspace deleted", name=name)
        return True
    return False


def list_workspaces() -> list[dict]:
    """List all saved workspaces"""
    if not WORKSPACES_DIR.exists():
        return []

    workspaces = []
    for path in WORKSPACES_DIR.glob("*.json"):
        try:
            with open(path, "rb") as f:
                state = msgspec.json.decode(f.read(), type=WorkspaceState)
            workspaces.append({
                "name": state.name,
                "datasets": len(state.datasets),
                "updated_at": state.updated_at
            })
        except Exception:
            # Skip invalid files
            pass

    return sorted(workspaces, key=lambda w: w.get("updated_at") or "", reverse=True)


def workspace_state_from_dict(data: dict) -> WorkspaceState:
    """Convert dict from frontend to WorkspaceState"""
    datasets = []
    for ds in data.get("datasets", []):
        datasets.append(DatasetState(
            id=ds.get("id", ""),
            name=ds.get("name", ""),
            source_type=ds.get("source_type", "csv"),
            path=ds.get("path"),
            connection_id=ds.get("connection_id"),
            query=ds.get("query"),
            osm_layer=ds.get("osm_layer"),
            transforms=ds.get("transforms", []),
            join_sources=ds.get("joinSources", [])
        ))

    return WorkspaceState(
        name=data.get("name", DEFAULT_WORKSPACE),
        datasets=datasets,
        active_dataset_id=data.get("active_dataset_id")
    )


def workspace_state_to_dict(state: WorkspaceState) -> dict:
    """Convert WorkspaceState to dict for frontend"""
    return {
        "name": state.name,
        "datasets": [
            {
                "id": ds.id,
                "name": ds.name,
                "source_type": ds.source_type,
                "path": ds.path,
                "connection_id": ds.connection_id,
                "query": ds.query,
                "osm_layer": ds.osm_layer,
                "transforms": ds.transforms,
                "joinSources": ds.join_sources
            }
            for ds in state.datasets
        ],
        "active_dataset_id": state.active_dataset_id,
        "updated_at": state.updated_at
    }
