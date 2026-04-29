"""Tests for the scheduler's pipeline handler.

Wires `_handle_pipeline` to the real polars_engine pipeline runner —
this suite verifies that:

1. A scheduled pipeline job actually runs and writes a parquet file.
2. Pipeline runs are persisted in the `pipeline_runs` table.
3. Missing workspace / dataset surface as ValueError (not
   NotImplementedError as the v0.4.8 stub raised).
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture
def isolated_tusk_home(tmp_path, monkeypatch):
    """Redirect ``TUSK_DIR`` (and the SQLite scheduler DB / parquet
    output dir) to a throwaway directory so tests don't touch the dev's
    real ``~/.tusk``.

    The module-level ``TUSK_DIR`` constant is captured by reference at
    import time in ``_handle_pipeline`` (it builds
    ``TUSK_DIR / "pipeline_runs"``). Patching the attribute on the
    module is enough because the handler reads ``TUSK_DIR`` at call
    time, not import time.
    """
    from tusk.core import scheduled_tasks

    fake_home = tmp_path / "tusk_home"
    fake_home.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(scheduled_tasks, "TUSK_DIR", fake_home)
    monkeypatch.setattr(
        scheduled_tasks, "SCHEDULER_DB", fake_home / "scheduler.db"
    )

    # ``load_workspace`` resolves WORKSPACES_DIR at call time too, so
    # patch its module-level constants the same way.
    from tusk.core import workspace as ws_mod

    monkeypatch.setattr(ws_mod, "TUSK_DIR", fake_home)
    monkeypatch.setattr(ws_mod, "WORKSPACES_DIR", fake_home / "workspaces")

    yield fake_home


def _write_csv(tmp_path: Path, name: str = "input.csv") -> Path:
    p = tmp_path / name
    p.write_text("id,value\n1,a\n2,b\n3,c\n")
    return p


def _make_workspace_with_dataset(
    fake_home: Path, dataset_id: str, csv_path: Path, transforms: list[dict] | None = None
):
    """Persist a workspace JSON with a single CSV-backed dataset."""
    from tusk.core import workspace as ws_mod

    ds = ws_mod.DatasetState(
        id=dataset_id,
        name="test_dataset",
        source_type="csv",
        path=str(csv_path),
        transforms=transforms or [],
        join_sources=[],
    )
    state = ws_mod.WorkspaceState(name="default", datasets=[ds])
    result = ws_mod.save_workspace(state)
    assert result.get("success"), f"failed to save workspace: {result}"


# ────────────────────────────────────────────────────────────────
# Behavior tests
# ────────────────────────────────────────────────────────────────


def test_handle_pipeline_no_longer_raises_not_implemented(isolated_tusk_home):
    """Regression: in v0.4.8 the handler raised NotImplementedError.

    After v0.4.9 it must surface domain errors as ValueError (or run
    successfully) — never NotImplementedError.
    """
    from tusk.core.scheduled_tasks import _handle_pipeline

    async def go():
        await _handle_pipeline({"workspace": "no-such-workspace", "pipeline_id": "x"})

    with pytest.raises(ValueError, match="not found"):
        asyncio.run(go())


def test_handle_pipeline_missing_pipeline_id_raises_value_error(isolated_tusk_home):
    from tusk.core.scheduled_tasks import _handle_pipeline

    async def go():
        await _handle_pipeline({"workspace": "default"})

    with pytest.raises(ValueError, match="pipeline_id"):
        asyncio.run(go())


def test_handle_pipeline_missing_dataset_raises_value_error(isolated_tusk_home, tmp_path):
    """Workspace exists but the dataset id doesn't — must be ValueError."""
    from tusk.core.scheduled_tasks import _handle_pipeline

    csv = _write_csv(tmp_path)
    _make_workspace_with_dataset(isolated_tusk_home, "ds-real", csv)

    async def go():
        await _handle_pipeline(
            {"workspace": "default", "pipeline_id": "ds-missing"}
        )

    with pytest.raises(ValueError, match="ds-missing"):
        asyncio.run(go())


def test_handle_pipeline_runs_and_writes_parquet(isolated_tusk_home, tmp_path):
    """End-to-end: workspace + dataset → handler runs → parquet on disk +
    pipeline_runs row recorded with correct row count."""
    from tusk.core.scheduled_tasks import _handle_pipeline, get_pipeline_runs

    csv = _write_csv(tmp_path)
    _make_workspace_with_dataset(isolated_tusk_home, "ds-1", csv)

    async def go():
        await _handle_pipeline(
            {
                "workspace": "default",
                "pipeline_id": "ds-1",
                "_job_id": "pipeline_test_job",
            }
        )

    asyncio.run(go())

    # Parquet file should exist under ~/.tusk/pipeline_runs/<job_id>/
    out_dir = isolated_tusk_home / "pipeline_runs" / "pipeline_test_job"
    assert out_dir.is_dir(), "pipeline_runs directory not created"
    parquets = list(out_dir.glob("*.parquet"))
    assert len(parquets) == 1, f"expected 1 parquet file, got {parquets}"

    # Parquet should be readable + have the same rows as the input CSV.
    df = pl.read_parquet(parquets[0])
    assert df.height == 3
    assert set(df.columns) == {"id", "value"}

    # pipeline_runs table must have a row keyed to the job_id.
    runs = get_pipeline_runs("pipeline_test_job")
    assert len(runs) == 1
    run = runs[0]
    assert run["error"] is None
    assert run["rows_written"] == 3
    assert run["output_path"] == str(parquets[0])


def test_handle_pipeline_records_failure(isolated_tusk_home, tmp_path):
    """If the pipeline raises mid-execution, a pipeline_runs row with
    the error must still be written (audit trail)."""
    from tusk.core.scheduled_tasks import _handle_pipeline, get_pipeline_runs

    # Point the dataset at a CSV that doesn't exist — _run_pipeline
    # raises FileNotFoundError when load_source tries to open it.
    missing = tmp_path / "does_not_exist.csv"
    _make_workspace_with_dataset(isolated_tusk_home, "ds-broken", missing)

    async def go():
        await _handle_pipeline(
            {
                "workspace": "default",
                "pipeline_id": "ds-broken",
                "_job_id": "pipeline_failure_job",
            }
        )

    with pytest.raises(Exception):
        asyncio.run(go())

    runs = get_pipeline_runs("pipeline_failure_job")
    assert len(runs) == 1
    assert runs[0]["error"], "failure must be recorded with error string"
    assert runs[0]["output_path"] is None
    assert runs[0]["rows_written"] is None


def test_handle_pipeline_applies_transforms(isolated_tusk_home, tmp_path):
    """Transforms saved on the dataset must be applied during the run.

    Filter out one row → parquet should have 2, not 3.
    """
    from tusk.core.scheduled_tasks import _handle_pipeline

    csv = _write_csv(tmp_path)
    transforms = [{"type": "filter", "column": "id", "operator": "lt", "value": 3}]
    _make_workspace_with_dataset(
        isolated_tusk_home, "ds-filter", csv, transforms=transforms
    )

    async def go():
        await _handle_pipeline(
            {
                "workspace": "default",
                "pipeline_id": "ds-filter",
                "_job_id": "pipeline_transform_job",
            }
        )

    asyncio.run(go())

    out_dir = isolated_tusk_home / "pipeline_runs" / "pipeline_transform_job"
    parquets = list(out_dir.glob("*.parquet"))
    assert len(parquets) == 1
    df = pl.read_parquet(parquets[0])
    assert df.height == 2, "filter `id < 3` should have left 2 rows"


def test_pipeline_runs_table_exists_after_init(isolated_tusk_home):
    """Smoke check that ``_init_db`` creates the pipeline_runs table."""
    from tusk.core.scheduled_tasks import _init_db, SCHEDULER_DB

    _init_db()
    conn = sqlite3.connect(str(SCHEDULER_DB))
    try:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_runs'"
        ).fetchall()
    finally:
        conn.close()
    assert rows, "pipeline_runs table not created by _init_db"


def test_dispatcher_injects_job_id_into_payload(isolated_tusk_home, tmp_path):
    """``_run_job`` must inject ``_job_id`` into the payload before calling
    the handler. Verified by registering a fake handler and asserting
    the key shows up."""
    import tusk.core.scheduled_tasks as st

    captured: dict = {}

    async def fake_handler(payload):
        captured.update(payload)

    spec = st.JobSpec(
        id="job-dispatcher-test",
        kind=st.JobKind.PIPELINE,
        name="test",
        payload={"workspace": "ws", "pipeline_id": "pid"},
        trigger={"type": "cron", "cron": "0 0 * * *"},
    )
    st.save_job(spec)

    original = st._KIND_HANDLERS.get(st.JobKind.PIPELINE)
    st._KIND_HANDLERS[st.JobKind.PIPELINE] = fake_handler
    try:
        asyncio.run(st._run_job("job-dispatcher-test"))
    finally:
        if original is not None:
            st._KIND_HANDLERS[st.JobKind.PIPELINE] = original

    assert captured.get("_job_id") == "job-dispatcher-test"
    assert captured.get("_job_name") == "test"
    assert captured.get("pipeline_id") == "pid"
