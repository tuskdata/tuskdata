"""Tests for the job watchdog (tusk.core.jobs.JobRegistry.mark_timed_out).

The watchdog is the resilience knob that catches jobs hung past their
declared max_duration_s. Without it, a stuck job ties up a worker slot
until Granian recycles the whole worker (1h).
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


@pytest.fixture
def registry():
    """A fresh JobRegistry backed by a tempdir DB."""
    home = tempfile.mkdtemp(prefix="tusk_wd_test_")
    Path(home, ".tusk").mkdir(parents=True, exist_ok=True)
    os.environ["HOME"] = home

    # Force re-import so JOBS_DB resolves against the new HOME.
    import importlib
    import tusk.core.jobs as jobs_mod
    importlib.reload(jobs_mod)

    yield jobs_mod.JobRegistry(db_path=Path(home, ".tusk", "jobs.db"))


def _job(registry, *, id: str, status: str = "running",
         started_offset_s: int = 0, max_duration_s: int = 0):
    """Insert a job at a controlled point in time. `started_offset_s`
    is seconds in the past."""
    from tusk.core.jobs import Job
    started = (datetime.now(timezone.utc) - timedelta(seconds=started_offset_s)).isoformat()
    job = Job(
        id=id, label=f"label-{id}", kind="test", status=status,
        started_at=started, max_duration_s=max_duration_s,
    )
    registry._insert(job)
    return job


def test_watchdog_kills_jobs_past_deadline(registry):
    """A job started 60s ago with max_duration_s=10 must be killed."""
    _job(registry, id="exp1", started_offset_s=60, max_duration_s=10)

    killed = registry.mark_timed_out()

    assert killed == 1
    after = registry.get("exp1")
    assert after.status == "failed_timeout"
    assert "exceeded max_duration_s=10" in after.error
    assert after.ended_at is not None


def test_watchdog_leaves_jobs_within_deadline(registry):
    """A fresh job with the same budget must stay running."""
    _job(registry, id="fresh", started_offset_s=2, max_duration_s=60)

    killed = registry.mark_timed_out()

    assert killed == 0
    assert registry.get("fresh").status == "running"


def test_watchdog_skips_zero_max_duration(registry):
    """max_duration_s=0 means "no deadline" — never kill."""
    _job(registry, id="nodeadline", started_offset_s=3600, max_duration_s=0)

    killed = registry.mark_timed_out()

    assert killed == 0
    assert registry.get("nodeadline").status == "running"


def test_watchdog_only_touches_running_jobs(registry):
    """`done`/`failed`/`interrupted` jobs are immutable, even if they
    look "past their deadline" on paper."""
    _job(registry, id="done", status="done", started_offset_s=60, max_duration_s=10)
    _job(registry, id="failed", status="failed", started_offset_s=60, max_duration_s=10)

    killed = registry.mark_timed_out()

    assert killed == 0
    assert registry.get("done").status == "done"
    assert registry.get("failed").status == "failed"


def test_watchdog_mixed_population(registry):
    """One pass should kill only the expired ones and report the count."""
    _job(registry, id="a", started_offset_s=120, max_duration_s=30)   # kill
    _job(registry, id="b", started_offset_s=10, max_duration_s=60)    # keep
    _job(registry, id="c", started_offset_s=120, max_duration_s=0)    # keep (opted out)
    _job(registry, id="d", started_offset_s=120, max_duration_s=30,   # keep (already done)
         status="done")

    killed = registry.mark_timed_out()

    assert killed == 1
    assert registry.get("a").status == "failed_timeout"
    assert registry.get("b").status == "running"
    assert registry.get("c").status == "running"
    assert registry.get("d").status == "done"


def test_default_max_duration_per_kind():
    """submit_sync without an explicit max_duration_s falls back to the
    per-kind default, not the catch-all."""
    from tusk.core.jobs import DEFAULT_MAX_DURATION_S, DEFAULT_MAX_DURATION_FALLBACK_S
    assert DEFAULT_MAX_DURATION_S["backup"] == 3600
    assert DEFAULT_MAX_DURATION_S["query"] == 600
    assert DEFAULT_MAX_DURATION_FALLBACK_S == 1800
