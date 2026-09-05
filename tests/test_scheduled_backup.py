"""Backups programados: carpeta de destino, formato y rotación.

Hasta 0.4.27 `_handle_backup` ignoraba `backup_dir` y no existía
rotación, así que un backup diario acababa llenando el volumen.
"""

import asyncio
import time
from pathlib import Path

import pytest

from tusk.admin import backup as backup_mod
from tusk.core import scheduled_tasks


def _touch(path: Path, age_s: float) -> None:
    path.write_bytes(b"x" * 200)
    ts = time.time() - age_s
    import os
    os.utime(path, (ts, ts))


def test_prune_keeps_newest_and_removes_sidecars(tmp_path: Path):
    for i, age in enumerate([10, 20, 30, 40]):
        f = tmp_path / f"shop_2026-01-0{i}_000000.dump"
        _touch(f, age)
        (tmp_path / (f.name + ".meta.json")).write_text("{}")
    # Otra base en la misma carpeta no debe tocarse
    _touch(tmp_path / "other_2026-01-01_000000.dump", 999)

    removed = backup_mod.prune_backups("shop", keep_last=2, backup_dir=tmp_path)

    assert sorted(removed) == ["shop_2026-01-02_000000.dump", "shop_2026-01-03_000000.dump"]
    remaining = sorted(p.name for p in tmp_path.iterdir())
    assert remaining == [
        "other_2026-01-01_000000.dump",
        "shop_2026-01-00_000000.dump", "shop_2026-01-00_000000.dump.meta.json",
        "shop_2026-01-01_000000.dump", "shop_2026-01-01_000000.dump.meta.json",
    ]


def test_prune_zero_means_keep_everything(tmp_path: Path):
    _touch(tmp_path / "shop_a.dump", 1)
    _touch(tmp_path / "shop_b.dump", 2)
    assert backup_mod.prune_backups("shop", keep_last=0, backup_dir=tmp_path) == []
    assert len(list(tmp_path.iterdir())) == 2


def test_prune_missing_dir_is_noop(tmp_path: Path):
    assert backup_mod.prune_backups("shop", keep_last=3, backup_dir=tmp_path / "nope") == []


def test_handle_backup_passes_dir_format_and_prunes(monkeypatch, tmp_path: Path):
    calls: dict = {}

    class Cfg:
        type = "postgres"
        database = "shop"

    monkeypatch.setattr(scheduled_tasks, "get_connection", lambda cid: Cfg())

    def fake_create_backup(config, *, format, tables, backup_dir):
        calls["create"] = {"format": format, "tables": tables, "backup_dir": backup_dir}
        return True, "ok", tmp_path / "shop_x.dump"

    def fake_prune(database, keep_last, backup_dir):
        calls["prune"] = {"database": database, "keep_last": keep_last, "backup_dir": backup_dir}
        return []

    monkeypatch.setattr(backup_mod, "create_backup", fake_create_backup)
    monkeypatch.setattr(backup_mod, "prune_backups", fake_prune)

    asyncio.run(scheduled_tasks._handle_backup({
        "connection_id": "c1", "format": "custom", "keep_last": 5, "backup_dir": str(tmp_path),
    }))

    assert calls["create"] == {"format": "custom", "tables": None, "backup_dir": str(tmp_path)}
    assert calls["prune"] == {"database": "shop", "keep_last": 5, "backup_dir": str(tmp_path)}


def test_handle_backup_failure_does_not_prune(monkeypatch, tmp_path: Path):
    class Cfg:
        type = "postgres"
        database = "shop"

    monkeypatch.setattr(scheduled_tasks, "get_connection", lambda cid: Cfg())
    monkeypatch.setattr(backup_mod, "create_backup", lambda *a, **k: (False, "pg_dump failed", None))
    pruned = []
    monkeypatch.setattr(backup_mod, "prune_backups", lambda *a, **k: pruned.append(a))

    with pytest.raises(RuntimeError, match="pg_dump failed"):
        asyncio.run(scheduled_tasks._handle_backup({"connection_id": "c1", "keep_last": 3}))
    assert pruned == []


def test_add_backup_schedule_stores_format_and_retention(monkeypatch):
    captured = {}
    monkeypatch.setattr(scheduled_tasks, "add_job", lambda spec: captured.update(spec.payload) or spec.id)
    scheduled_tasks.add_backup_schedule("c1", backup_dir="/mnt/bk", format="custom", keep_last=14)
    assert captured == {"connection_id": "c1", "backup_dir": "/mnt/bk", "format": "custom", "keep_last": 14}
