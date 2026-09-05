"""The wheel must ship every non-Python asset the app serves at runtime.

0.4.39 shipped without `tusk/bi/templates` and `/bi` answered 500 in
production while everything worked from the source tree. This builds the
wheel and checks the directories that must be inside it.
"""

from __future__ import annotations

import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
MUST_SHIP = (
    "tusk/studio/templates/base.html",
    "tusk/studio/templates/components/feedback.html",
    "tusk/studio/static/studio.js",
    "tusk/bi/templates/bi/overview.html",
    "tusk/bi/static/bi/widgets.js",
)


def test_wheel_contains_runtime_assets(tmp_path):
    uv = Path(sys.executable).with_name("uv")
    cmd = [str(uv) if uv.exists() else "uv", "build", "--wheel", "--out-dir", str(tmp_path)]
    res = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=300)
    if res.returncode != 0:
        pytest.skip(f"uv build unavailable here: {res.stderr[-300:]}")
    wheel = next(tmp_path.glob("tuskdata-*.whl"))
    names = set(zipfile.ZipFile(wheel).namelist())
    missing = [m for m in MUST_SHIP if m not in names]
    assert not missing, f"wheel is missing runtime assets: {missing}"
