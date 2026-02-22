"""Shared fixtures for Tusk tests"""

import sys
from pathlib import Path

import pytest

# Ensure src/ is on the path for imports
SRC_DIR = Path(__file__).parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


@pytest.fixture(autouse=True)
def _reset_plugin_registry():
    """Reset plugin registry between tests to avoid state leakage."""
    from tusk.plugins.registry import reset_registry
    reset_registry()
    yield
    reset_registry()
