"""The Copilot panel must ask with the connection the page has selected.

From 0.4.6 to 0.4.48 tusk-ai.js read ``window.currentConnectionId`` while
Studio published ``window.currentConnection``: every question from the UI
went out with no connection, so no schema grounding and no dry run. These
checks pin the contract between the scripts so the names cannot drift
apart again.
"""

from __future__ import annotations

import re
from pathlib import Path

STATIC = Path(__file__).resolve().parent.parent / "src" / "tusk" / "studio" / "static"


def test_studio_publishes_the_selected_connection():
    studio = (STATIC / "studio.js").read_text()
    assert "window.currentConnection = currentConnection;" in studio
    assert "window.currentConnection = null;" in studio  # cleared on deselect


def test_explore_publishes_the_selected_connection():
    explore = (STATIC / "explore.js").read_text()
    assert re.search(r"window\.currentConnection = this\.selectedConn", explore)


def test_copilot_reads_what_studio_publishes():
    ai = (STATIC / "tusk-ai.js").read_text()
    body = ai[ai.index("function _currentConnectionId()"):]
    body = body[: body.index("\n    }\n")]
    assert "window.currentConnection" in body and "c.id" in body


def test_copilot_keeps_the_server_verdicts_for_the_card():
    ai = (STATIC / "tusk-ai.js").read_text()
    stored = ai[ai.index('STATE.last_response = {\n                kind: "sql"'):]
    stored = stored[: stored.index("};")]
    for field in ("verified", "verify_error", "joins", "join_warnings", "confidence"):
        assert f"{field}: res.{field}" in stored, f"{field} not kept in STATE.last_response"
