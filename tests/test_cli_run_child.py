"""`tusk studio` must not orphan the granian child when it is terminated."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
import time

import pytest

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")


def test_sigterm_reaches_child():
    # Parent: runs a sleeping child through _run_child. Child: prints its pid.
    parent_code = textwrap.dedent(
        """
        import sys
        from tusk.cli import _run_child
        code = _run_child([sys.executable, "-c", "import os,time,sys; print(os.getpid(), flush=True); time.sleep(60)"])
        print("child exit", code, flush=True)
        """
    )
    parent = subprocess.Popen([sys.executable, "-c", parent_code], stdout=subprocess.PIPE, text=True)
    child_pid = int(parent.stdout.readline().strip())
    time.sleep(0.2)
    parent.send_signal(signal.SIGTERM)
    out, _ = parent.communicate(timeout=10)
    assert "child exit" in out
    # The child is gone (ESRCH) or at least no longer sleeping.
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            os.kill(child_pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    else:
        pytest.fail(f"child {child_pid} survived the parent's SIGTERM")


def test_child_exit_code_propagates():
    from tusk.cli import _run_child

    assert _run_child([sys.executable, "-c", "raise SystemExit(3)"]) == 3
