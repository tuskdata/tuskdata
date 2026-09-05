"""Print stats about the AI Copilot's track record.

Reads `~/.tusk/ai_memory.db` (conversation log) and `~/.tusk/history.db`
(every query ever executed) and correlates them by time + connection
to answer the only question that matters: **when the AI suggested SQL,
did it actually work when run?**

Usage:

    tusk ai stats                       # default, last 30 days
    tusk ai stats --days 7              # last week
    tusk ai stats --session <key>       # one session
    tusk ai stats --verbose             # include every prompt + verdict

Or, if you don't have the tusk CLI on the host (e.g. you're sshing into
the Coolify container to look at production state), run it directly:

    python -m tusk.scripts.ai_stats

Output format is plain text — designed to be readable in a terminal
without scrollbar abuse.

Heuristics (because "success" of an AI suggestion isn't trivial):

  HONEST     The AI returned SQL starting with `-- ` (its "I don't
             know, here's what's missing" pattern from the prompt).
             Counted as a positive — the grounding worked, the model
             admitted ignorance instead of hallucinating.

  CONFIRMED  AI suggested SQL, a query ran on the same connection
             within 5 min, and it succeeded. Strong signal the
             suggestion was usable.

  FAILED     AI suggested SQL, a query ran on the same connection
             within 5 min, and it errored. Strong signal the
             suggestion was broken (most often: hallucinated columns).

  ABANDONED  AI suggested SQL but nothing ran on that connection in
             the 5 min after. User likely read it and rejected.

The CONFIRMED / FAILED / ABANDONED split tells you the actual hit rate
of the Copilot. ABANDONED + FAILED together = "user didn't get
value", and any release-blocker for the AI feature should track that.
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tusk.core import meta

HOME = Path.home() / ".tusk"
AI_DB = meta.TUSK_DB  # both lived in their own files before 0.4.38
HISTORY_DB = meta.TUSK_DB

# session_key shape (from src/tusk/studio/routes/ai.py): "<user|anon>:<conn_id>"
SESSION_RE = re.compile(r"^(?P<user>[^:]*):(?P<conn>.+)$")

# AI's "I don't know" leader (set in routes/ai.py few-shots).
HONEST_PREFIX = ("-- ", "-- the schema doesn't include", "-- ask")


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--days", type=int, default=30, help="how far back to look (default 30)")
    p.add_argument("--session", help="restrict to one session_key")
    p.add_argument("--verbose", "-v", action="store_true", help="print every prompt + verdict")
    p.add_argument("--ai-db", default=str(AI_DB), help=f"path to ai_memory.db (default {AI_DB})")
    p.add_argument("--history-db", default=str(HISTORY_DB), help=f"path to history.db (default {HISTORY_DB})")
    return p.parse_args(argv)


def _extract_sql(content: str) -> str | None:
    """Pull the SQL out of an assistant turn. The prompt asks for a JSON
    object with a `sql` field, but the model sometimes ignores that and
    returns raw SQL or markdown-fenced SQL. Handle all three."""
    content = content.strip()
    # 1. JSON object as expected
    if content.startswith("{"):
        try:
            obj = json.loads(content)
            if isinstance(obj, dict) and "sql" in obj:
                return str(obj["sql"]).strip()
        except json.JSONDecodeError:
            pass
    # 2. Markdown fenced
    m = re.search(r"```(?:sql)?\n(.+?)\n```", content, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    # 3. Raw SQL — only if it looks like SQL
    head = content.split("\n", 1)[0].strip().upper()
    if head.startswith(("SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "--")):
        return content
    return None


def _verdict(sql: str | None, suggestion_time: datetime, conn_id: str | None,
             history_rows: list[tuple]) -> tuple[str, dict]:
    """Return (verdict, detail) where detail carries the why-of-failure.

    Verdicts:
      NO_SQL     — assistant turn had no SQL we could parse
      HONEST     — AI returned `-- ` SQL comment (its "I don't know")
      CONFIRMED  — AI suggested, a query ran ≤5min later and succeeded
      FAILED     — AI suggested, a query ran ≤5min later and errored
      ABANDONED  — AI suggested but no query ran on this conn

    Detail dict keys (always present, possibly None):
      ran_sql    — the SQL that actually executed (FAILED + CONFIRMED)
      error      — the database error string (FAILED only)
    """
    detail: dict[str, object] = {"ran_sql": None, "error": None}
    if not sql:
        return "NO_SQL", detail
    if any(sql.lstrip().startswith(p) for p in HONEST_PREFIX):
        return "HONEST", detail
    if not conn_id:
        return "ABANDONED", detail
    window_end = suggestion_time + timedelta(minutes=5)
    # history_rows: [(connection_id, executed_at, status, error, sql), ...]
    for row in history_rows:
        if row[0] != conn_id:
            continue
        try:
            exec_at = datetime.fromisoformat(row[1].replace("Z", "+00:00"))
        except Exception:
            continue
        if exec_at.tzinfo is None:
            exec_at = exec_at.replace(tzinfo=timezone.utc)
        if suggestion_time <= exec_at <= window_end:
            errored = row[2] == "error" or row[3]
            detail["ran_sql"] = row[4]
            if errored:
                detail["error"] = row[3]
                return "FAILED", detail
            return "CONFIRMED", detail
    return "ABANDONED", detail


def main(argv=None):
    args = parse_args(argv)

    ai_path = Path(args.ai_db)
    history_path = Path(args.history_db)
    if not ai_path.exists():
        print(f"warn: {ai_path} does not exist — nothing to report", file=sys.stderr)
        sys.exit(1)

    since = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()

    ai = sqlite3.connect(f"file:{ai_path}?mode=ro", uri=True)
    ai.row_factory = sqlite3.Row

    where_session = "AND session_key = ?" if args.session else ""
    params: tuple = (since,) + ((args.session,) if args.session else ())

    rows = ai.execute(
        f"""SELECT session_key, role, content, created_at FROM conversations
            WHERE created_at >= ? {where_session}
            ORDER BY id ASC""",
        params,
    ).fetchall()
    ai.close()

    if not rows:
        print("No AI conversations in the requested window.")
        return

    # Load history once, in-memory — it's small enough. Pull the SQL
    # text too so FAILED verdicts can surface what actually ran (and
    # the error string).
    hist_rows: list[tuple] = []
    if history_path.exists():
        h = sqlite3.connect(f"file:{history_path}?mode=ro", uri=True)
        hist_rows = h.execute(
            "SELECT connection_id, executed_at, status, error, sql FROM query_history "
            "WHERE executed_at >= ?",
            (since,),
        ).fetchall()
        h.close()

    # Walk conversations in order, pairing every assistant turn with the
    # preceding user prompt (which has timestamp + session).
    verdict_counts: Counter[str] = Counter()
    by_session: dict[str, Counter] = {}
    detail: list[dict] = []

    last_user: sqlite3.Row | None = None
    for r in rows:
        if r["role"] == "user":
            last_user = r
            continue
        if r["role"] != "assistant" or last_user is None:
            continue
        # Pair this assistant turn with last_user.
        session = r["session_key"]
        conn_match = SESSION_RE.match(session)
        conn_id = conn_match.group("conn") if conn_match else None
        try:
            ts = datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
        except Exception:
            ts = datetime.now(timezone.utc)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)

        sql = _extract_sql(r["content"])
        verdict, vdetail = _verdict(sql, ts, conn_id, hist_rows)

        verdict_counts[verdict] += 1
        by_session.setdefault(session, Counter())[verdict] += 1

        # Always keep FAILED + ABANDONED for the post-stats breakdown.
        # ABANDONED is the most actionable signal in practice: it means
        # the AI suggested SQL and the user *looked at it and rejected*.
        # When the user says "AI hallucinates columns", what they
        # really mean is "AI suggested plausible-looking SQL that
        # references things that don't exist, I caught it on review,
        # never ran it". That maps to ABANDONED, not FAILED.
        if verdict in ("FAILED", "ABANDONED") or args.verbose:
            detail.append({
                "session": session,
                "ts": ts.isoformat(),
                "prompt": (last_user["content"] or "")[:200],
                "verdict": verdict,
                "ai_sql": (sql or "")[:400] if sql else None,
                "ran_sql": (vdetail["ran_sql"] or "")[:200] if vdetail["ran_sql"] else None,
                "error": (vdetail["error"] or "")[:200] if vdetail["error"] else None,
            })
        last_user = None

    total = sum(verdict_counts.values())
    if total == 0:
        print("No assistant turns paired with a user prompt found.")
        return

    print("=" * 60)
    print(f"AI Copilot stats — last {args.days} days")
    print(f"AI DB:       {ai_path}")
    print(f"History DB:  {history_path}")
    print("=" * 60)
    print(f"Total prompts:  {total}")
    print(f"Sessions:       {len(by_session)}")
    print()

    bar = lambda n, t: ("█" * int(n / max(t, 1) * 30)).ljust(30)
    for label, key in [
        ("CONFIRMED  (AI suggested, ran OK)", "CONFIRMED"),
        ("HONEST     (AI admitted it can't)", "HONEST"),
        ("ABANDONED  (user didn't run it)  ", "ABANDONED"),
        ("FAILED     (AI suggested, errored)", "FAILED"),
        ("NO_SQL     (assistant returned no SQL)", "NO_SQL"),
    ]:
        n = verdict_counts.get(key, 0)
        pct = (n / total * 100) if total else 0
        print(f"  {label:42}  {n:4d}  {pct:5.1f}%  {bar(n, total)}")

    useful = verdict_counts.get("CONFIRMED", 0) + verdict_counts.get("HONEST", 0)
    print()
    print(f"Hit rate (CONFIRMED + HONEST / total):  {useful}/{total} = {useful/total*100:.1f}%")
    miss = verdict_counts.get("FAILED", 0) + verdict_counts.get("ABANDONED", 0)
    print(f"Miss rate (FAILED + ABANDONED / total): {miss}/{total} = {miss/total*100:.1f}%")
    print()

    if len(by_session) > 1:
        print("By session (top 10 by activity):")
        ranked = sorted(by_session.items(), key=lambda kv: sum(kv[1].values()), reverse=True)
        for session, c in ranked[:10]:
            tot = sum(c.values())
            ok = c.get("CONFIRMED", 0) + c.get("HONEST", 0)
            print(f"  {session:40}  {tot:3d} prompts  {ok/tot*100:5.1f}% hit")
        print()

    # Always surface FAILED — that's the most actionable signal.
    fails = [d for d in detail if d["verdict"] == "FAILED"]
    if fails:
        print(f"FAILED prompts — the {len(fails)} cases where the AI suggested SQL and the run errored:")
        print()
        for d in fails[:20]:
            print(f"  [{d['ts']}]")
            print(f"  prompt:  {d['prompt']}")
            if d['ai_sql']:
                print(f"  ai sql:  {d['ai_sql']}")
            if d['ran_sql'] and d['ran_sql'] != d['ai_sql']:
                print(f"  ran sql: {d['ran_sql']}")
            if d['error']:
                print(f"  error:   {d['error']}")
            print()
        if len(fails) > 20:
            print(f"  ... and {len(fails) - 20} more (rerun with --verbose to see all).")

    # Surface ABANDONED — these are the AI's SQL suggestions the user
    # *read and rejected*. Often the same hallucinated-column pattern
    # as FAILED, just caught before the DB sees it.
    abandoned = [d for d in detail if d["verdict"] == "ABANDONED"]
    if abandoned:
        print()
        print(f"ABANDONED prompts — the {len(abandoned)} cases where the AI suggested SQL but no query ran on that conn within 5 min:")
        print()
        for d in abandoned[:20]:
            print(f"  [{d['ts']}]")
            print(f"  prompt:  {d['prompt']}")
            if d['ai_sql']:
                print(f"  ai sql:  {d['ai_sql']}")
            print()
        if len(abandoned) > 20:
            print(f"  ... and {len(abandoned) - 20} more (rerun with --verbose to see all).")

    if args.verbose:
        print()
        print("Per-prompt detail (all):")
        for d in detail:
            mark = {
                "CONFIRMED": "✓", "HONEST": "·",
                "FAILED": "✗", "ABANDONED": "?",
                "NO_SQL": "—",
            }.get(d["verdict"], "?")
            print(f"  {mark} {d['ts']}  [{d['verdict']:<10}]  {d['prompt']}")


if __name__ == "__main__":
    main()
