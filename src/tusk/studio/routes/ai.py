"""AI Copilot routes — provider config + completion endpoints.

`/api/ai/status` is the only endpoint that always returns 200 — every
other endpoint returns 412 Precondition Failed when no provider is
configured. The frontend reads that and shows the "Configure AI" empty
state instead of a generic error.
"""

from __future__ import annotations

from litestar import Controller, Request, get, post
from litestar.params import Body
from litestar.response import Template

import msgspec

from tusk.core.ai import (
    PROVIDER_DEFAULTS,
    AIConfig,
    OllamaProvider,
    OpenAIProvider,
    AnthropicProvider,
    build_provider,
    compute_suggestions,
    get_provider,
    load_config,
    save_config,
)
from tusk.core.ai_struct import complete_struct
from tusk.core.crypto import is_encrypted
from tusk.core.logging import get_logger
from tusk.studio.routes.base import TuskController


# ──────────────────────── Structured response shapes ────────────────────────


class SQLResponse(msgspec.Struct):
    """Schema-validated reply to /api/ai/sql.

    The model is asked to return JSON with these exact fields. A small
    model that ignores prose-format instructions can still hit this,
    because msgspec rejects anything that doesn't match the schema and
    `complete_struct` retries once with a remediation note.
    """
    sql: str  # the generated PostgreSQL statement, no fences, no comments
    explanation: str  # one-line natural-language summary in user's language
    confidence: str = "medium"  # "high" | "medium" | "low" — model's self-grade


class ExplainResponse(msgspec.Struct):
    """Schema-validated reply to /api/ai/explain."""
    explanation: str  # 2-4 short sentences in user's language
    tables: list[str] = []  # tables the SQL reads/writes
    warnings: list[str] = []  # performance gotchas, locks, full table scans, etc.

log = get_logger("ai_routes")


class AICopilotController(Controller):
    """JSON API for AI Copilot."""

    path = "/api/ai"

    @get("/status")
    async def status(self) -> dict:
        cfg = load_config()
        provider = build_provider(cfg)
        healthy = await provider.health() if provider else False
        return {
            "enabled": cfg.enabled,
            "provider": cfg.provider,
            "model": cfg.model,
            "base_url": cfg.base_url,
            "configured": provider is not None,
            "healthy": healthy,
            "has_api_key": bool(cfg.api_key),
        }

    @post("/config")
    async def update_config(self, data: dict = Body()) -> dict:
        """Save AI config. The api_key field has special handling:
        an empty string keeps the existing encrypted value, any other
        value is taken as plaintext and encrypted before persistence.
        """
        try:
            cfg = AIConfig(
                enabled=bool(data.get("enabled", False)),
                provider=str(data.get("provider", "ollama")),
                base_url=str(data.get("base_url", "")).strip(),
                model=str(data.get("model", "")).strip(),
                # Will be replaced via save_config() if a plaintext key was sent.
                api_key=load_config().api_key,
            )
            plaintext = data.get("api_key")
            if plaintext == "":
                # User explicitly cleared the key.
                save_config(cfg, plaintext_api_key="")
            elif plaintext and not is_encrypted(plaintext):
                save_config(cfg, plaintext_api_key=plaintext)
            else:
                # No new key supplied — keep what's on disk.
                save_config(cfg, plaintext_api_key=None)
            return {"ok": True}
        except Exception as e:
            log.error("Failed to save AI config", error=str(e))
            return {"ok": False, "error": str(e)}

    @post("/test")
    async def test_provider(self, data: dict = Body()) -> dict:
        """Round-trip a tiny prompt against an *unsaved* config — used by
        the settings page to verify the user typed the right thing
        before saving. The API key arrives as plaintext from the form;
        we encrypt for the provider class which expects encrypted input."""
        from tusk.core.crypto import encrypt

        provider_kind = str(data.get("provider", "ollama"))
        base_url = str(data.get("base_url", "")).strip()
        model = str(data.get("model", "")).strip()
        plaintext_key = str(data.get("api_key", ""))
        enc_key = encrypt(plaintext_key) if plaintext_key else ""

        try:
            if provider_kind == "ollama":
                provider = OllamaProvider(base_url, model)
            elif provider_kind in ("openai", "custom"):
                provider = OpenAIProvider(base_url, enc_key, model)
            elif provider_kind == "anthropic":
                provider = AnthropicProvider(base_url, enc_key, model)
            else:
                return {"ok": False, "error": f"unknown provider {provider_kind!r}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

        try:
            text = await provider.complete(
                "Reply with exactly: pong",
                system="You are a connectivity probe. Reply with exactly the word the user asks for.",
                max_tokens=8,
                temperature=0.0,
            )
            return {"ok": True, "response": text[:200]}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    @post("/models")
    async def list_models(self, data: dict = Body()) -> dict:
        """List available models for an *unsaved* provider config —
        powers the model dropdown on the settings page."""
        try:
            provider_kind = str(data.get("provider", "ollama"))
            base_url = str(data.get("base_url", "")).strip()
            api_key = str(data.get("api_key", ""))
            if provider_kind == "ollama":
                provider = OllamaProvider(base_url, "any")
            elif provider_kind in ("openai", "custom"):
                from tusk.core.crypto import encrypt
                enc = encrypt(api_key) if api_key else ""
                provider = OpenAIProvider(base_url, enc, "any")
            elif provider_kind == "anthropic":
                from tusk.core.crypto import encrypt
                enc = encrypt(api_key) if api_key else ""
                provider = AnthropicProvider(base_url, enc, "any")
            else:
                return {"models": []}
            return {"models": await provider.list_models()}
        except Exception as e:
            return {"error": str(e), "models": []}

    @post("/sql")
    async def text_to_sql(self, request: Request, data: dict = Body()) -> dict:
        """Generate SQL from a natural-language prompt + real schema +
        recent conversation memory.
        """
        from tusk.core import ai_memory

        provider = get_provider()
        if not provider:
            return {"error": "AI provider not configured", "code": 412}

        prompt = str(data.get("prompt", "")).strip()
        if not prompt:
            return {"error": "prompt required", "code": 400}
        # Cap to keep token cost (and ai_memory.db size) bounded.
        if len(prompt) > 8_000:
            return {"error": "prompt too long (max 8000 chars)", "code": 400}

        connection_id = data.get("connection_id")
        schema_text = ""
        if connection_id:
            schema_text = await _schema_summary(connection_id, prompt)

        # Conversation memory keyed by (user, connection). _session_key
        # returns None when no stable identity is available — skip
        # memory in that case rather than risk cross-session bleed.
        session_key = _session_key(request, connection_id)
        history_text = ai_memory.format_for_prompt(
            ai_memory.get_recent_turns(session_key, limit=8) if session_key else [],
            max_chars=1200,
        )

        # Few-shot examples — small models (qwen 3b/7b) ignore format
        # instructions written in prose; concrete examples FIX it.
        # Two shots: one EN, one ES, one with a "table doesn't exist"
        # case so the model learns to admit when it can't answer.
        few_shots = (
            "### Examples\n"
            "Question: List the 5 most recent orders\n"
            "→ {\"sql\":\"SELECT id, customer_id, total, created_at FROM orders ORDER BY created_at DESC LIMIT 5\","
            "\"explanation\":\"Returns the five most recently created orders.\","
            "\"confidence\":\"high\"}\n\n"
            "Pregunta: cuántas discotecas hay por sector\n"
            "→ {\"sql\":\"SELECT sector, COUNT(*) AS total FROM geo_pois "
            "WHERE subcategoria = 'nightclub' GROUP BY sector ORDER BY total DESC\","
            "\"explanation\":\"Cuenta los POIs marcados como nightclub agrupados por sector.\","
            "\"confidence\":\"high\"}\n\n"
            "Question: which patients have high blood pressure\n"
            "→ {\"sql\":\"-- the schema doesn't include a patients or vitals table; "
            "ask which dataset to use\",\"explanation\":\"No patient or vital-sign tables in this schema. "
            "Tell me which connection holds the medical data.\",\"confidence\":\"low\"}"
        )

        system = (
            "You are a SQL assistant for the Tusk data platform. "
            "Generate concise, correct PostgreSQL by default unless the "
            "user specifies a different dialect. "
            "ONLY reference tables and columns that appear in the schema "
            "reference below — never invent table or column names. If "
            "the user's question can't be answered from the available "
            "schema, return a `sql` field starting with `-- ` that "
            "explains what's missing, and set `confidence` to `low`. "
            "Match the language of the user's prompt for the "
            "`explanation` field. SQL keywords stay in English. "
            "The `sql` field must be ONLY the SQL statement — no fences, "
            "no leading prose. Set `confidence` honestly: high when the "
            "schema directly answers the question, medium when you're "
            "guessing at table relationships, low when you can't fully "
            "answer.\n\n" + few_shots
        )
        parts: list[str] = []
        if history_text:
            parts.append(f"### Previous conversation\n{history_text}")
        if schema_text:
            parts.append(schema_text)
        parts.append(f"### Question\n{prompt}")
        full_prompt = "\n\n".join(parts)

        try:
            response = await complete_struct(
                provider, full_prompt, SQLResponse,
                system=system, max_tokens=800, temperature=0.2,
            )
            sql_text = response.sql.strip()
            explanation_text = response.explanation.strip()
            confidence = response.confidence
            raw_for_memory = msgspec.json.encode(response).decode()
        except Exception as e:
            log.warning("structured AI call failed", error=str(e))
            return {"error": str(e), "code": 502}

        # Persist this exchange so follow-ups have context. Only when
        # we have a real, identity-bound session key — anonymous
        # callers don't get memory.
        if session_key:
            try:
                ai_memory.add_turn(session_key, "user", prompt)
                ai_memory.add_turn(session_key, "assistant", raw_for_memory)
            except Exception:
                pass

        return {
            "sql": sql_text,
            "explanation": explanation_text,
            "confidence": confidence,
            "session_key": session_key,
            "schema_chars": len(schema_text),
        }

    @post("/explain")
    async def explain_sql(self, request: Request, data: dict = Body()) -> dict:
        from tusk.core import ai_memory

        provider = get_provider()
        if not provider:
            return {"error": "AI provider not configured", "code": 412}

        sql = str(data.get("sql", "")).strip()
        if not sql:
            return {"error": "sql required", "code": 400}
        if len(sql) > 16_000:
            return {"error": "sql too long (max 16000 chars)", "code": 400}

        connection_id = data.get("connection_id")
        # The "prompt" for schema-keyword matching is the SQL itself —
        # the model sees what tables it touches.
        schema_text = await _schema_summary(connection_id, sql) if connection_id else ""
        session_key = _session_key(request, connection_id)
        history_text = ai_memory.format_for_prompt(
            ai_memory.get_recent_turns(session_key, limit=4) if session_key else [],
            max_chars=600,
        )

        system = (
            "Explain this SQL in 2-4 short sentences. List the tables "
            "the query reads or writes (`tables`) and any performance "
            "gotchas (`warnings`) — full table scans, missing index "
            "hints, lock risks, etc. Respond in the same language the "
            "user has been using; default to English if you can't tell. "
            "Set `tables` and `warnings` to empty arrays if there's "
            "nothing to add — they are required fields."
        )
        parts: list[str] = []
        if history_text:
            parts.append(f"### Previous conversation\n{history_text}")
        if schema_text:
            parts.append(schema_text)
        parts.append(f"### SQL\n```sql\n{sql}\n```")
        prompt = "\n\n".join(parts)

        try:
            response = await complete_struct(
                provider, prompt, ExplainResponse,
                system=system, max_tokens=400, temperature=0.2,
            )
            explanation_text = response.explanation.strip()
            raw_for_memory = msgspec.json.encode(response).decode()
        except Exception as e:
            log.warning("structured AI explain failed", error=str(e))
            return {"error": str(e), "code": 502}

        if session_key:
            try:
                ai_memory.add_turn(session_key, "user", f"Explain this SQL:\n{sql}")
                ai_memory.add_turn(session_key, "assistant", raw_for_memory)
            except Exception:
                pass

        return {
            "explanation": explanation_text,
            "tables": response.tables,
            "warnings": response.warnings,
        }

    @post("/clear-memory")
    async def clear_memory(self, request: Request, data: dict = Body()) -> dict:
        """Drop the conversation memory for the current session/connection."""
        from tusk.core import ai_memory

        connection_id = data.get("connection_id") if isinstance(data, dict) else None
        session_key = _session_key(request, connection_id)
        if not session_key:
            return {"ok": True, "removed": 0}
        removed = ai_memory.clear_session(session_key)
        return {"ok": True, "removed": removed}

    @get("/suggest")
    async def suggest(self, request: Request) -> dict | Template:
        """Homepage suggestions.

        - HTMX requests get rendered HTML for direct swap into the panel.
        - Plain JSON callers (curl, the cmdk palette) get the raw list.
        """
        try:
            suggestions = await compute_suggestions()
        except Exception as e:
            log.warning("compute_suggestions failed", error=str(e))
            suggestions = []

        if request.headers.get("hx-request") == "true":
            return Template(
                "partials/ai_suggestions.html",
                context={"suggestions": suggestions},
            )
        return {"suggestions": suggestions}


# ──────────────────────── Settings page ────────────────────────


class AISettingsPageController(TuskController):
    """The /settings/ai page itself (HTML, not JSON)."""

    path = "/settings/ai"

    @get("/")
    async def page(self, request: Request) -> Template:
        cfg = load_config()
        return self.render(
            "settings_ai.html",
            active_page="settings_ai",
            ai_config={
                "enabled": cfg.enabled,
                "provider": cfg.provider or "ollama",
                "base_url": cfg.base_url or PROVIDER_DEFAULTS["ollama"]["base_url"],
                "model": cfg.model or PROVIDER_DEFAULTS["ollama"]["model"],
                "has_api_key": bool(cfg.api_key),
            },
            provider_defaults=PROVIDER_DEFAULTS,
        )


# ──────────────────────── Helpers ────────────────────────


def _session_key(request: Request, connection_id: str | None) -> str | None:
    """Stable session identifier for AI memory.

    Multi-user: keyed on the actual user id + connection so each user
    has their own thread per database. Single-user: keyed on the CSRF
    cookie (which the middleware sets on every response) so a single
    browser session stays consistent across reloads but a different
    browser starts fresh.

    Returns `None` when no stable identity can be built — caller MUST
    skip persisting memory in that case. Before v0.4.8.1 the fallback
    was the literal string `"anon"`, which collapsed every cookie-less
    request into a single shared thread (memory bleed across
    incognito tabs and unrelated visitors in single-user mode).
    """
    cid = connection_id or "_no_conn"
    try:
        from tusk.core.auth import get_session, get_user_by_id
        from tusk.core.config import get_config

        config = get_config()
        if config.auth_mode == "multi":
            session_id = request.cookies.get("tusk_session")
            if session_id:
                session = get_session(session_id)
                if session:
                    user = get_user_by_id(session.user_id)
                    if user:
                        return f"u:{user.id}:c:{cid}"
            # Multi-user without a session cookie shouldn't even reach
            # this code path (auth middleware rejects), but if it does
            # we refuse to invent a key.
            return None
    except Exception:
        pass
    csrf = request.cookies.get("tusk_csrf")
    if not csrf or len(csrf) < 16:
        # No CSRF cookie yet (first request from a brand-new client).
        # Skip persistence rather than cross-pollinate memory.
        return None
    return f"csrf:{csrf[:16]}:c:{cid}"


async def _schema_summary(connection_id: str | None, prompt: str = "") -> str:
    """Build a focused schema description for the model.

    Old version dumped the first 30 tables blindly and the model
    hallucinated tables that didn't exist. New version:

    1. Lists ALL table names with row counts and column counts (cheap,
       just one row per table) — so the model knows what exists.
    2. For tables whose name (or any column name) appears in `prompt`,
       OR are referenced via FK from a matched table, includes the
       full column definitions + primary keys + foreign keys.
    3. Caps the detailed section at ~3000 chars so the prompt stays
       bounded on a 8k-context model.

    The result reads like a real schema reference rather than a
    truncated guess. The model can pick the right table even when the
    user uses Spanish or fuzzy names.
    """
    if not connection_id:
        return ""

    try:
        from tusk.core.connection import get_connection
        from tusk.engines.postgres import execute_query, get_row_counts

        conn = get_connection(connection_id)
        if not conn or conn.type != "postgres":
            return ""

        # Single pass: pull table+column info + PK/FK from pg_catalog.
        sql = """
            SELECT
                ns.nspname  AS schema,
                cl.relname  AS table_name,
                att.attname AS column_name,
                pg_catalog.format_type(att.atttypid, att.atttypmod) AS data_type,
                CASE WHEN pk.contype = 'p' THEN 1 ELSE 0 END AS is_pk,
                fk.confrelid::regclass::text AS fk_to_table,
                fk_col.attname AS fk_to_column,
                att.attnotnull AS notnull
            FROM pg_attribute att
            JOIN pg_class cl ON cl.oid = att.attrelid
            JOIN pg_namespace ns ON ns.oid = cl.relnamespace
            LEFT JOIN pg_constraint pk
                ON pk.conrelid = cl.oid
                AND pk.contype = 'p'
                AND att.attnum = ANY(pk.conkey)
            LEFT JOIN pg_constraint fk
                ON fk.conrelid = cl.oid
                AND fk.contype = 'f'
                AND att.attnum = ANY(fk.conkey)
            LEFT JOIN pg_attribute fk_col
                ON fk_col.attrelid = fk.confrelid
                AND fk_col.attnum = ANY(fk.confkey)
            WHERE cl.relkind = 'r'
              AND att.attnum > 0
              AND NOT att.attisdropped
              AND ns.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY ns.nspname, cl.relname, att.attnum
        """
        result = await execute_query(conn, sql)
        if result.error or not result.rows:
            return ""

        # Group by qualified table name.
        tables: dict[str, dict] = {}
        for row in result.rows:
            schema, tname, col, dtype, is_pk, fk_to, fk_col, notnull = row
            qname = f"{schema}.{tname}" if schema and schema != "public" else tname
            t = tables.setdefault(qname, {"cols": [], "pks": [], "fks": []})
            t["cols"].append({"name": col, "type": dtype, "nn": bool(notnull)})
            if is_pk:
                t["pks"].append(col)
            if fk_to and fk_col:
                # `fk_to` is the qualified relname. Strip "public." for parity.
                fk_table = fk_to.replace("public.", "") if fk_to else fk_to
                t["fks"].append({"col": col, "to_table": fk_table, "to_col": fk_col})

        # Row counts (best-effort — pg_stat_user_tables, fast).
        try:
            row_counts = await get_row_counts(conn)
        except Exception:
            row_counts = {}

        # Pick which tables to detail. Rules:
        # - Always: tables matching keywords from `prompt`
        # - Always: tables FK-referenced by a matched table (1 hop)
        # - Top up with the largest-by-row-count tables until budget fills
        prompt_lower = (prompt or "").lower()
        # Tokenize the prompt into words ≥3 chars; ignore common SQL keywords.
        import re
        STOP = {"the", "and", "for", "from", "with", "give", "show", "list",
                "select", "where", "order", "group", "limit", "table",
                "tabla", "tablas", "muestra", "dame", "lista", "que",
                "todos", "todas", "esta", "esto", "como", "para",
                "more", "less"}
        tokens = {t for t in re.findall(r"[a-z_]{3,}", prompt_lower) if t not in STOP}

        def matches_prompt(tname: str, t: dict) -> bool:
            tn = tname.lower()
            if any(tok in tn for tok in tokens):
                return True
            for c in t["cols"]:
                cn = c["name"].lower()
                if any(tok in cn for tok in tokens):
                    return True
            return False

        priority_set: set[str] = {n for n, t in tables.items() if matches_prompt(n, t)}
        # 1-hop FK expansion
        expanded: set[str] = set(priority_set)
        for n in list(priority_set):
            for fk in tables[n]["fks"]:
                target = fk["to_table"]
                if target in tables:
                    expanded.add(target)
        priority_set = expanded

        # Build the output.
        out_lines: list[str] = []
        out_lines.append("### Available tables")
        # Sort all tables by row count desc for the overview section.
        all_sorted = sorted(
            tables.items(),
            key=lambda kv: row_counts.get(kv[0], -1),
            reverse=True,
        )
        for tname, t in all_sorted[:120]:
            rc = row_counts.get(tname)
            rc_str = f" ~{rc:,} rows" if isinstance(rc, int) else ""
            out_lines.append(f"- {tname} ({len(t['cols'])} cols{rc_str})")

        # Detail section for matched + FK-related tables, plus top by rows
        # to fill any remaining budget.
        out_lines.append("\n### Detailed schema")
        budget_chars = 3000
        used = 0

        def emit_table(tname: str, t: dict) -> str:
            cols_lines = []
            for c in t["cols"]:
                marker = " PK" if c["name"] in t["pks"] else ""
                fk_match = next((f for f in t["fks"] if f["col"] == c["name"]), None)
                if fk_match:
                    marker += f" -> {fk_match['to_table']}.{fk_match['to_col']}"
                nn = " NOT NULL" if c["nn"] else ""
                cols_lines.append(f"  - {c['name']} {c['type']}{nn}{marker}")
            return f"\n{tname}\n" + "\n".join(cols_lines)

        # Detail priority tables first.
        seen: set[str] = set()
        for tname in priority_set:
            t = tables.get(tname)
            if not t:
                continue
            block = emit_table(tname, t)
            if used + len(block) > budget_chars:
                break
            out_lines.append(block)
            used += len(block)
            seen.add(tname)

        # If no prompt keywords matched anything, fall back to top-N by rows
        # so the model still sees ~8 detailed tables it can rely on.
        if not seen:
            for tname, t in all_sorted[:8]:
                block = emit_table(tname, t)
                if used + len(block) > budget_chars:
                    break
                out_lines.append(block)
                used += len(block)
                seen.add(tname)

        return "\n".join(out_lines)
    except Exception as e:
        # Surface the failure to the model rather than silently feeding
        # an empty schema — that used to make the model hallucinate
        # wildly because the system prompt said "ONLY reference tables
        # that appear below" and "below" was empty. With this comment
        # the model knows the schema is unknown and asks the user to
        # specify, instead of inventing.
        log.warning("_schema_summary failed", error=str(e))
        return f"### Schema\n# (schema fetch failed: {str(e)[:200]} — ask the user which tables they mean before generating SQL)"


def _parse_sql_response(text: str) -> tuple[str, str]:
    """Extract a fenced ```sql block + the trailing `-- explanation` line.

    Models often deviate from the requested format — degrade gracefully
    by returning the whole response as `sql` if no fences are found.
    """
    import re

    m = re.search(r"```(?:sql)?\s*\n(.*?)```", text, re.DOTALL | re.IGNORECASE)
    if not m:
        return text.strip(), ""
    sql = m.group(1).strip()
    after = text[m.end():].strip()
    expl_lines: list[str] = []
    for line in after.splitlines():
        line = line.strip()
        if line.startswith("--"):
            expl_lines.append(line[2:].strip())
        elif line:
            expl_lines.append(line)
    return sql, " ".join(expl_lines).strip()
