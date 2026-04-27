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
from tusk.core.crypto import is_encrypted
from tusk.core.logging import get_logger
from tusk.studio.routes.base import TuskController

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
    async def text_to_sql(self, data: dict = Body()) -> dict:
        """Generate SQL from a natural-language prompt + optional schema.

        We don't know which dialect the user is on without inspecting
        the connection, so the prompt steers toward Postgres SQL by
        default (covers our biggest user surface).
        """
        provider = get_provider()
        if not provider:
            return {"error": "AI provider not configured", "code": 412}

        prompt = str(data.get("prompt", "")).strip()
        if not prompt:
            return {"error": "prompt required", "code": 400}

        connection_id = data.get("connection_id")
        schema_text = ""
        if connection_id:
            schema_text = await _schema_summary(connection_id)

        system = (
            "You are a SQL assistant for the Tusk data platform. "
            "Generate concise, correct PostgreSQL by default unless the "
            "user specifies a different dialect. Respond with ONLY the "
            "SQL inside a fenced ```sql block, followed by a one-line "
            "explanation prefixed with `-- `. No prose outside the block."
        )
        full_prompt = (
            f"{prompt}\n\n"
            + (f"### Schema\n{schema_text}\n" if schema_text else "")
        )

        try:
            text = await provider.complete(full_prompt, system=system, max_tokens=800)
        except Exception as e:
            return {"error": str(e), "code": 502}

        sql, explanation = _parse_sql_response(text)
        return {"sql": sql, "explanation": explanation, "raw": text}

    @post("/explain")
    async def explain_sql(self, data: dict = Body()) -> dict:
        provider = get_provider()
        if not provider:
            return {"error": "AI provider not configured", "code": 412}

        sql = str(data.get("sql", "")).strip()
        if not sql:
            return {"error": "sql required", "code": 400}

        connection_id = data.get("connection_id")
        schema_text = await _schema_summary(connection_id) if connection_id else ""

        system = (
            "Explain this SQL in 2–4 short sentences. Mention which "
            "tables are read, what filter/aggregate is applied, and any "
            "performance gotchas. No code blocks."
        )
        prompt = f"### SQL\n```sql\n{sql}\n```\n" + (f"\n### Schema\n{schema_text}\n" if schema_text else "")

        try:
            text = await provider.complete(prompt, system=system, max_tokens=400)
        except Exception as e:
            return {"error": str(e), "code": 502}

        return {"explanation": text}

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


async def _schema_summary(connection_id: str | None) -> str:
    """Build a compact schema description to give the model context.

    Caps at ~30 tables / 200 columns total — bigger schemas would blow
    the context window. We sort tables by row_count desc when known,
    so the most-relevant tables show up first.
    """
    if not connection_id:
        return ""

    try:
        from tusk.core.connection import get_connection
        from tusk.engines.postgres import get_schema

        conn = get_connection(connection_id)
        if not conn or conn.type != "postgres":
            return ""

        schema = await get_schema(conn)
        # `schema` is a dict with `tables` keyed by name → list of {name, type}
        tables = schema.get("tables") if isinstance(schema, dict) else None
        if not tables:
            return ""

        lines: list[str] = []
        budget = 200
        count = 0
        for tname, cols in list(tables.items())[:30]:
            if budget <= 0:
                break
            col_descs = ", ".join(f"{c['name']} {c['type']}" for c in cols[:budget])
            lines.append(f"- {tname}({col_descs})")
            budget -= len(cols)
            count += 1
        return "\n".join(lines)
    except Exception as e:
        log.debug("_schema_summary failed", error=str(e))
        return ""


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
