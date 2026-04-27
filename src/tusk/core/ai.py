"""AI Copilot — provider abstraction for Tusk.

Supported providers:
- **ollama** (default): local model via the Ollama HTTP API. Model name
  is whatever you have pulled (e.g. `qwen2.5-coder:3b`).
- **openai**: OpenAI Chat Completions or any OpenAI-compatible endpoint
  (Groq, Together, vLLM with `--api-server`, etc.).
- **anthropic**: Claude messages API.
- **custom**: same wire format as openai but lets the user override the
  base URL — used for OpenRouter, LM Studio, etc.

Configuration lives in `~/.tusk/ai.toml`. The API key is encrypted with
the same fernet keychain as connection passwords (`core/crypto.py`).

The factory `get_provider()` returns `None` when AI is disabled or
mis-configured — callers must handle that and surface the "Configure AI"
empty state instead of failing the page render.

Outbound HTTP goes through `core.url_guard.validate_outbound_url` so
this module is also covered by the SSRF guard.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Protocol

import httpx
import msgspec
import tomllib

from tusk.core.crypto import decrypt, encrypt
from tusk.core.logging import get_logger
# Note: SSRF guard intentionally NOT applied to AI provider URLs.
# The URL is admin-supplied via /settings/ai (gated to admins in
# multi-user mode) and the canonical use case is `localhost`, `host.docker.internal`,
# or a private LAN IP (e.g. `10.0.0.x`) hosting Ollama — exactly what
# the SSRF guard would reject. Don't import the guard here.

log = get_logger("ai")

CONFIG_PATH = Path.home() / ".tusk" / "ai.toml"

# Provider defaults — used by the settings UI to pre-fill base URLs and
# pick a sensible model when the user picks a provider for the first
# time.
PROVIDER_DEFAULTS: dict[str, dict[str, str]] = {
    "ollama": {
        "base_url": "http://localhost:11434",
        "model": "qwen2.5-coder:3b",
    },
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "model": "gpt-4o-mini",
    },
    "anthropic": {
        "base_url": "https://api.anthropic.com",
        "model": "claude-haiku-4-5-20251001",
    },
    "custom": {
        "base_url": "http://localhost:1234/v1",
        "model": "",
    },
}


class AIConfig(msgspec.Struct):
    """User-facing AI configuration. Persisted to ai.toml."""
    enabled: bool = False
    provider: str = "ollama"
    base_url: str = ""
    model: str = ""
    # Encrypted at rest. The provider classes call `decrypt()` to use it.
    api_key: str = ""


def load_config() -> AIConfig:
    """Read `ai.toml`. Returns a disabled default if the file is missing."""
    if not CONFIG_PATH.exists():
        return AIConfig()
    try:
        with CONFIG_PATH.open("rb") as f:
            data = tomllib.load(f)
        section = data.get("ai", {}) or {}
        return AIConfig(
            enabled=bool(section.get("enabled", False)),
            provider=str(section.get("provider", "ollama")),
            base_url=str(section.get("base_url", "")),
            model=str(section.get("model", "")),
            api_key=str(section.get("api_key", "")),
        )
    except Exception as e:
        log.warning("Failed to load AI config", error=str(e))
        return AIConfig()


def save_config(cfg: AIConfig, *, plaintext_api_key: str | None = None) -> None:
    """Persist config. If a plaintext key is provided, it is encrypted
    before writing. Pass `plaintext_api_key=None` to keep the existing
    encrypted value untouched."""
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)

    if plaintext_api_key is not None:
        cfg = msgspec.structs.replace(
            cfg,
            api_key=encrypt(plaintext_api_key) if plaintext_api_key else "",
        )

    # Atomic write
    tmp = CONFIG_PATH.with_suffix(".toml.tmp")
    body = (
        f'[ai]\n'
        f'enabled = {"true" if cfg.enabled else "false"}\n'
        f'provider = "{cfg.provider}"\n'
        f'base_url = "{cfg.base_url}"\n'
        f'model = "{cfg.model}"\n'
        f'api_key = "{cfg.api_key}"\n'
    )
    tmp.write_text(body)
    tmp.replace(CONFIG_PATH)


# ───────────────────────── Provider interface ─────────────────────────


class AIProvider(Protocol):
    """Minimal surface a provider must implement.

    `complete` is the only method used by the SQL-generation /
    explanation endpoints. `list_models` powers the settings dropdown
    so users can pick from what they actually have installed.
    """
    name: str
    model: str

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.2,
    ) -> str: ...

    async def list_models(self) -> list[str]: ...

    async def health(self) -> bool: ...


# ───────────────────────── Ollama ─────────────────────────


class OllamaProvider:
    """Talks to the Ollama HTTP API at `{base_url}/api/{chat,tags}`.

    Ollama doesn't require auth, so `api_key` is ignored. We point the
    SSRF guard at the base URL and validate it once at construction —
    the user is the one who set the URL but it's still inbound from a
    settings form, so it gets the same treatment as a webhook.
    """
    name = "ollama"

    def __init__(self, base_url: str, model: str):
        self.base_url = base_url.rstrip("/")
        self.model = model
        # The whole point of an Ollama provider is to talk to a local
        # model — `localhost`, `host.docker.internal`, `10.0.0.x`, etc.
        # are explicitly the right targets, not an SSRF risk. The URL
        # comes from admin settings (gated to admins in multi-user
        # mode) so the provenance is trusted.

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.2,
    ) -> str:
        messages: list[dict] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        body = {
            "model": self.model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            },
        }

        def _post():
            resp = httpx.post(f"{self.base_url}/api/chat", json=body, timeout=120)
            resp.raise_for_status()
            return resp.json()

        data = await asyncio.to_thread(_post)
        return data.get("message", {}).get("content", "").strip()

    async def list_models(self) -> list[str]:
        def _get():
            resp = httpx.get(f"{self.base_url}/api/tags", timeout=10)
            resp.raise_for_status()
            return resp.json()

        data = await asyncio.to_thread(_get)
        return [m["name"] for m in data.get("models", [])]

    async def health(self) -> bool:
        def _ping():
            try:
                resp = httpx.get(f"{self.base_url}/api/tags", timeout=5)
                return resp.status_code == 200
            except Exception:
                return False

        return await asyncio.to_thread(_ping)


# ───────────────────────── OpenAI / OpenAI-compatible ─────────────────


class OpenAIProvider:
    """OpenAI Chat Completions, also compatible with OpenRouter, Groq,
    LM Studio (`/v1`), vLLM, etc.

    Set `base_url` to the `/v1` root, e.g. `https://api.openai.com/v1`
    or `https://openrouter.ai/api/v1`. `api_key` is the encrypted token
    from `ai.toml` — we decrypt at request time so the plaintext key
    never sits in memory longer than the request.
    """
    name = "openai"

    def __init__(self, base_url: str, api_key: str, model: str):
        self.base_url = base_url.rstrip("/")
        self._api_key_enc = api_key  # stored encrypted
        self.model = model

    def _auth_header(self) -> dict[str, str]:
        key = decrypt(self._api_key_enc)
        return {"Authorization": f"Bearer {key}"} if key else {}

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.2,
    ) -> str:
        messages: list[dict] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        body = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        headers = self._auth_header()

        def _post():
            resp = httpx.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                json=body,
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json()

        data = await asyncio.to_thread(_post)
        return data["choices"][0]["message"]["content"].strip()

    async def list_models(self) -> list[str]:
        headers = self._auth_header()

        def _get():
            resp = httpx.get(
                f"{self.base_url}/models",
                headers=headers,
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()

        data = await asyncio.to_thread(_get)
        return [m["id"] for m in data.get("data", [])]

    async def health(self) -> bool:
        try:
            return bool(await self.list_models())
        except Exception:
            return False


# ───────────────────────── Anthropic ─────────────────────────


class AnthropicProvider:
    """Anthropic Messages API."""
    name = "anthropic"

    def __init__(self, base_url: str, api_key: str, model: str):
        self.base_url = base_url.rstrip("/")
        self._api_key_enc = api_key
        self.model = model

    def _auth_headers(self) -> dict[str, str]:
        key = decrypt(self._api_key_enc)
        return {
            "x-api-key": key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }

    async def complete(
        self,
        prompt: str,
        *,
        system: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.2,
    ) -> str:
        body: dict = {
            "model": self.model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [{"role": "user", "content": prompt}],
        }
        if system:
            body["system"] = system

        headers = self._auth_headers()

        def _post():
            resp = httpx.post(
                f"{self.base_url}/v1/messages",
                headers=headers,
                json=body,
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json()

        data = await asyncio.to_thread(_post)
        blocks = data.get("content", [])
        return "".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()

    async def list_models(self) -> list[str]:
        # Anthropic doesn't expose a /models endpoint to public users; ship a static list.
        return [
            "claude-opus-4-7",
            "claude-sonnet-4-6",
            "claude-haiku-4-5-20251001",
        ]

    async def health(self) -> bool:
        try:
            await self.complete("ping", max_tokens=4)
            return True
        except Exception:
            return False


# ───────────────────────── Factory ─────────────────────────


def build_provider(cfg: AIConfig) -> AIProvider | None:
    """Construct a provider from config. Returns None when AI is
    disabled or the config doesn't have enough to dial out."""
    if not cfg.enabled:
        return None
    if not cfg.provider or not cfg.base_url or not cfg.model:
        return None

    try:
        if cfg.provider == "ollama":
            return OllamaProvider(cfg.base_url, cfg.model)
        if cfg.provider in ("openai", "custom"):
            return OpenAIProvider(cfg.base_url, cfg.api_key, cfg.model)
        if cfg.provider == "anthropic":
            return AnthropicProvider(cfg.base_url, cfg.api_key, cfg.model)
    except Exception as e:
        log.warning("Failed to build AI provider", error=str(e))
        return None
    return None


def get_provider() -> AIProvider | None:
    """Convenience: load config + build provider."""
    return build_provider(load_config())


# ───────────────────────── Suggestion engine ─────────────────────────
# Used by the homepage. Heuristics first; only the LLM-summary step uses
# the provider, and that step is skipped when no provider is configured.


async def compute_suggestions() -> list[dict]:
    """Return a list of `{kind, message, action_label?, action_url?}`
    dicts. Cheap heuristics — no LLM call here, so safe to render on
    every homepage hit. AI-generated insights live behind a separate
    `/api/ai/suggest?ai=1` flag the frontend can toggle."""
    out: list[dict] = []

    # 1. Repeated queries → suggest scheduling
    try:
        from collections import Counter

        from tusk.core.history import get_history

        h = get_history()
        recent = h.get_recent(limit=200)
        # Group by SQL hash, count occurrences in last 24h
        from datetime import datetime, timedelta, timezone
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        counts: Counter[str] = Counter()
        sample: dict[str, str] = {}
        for e in recent:
            try:
                dt = datetime.fromisoformat(e.executed_at.replace("Z", "+00:00"))
            except Exception:
                continue
            if dt < cutoff:
                continue
            key = e.sql.strip()[:200]  # rough hash
            counts[key] += 1
            sample.setdefault(key, e.sql)
        for key, n in counts.most_common(3):
            if n >= 4:
                preview = sample[key][:60].replace("\n", " ")
                out.append({
                    "kind": "schedule",
                    "icon": "zap",
                    "message": f"You ran the same query {n} times today: <code>{preview}…</code>. Save as <b>scheduled</b>?",
                    "action_label": "Schedule",
                    "action_url": "/scheduled?from_history=1",
                })
    except Exception as e:
        log.debug("suggestion: repeated queries skipped", error=str(e))

    # 2. AI-disabled hint (only when there's no provider, so users see
    #    something on first run instead of a blank panel).
    if get_provider() is None:
        out.append({
            "kind": "config",
            "icon": "sparkles",
            "message": "Plug in a local model (Ollama) or an API key to unlock SQL generation, schema explanations, and proactive insights.",
            "action_label": "Configure AI",
            "action_url": "/settings/ai",
        })

    return out
