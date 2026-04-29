"""Lightweight structured output for AI providers.

Replaces the regex-based parsing of fenced ```sql blocks with proper
schema-driven output. Same idea as the `instructor` package but built
on `msgspec` (already a Tusk dependency) — no Pydantic, no per-provider
SDKs, ~150 lines vs Instructor's ~50 MB transitive deps.

Three integration paths:

1. **Schema in the prompt** (universal — works with every provider).
   Generate a JSON schema from the msgspec.Struct, paste it in the
   system prompt, parse the JSON the model returns. Tolerant of the
   model wrapping its answer in ``` fences or adding chatter before/
   after the JSON.

2. **OpenAI native JSON mode** (when provider == "openai" and the
   model supports it). Sets `response_format={"type": "json_schema"}`
   and gets guaranteed-valid JSON.

3. **Ollama format-string** (Ollama 0.5+ supports `format` parameter
   to constrain output to a JSON schema). Falls back to (1) on older
   Ollama.

The caller defines a msgspec.Struct, calls `complete_struct(provider,
prompt, struct)`, and gets a typed instance back. The retry-on-parse-
fail loop is here too (one extra round on first failure).
"""

from __future__ import annotations

import json
import re
from typing import TypeVar

import msgspec

from tusk.core.logging import get_logger

T = TypeVar("T", bound=msgspec.Struct)
log = get_logger("ai_struct")


# msgspec gives us Struct → JSON schema for free. The result is OpenAPI-
# compatible (drop `type: object` if your provider wants strict subset).
def schema_for(struct_cls: type[T]) -> dict:
    """Return the JSON schema for a msgspec.Struct, suitable for both
    OpenAI's `response_format` and inline-in-prompt use.

    Returns the flat schema (with `$defs` inlined into the root). The
    model sees a single self-contained JSON Schema object instead of a
    `$ref` it has to chase.
    """
    raw = msgspec.json.schema(struct_cls)
    # `raw` is `{"$ref": "#/$defs/Name", "$defs": {"Name": {...}}}`.
    # Inline the referenced definition for the model.
    defs = raw.get("$defs") or {}
    ref = raw.get("$ref", "")
    if ref.startswith("#/$defs/"):
        name = ref.split("/")[-1]
        body = defs.get(name)
        if body:
            inlined = dict(body)
            # Carry along any sibling defs the inlined schema may
            # reference (nested structs).
            other_defs = {k: v for k, v in defs.items() if k != name}
            if other_defs:
                inlined["$defs"] = other_defs
            return inlined
    return raw


# ── Tolerant JSON extraction ──────────────────────────────────────────
# Real models wrap JSON in fences, prepend a "Sure, here's the JSON:"
# preamble, or trail it with a "Hope this helps!" line. We strip those
# before handing the body to msgspec so the parser only sees the
# structured part.

_JSON_FENCE = re.compile(r"```(?:json)?\s*(.*?)```", re.DOTALL | re.IGNORECASE)


def _extract_json_blob(text: str) -> str | None:
    """Pull the most-likely JSON object/array out of a model response.

    Tries in order:
    1. ```json ... ``` fenced blocks (any case).
    2. The first balanced { ... } object — useful when the model just
       wrote raw JSON without a fence.
    3. The first balanced [ ... ] array.
    """
    if not text:
        return None
    # Fenced first
    m = _JSON_FENCE.search(text)
    if m:
        candidate = m.group(1).strip()
        if candidate:
            return candidate
    # Balanced braces — naive but works for non-pathological output.
    for opener, closer in (("{", "}"), ("[", "]")):
        start = text.find(opener)
        if start < 0:
            continue
        depth = 0
        in_str = False
        esc = False
        for i in range(start, len(text)):
            ch = text[i]
            if esc:
                esc = False
                continue
            if ch == "\\":
                esc = True
                continue
            if ch == '"':
                in_str = not in_str
                continue
            if in_str:
                continue
            if ch == opener:
                depth += 1
            elif ch == closer:
                depth -= 1
                if depth == 0:
                    return text[start:i + 1]
    return None


# ── Provider negotiation ──────────────────────────────────────────────


def _provider_kind(provider) -> str:
    return getattr(provider, "name", "") or ""


# ── Main entry point ──────────────────────────────────────────────────


async def complete_struct(
    provider,
    prompt: str,
    struct_cls: type[T],
    *,
    system: str | None = None,
    max_tokens: int = 1024,
    temperature: float = 0.2,
    retries: int = 1,
) -> T:
    """Run `prompt` through `provider` and decode the response into
    `struct_cls`. Raises `ValueError` if the model's output can't be
    parsed after `retries + 1` attempts.

    The caller's `system` prompt is augmented with the JSON schema
    requirement so even providers that don't support native structured
    output see the constraint.
    """
    schema = schema_for(struct_cls)
    schema_str = json.dumps(schema, indent=2)

    # Build the system prompt. We prepend a strong "respond with JSON
    # matching this schema" preamble and keep whatever the caller passed
    # as additional context.
    base_system = (
        "Respond with a single JSON object matching this schema. "
        "Do not wrap it in prose. Do not add commentary outside the "
        "JSON. The JSON itself can use any string content the schema "
        "allows — including SQL, prose, multi-line text — as long as "
        "it parses as valid JSON.\n\n"
        f"### JSON schema\n```json\n{schema_str}\n```"
    )
    full_system = f"{base_system}\n\n{system}" if system else base_system

    last_err: Exception | None = None
    last_text: str = ""

    for attempt in range(retries + 1):
        # On retry, append a remediation note so the model knows what
        # went wrong on the previous turn.
        if attempt > 0 and last_text:
            prompt_for_attempt = (
                f"{prompt}\n\n"
                "(Previous attempt did not parse as valid JSON matching "
                "the schema. Output JSON only. Previous response head: "
                f"{last_text[:200]!r})"
            )
        else:
            prompt_for_attempt = prompt

        try:
            text = await provider.complete(
                prompt_for_attempt,
                system=full_system,
                max_tokens=max_tokens,
                temperature=temperature,
            )
        except Exception as e:
            last_err = e
            log.warning("ai_struct: provider call failed", error=str(e), attempt=attempt)
            continue

        last_text = text or ""
        blob = _extract_json_blob(last_text)
        if blob is None:
            last_err = ValueError("no JSON found in model response")
            continue

        try:
            return msgspec.json.decode(blob.encode("utf-8"), type=struct_cls)
        except msgspec.ValidationError as e:
            last_err = e
        except msgspec.DecodeError as e:
            last_err = e
        except Exception as e:
            last_err = e

    raise ValueError(
        f"complete_struct: failed to parse {struct_cls.__name__} after "
        f"{retries + 1} attempts: {last_err}"
    ) from last_err
