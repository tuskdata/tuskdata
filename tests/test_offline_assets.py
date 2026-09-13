"""The UI must render without the internet.

Every browser asset (Tailwind, Alpine, HTMX, Lucide, MapLibre, proj4, Dagre,
Chart.js, gridstack, fonts) is vendored under ``static/vendor`` and
committed; templates reference those files. The only external URLs allowed
are inside a ``{% if use_cdn %}`` branch, which is opt-in with TUSK_CDN=1.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src" / "tusk"
STATIC = SRC / "studio" / "static"
VENDOR = STATIC / "vendor"
TEMPLATE_DIRS = [SRC / "studio" / "templates", SRC / "bi" / "templates"]

EXTERNAL = re.compile(r"https?://(cdn\.|unpkg\.com|cdnjs\.|fonts\.g)")
CDN_BRANCH = re.compile(r"{%-?\s*if\s+use_cdn[^%]*%}.*?{%-?\s*(else|endif)\s*-?%}", re.S)


def _templates():
    for d in TEMPLATE_DIRS:
        yield from sorted(d.rglob("*.html"))


def test_no_external_assets_outside_cdn_branch():
    offenders = []
    for path in _templates():
        text = CDN_BRANCH.sub("", path.read_text())
        for n, line in enumerate(text.splitlines(), 1):
            if EXTERNAL.search(line):
                offenders.append(f"{path.relative_to(SRC)}:{n}: {line.strip()[:90]}")
    assert not offenders, "external asset URLs outside {% if use_cdn %}:\n" + "\n".join(offenders)


def test_every_vendor_reference_exists():
    refs = set()
    for path in list(_templates()) + list(STATIC.glob("*.js")) + list(STATIC.glob("*.css")):
        refs.update(re.findall(r"/static/vendor/[\w./-]+", path.read_text()))
    refs.update(re.findall(r"/static/vendor/[\w./-]+", (VENDOR / "fonts.css").read_text()))
    assert refs, "no vendor references found at all"
    missing = sorted(r for r in refs if not (STATIC / r.removeprefix("/static/")).is_file())
    assert not missing, f"referenced but not vendored: {missing}"


def test_vendor_bundle_is_complete():
    expected = {
        "tailwind.min.css", "alpine.min.js", "htmx.min.js", "lucide.min.js",
        "maplibre-gl.js", "maplibre-gl.css", "proj4.min.js", "dagre.min.js",
        "chart.umd.min.js", "gridstack-all.js", "gridstack.min.css", "fonts.css",
    }
    present = {p.name for p in VENDOR.iterdir()}
    assert expected <= present, f"missing from static/vendor: {sorted(expected - present)}"
    fonts = (VENDOR / "fonts.css").read_text()
    assert "gstatic.com" not in fonts, "fonts.css still points at Google"
    for family in ("Geist", "Geist Mono", "Instrument Serif"):
        assert f"font-family: '{family}'" in fonts, f"{family} not in fonts.css"


_CLASS_ATTR = re.compile(r'\bclass="([^"]*)"')
# Tokens that are plainly Tailwind utilities. Everything else (tusk-app.css
# classes, Alpine bindings, Jinja) is ignored: we only want to catch a stale
# compiled file, not to police naming.
_TW = re.compile(
    r"^(?:(?:sm|md|lg|xl|2xl|hover|focus|active|disabled|group-hover|peer-checked|first|last|odd|even):)*"
    r"(?:-?(?:m|p|mx|my|mt|mb|ml|mr|px|py|pt|pb|pl|pr|w|h|min-w|min-h|max-w|max-h|gap|gap-x|gap-y|space-x|space-y|"
    r"text|font|leading|tracking|bg|border|rounded|shadow|ring|opacity|flex|grid|col|row|items|justify|self|"
    r"overflow|truncate|whitespace|inline|block|hidden|absolute|relative|fixed|sticky|inset|top|bottom|left|right|"
    r"z|cursor|select|transition|duration|ease|transform|scale|rotate|translate|uppercase|lowercase|capitalize|"
    r"italic|underline|line-through|list|divide|order|shrink|grow|basis|aspect|object|resize|pointer-events|"
    r"outline|animate|backdrop|filter|blur|sr-only|not-sr-only|table|align|break|content|place|columns|float|"
    r"clear|isolate|visible|invisible|collapse|appearance|accent|caret|decoration|indent|tabular-nums|antialiased))"
    r"(?:-[\w./%\[\]#,()]+)?$"
)


def _template_classes():
    tokens = set()
    for path in _templates():
        text = path.read_text()
        for attr in _CLASS_ATTR.findall(text):
            if "{" in attr or "}" in attr:
                continue  # Jinja inside the attribute: not a static token list
            tokens.update(t for t in attr.split() if _TW.match(t))
    return tokens


def _css_escape(token: str) -> str:
    return re.sub(r"([:/.\[\]#%,()])", r"\\\1", token)


def test_compiled_tailwind_is_not_stale():
    css = (VENDOR / "tailwind.min.css").read_text()
    tokens = _template_classes()
    assert len(tokens) > 100, "class scan found suspiciously few utilities"
    missing = sorted(t for t in tokens if "." + _css_escape(t) not in css)
    # A handful of tokens are legitimately absent (typos in old templates,
    # classes only Tailwind 3 knew). Keep the list short and explicit.
    allowed = set(os.environ.get("TUSK_TEST_TW_ALLOW", "").split())
    missing = [t for t in missing if t not in allowed]
    assert len(missing) <= 25, (
        "classes used in templates but absent from static/vendor/tailwind.min.css "
        f"— run `make css`: {missing[:60]}"
    )


def test_cdn_mode_is_opt_in(monkeypatch):
    from tusk.studio.routes.base import _use_cdn

    monkeypatch.delenv("TUSK_CDN", raising=False)
    assert _use_cdn() is False
    monkeypatch.setenv("TUSK_CDN", "1")
    assert _use_cdn() is True
    monkeypatch.setenv("TUSK_CDN", "0")
    assert _use_cdn() is False


@pytest.mark.skipif(not (SRC.parent.parent / "pyproject.toml").exists(), reason="source checkout only")
def test_vendor_is_not_gitignored():
    gi = (SRC.parent.parent / ".gitignore").read_text()
    assert "static/vendor" not in gi, "static/vendor must be committed"
