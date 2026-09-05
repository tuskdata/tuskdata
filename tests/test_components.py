"""The component library (templates/components/*.html) renders and stays on tokens.

Every macro is rendered once with representative arguments so a template
syntax error or a renamed argument fails here, not on a plugin page at
runtime. A second test guards the design rule: components carry no
hard-coded colours.
"""

from __future__ import annotations

import re
from pathlib import Path

import minijinja
import pytest

COMPONENTS = Path(__file__).resolve().parent.parent / "src" / "tusk" / "studio" / "templates"


@pytest.fixture(scope="module")
def env() -> minijinja.Environment:
    return minijinja.Environment(loader=minijinja.load_from_path(str(COMPONENTS)))


def render(env: minijinja.Environment, source: str, **ctx) -> str:
    env.add_template("_probe.html", source)
    return env.render_template("_probe.html", **ctx)


def test_feedback_macros(env):
    out = render(env, """
        {% from "components/feedback.html" import badge, severity_badge, status_badge, alert, empty_state, modal %}
        {{ badge("12 rows", "success") }} {{ badge("x") }}
        {{ severity_badge("critical") }} {{ status_badge("running") }} {{ status_badge("pending") }} {{ status_badge("paused") }}
        {{ alert("Saved", "success") }} {{ alert("Careful", "warning", dismissible=true) }}
        {{ empty_state("Nothing here", icon="inbox", action_text="Add one", action_url="/new", title="Empty") }}
        {% call modal(id="m1", title="Edit", icon="pencil") %}<p>body</p>{% endcall %}
        {% call modal(id="m2", title="Delete", danger=true, close_event="done") %}<p>sure?</p>{% endcall %}
    """)
    assert "chip chip-green" in out and "chip chip-neutral" in out
    assert "chip chip-rose" in out and "chip chip-amber" in out
    assert "alert alert-success" in out and "alert alert-warning" in out
    assert 'class="empty"' in out and "empty-title" in out and "Add one</a>" in out
    assert "modal-mask" in out and "modal-body" in out and "<p>body</p>" in out
    assert 'x-on:open-m1.window="open = true"' in out
    assert "modal-head danger" in out and 'x-on:done.window="open = false"' in out


def test_forms_macros(env):
    out = render(env, """
        {% from "components/forms.html" import text_input, textarea, select_input, checkbox, toggle, button, icon_button, alpine_input, alpine_select, alpine_textarea, form_group %}
        {{ text_input("name", label="Name", value="tusk", help="Required") }}
        {{ textarea("notes", label="Notes", rows=3, mono=true) }}
        {{ select_input("kind", [{"value": "a", "label": "A"}, "b"], label="Kind", selected="b") }}
        {{ checkbox("ok", "Agree", checked=true) }}
        {{ toggle("dark", "Dark mode", checked=true) }}
        {{ button("Save", type="submit", variant="brand", icon="save") }} {{ button("Cancel", variant="ghost", size="sm") }}
        {{ icon_button("trash", title="Delete", variant="danger", size="sm") }}
        {{ alpine_input("form.host", label="Host", mono=true) }}
        {{ alpine_select("form.kind", ["x", "y"], label="Kind") }}
        {{ alpine_textarea("form.sql", rows=2) }}
        {% call form_group(label="Colour", error="Bad colour") %}<input>{% endcall %}
    """)
    assert out.count('class="field"') == 7  # checkbox and toggle are inline labels
    assert '<option value="b" selected>' in out
    assert 'class="switch"' in out and "checked" in out
    assert "btn btn-brand" in out and "btn-ghost" in out and "btn-sm" in out
    assert "icon-btn icon-btn-danger icon-btn-sm" in out and 'aria-label="Delete"' in out
    assert 'x-model="form.host"' in out and 'x-model="form.kind"' in out
    assert "Bad colour" in out


def test_layout_card_status_macros(env):
    out = render(env, """
        {% from "components/layout.html" import page_header, section_card, info_item, info_grid, collapsible %}
        {% from "components/card.html" import stat_card %}
        {% from "components/status.html" import status_dot, stage_dots, progress_stages, status_icon %}
        {% call page_header("Runs", back_url="/ci", description="Recent") %}<button class="btn">Go</button>{% endcall %}
        {% call section_card("History", subtitle="10", icon="clock") %}<table></table>{% endcall %}
        {% call info_grid(3) %}{{ info_item("Branch", "main", mono=true) }}{% endcall %}
        {% call collapsible("Log", expanded=false, id="log") %}<pre>x</pre>{% endcall %}
        {{ stat_card("Queries", 42, icon="zap", color="green", change="5%", change_type="up") }}
        {{ status_dot("running") }} {{ status_dot("failed", size="md", animate=false) }} {{ status_dot("queued", size="xs") }}
        {{ stage_dots(stages) }} {{ progress_stages(stages) }} {{ status_icon("success") }} {{ status_icon("running", size="md") }}
    """, stages=[{"stage_name": "build", "status": "success"}, {"stage_name": "test", "status": "running"}])
    assert 'aria-label="Back"' in out and "Recent" in out and "section-head" in out
    assert "grid-cols-3" in out and 'id="log"' in out and 'x-data="{ show: false }"' in out
    assert "stat-value" in out and "var(--green)" in out and "chip chip-green" in out
    assert "dot amber" in out and "pulse" in out and "dot red" in out and "dot-md" in out and "dot violet" in out
    assert "var(--accent-amber)" in out and "chip-rose" not in out.split("progress")[0]
    assert "loader-2" in out


def test_map_and_pipeline_macros(env):
    out = render(env, """
        {% from "components/map.html" import map_assets, map_container, map_dark_styles, carto_dark_style %}
        {% from "components/pipeline.html" import pipeline_assets, pipeline_canvas %}
        {{ map_assets() }} {{ map_container("map1", height="300px") }} {{ map_dark_styles() }}
        {{ pipeline_assets() }} {{ pipeline_canvas("dag", height="200px") }}
    """)
    assert 'id="map1"' in out and "maplibregl-popup-content" in out and "var(--surface)" in out
    assert 'id="dag"' in out


def test_components_carry_no_hardcoded_colours():
    """Components use tokens/classes only. The map basemap style JSON is data, not styling."""
    hex_re = re.compile(r"#[0-9a-fA-F]{6}\b")
    offenders = {}
    for f in sorted((COMPONENTS / "components").glob("*.html")):
        text = f.read_text()
        if f.name == "map.html":
            # carto_dark_style() is a MapLibre style document; strip it.
            text = text.split("{% macro carto_dark_style() %}")[0]
        hits = hex_re.findall(text)
        if hits:
            offenders[f.name] = hits
    assert not offenders, offenders
