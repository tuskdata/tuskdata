"""Tests for the Pipeline Canvas component (Phase 10).

Verifies:
- pipeline.js exists and has expected API functions
- pipeline.html template has expected macros
- Transform adapter functions are syntactically correct
- Data tab template properly integrates pipeline assets
"""

import os
from pathlib import Path

STATIC = Path(__file__).parent.parent / "src" / "tusk" / "studio" / "static"
TEMPLATES = Path(__file__).parent.parent / "src" / "tusk" / "studio" / "templates"


# ── File existence ──────────────────────────────────────────────

def test_pipeline_js_exists():
    assert (STATIC / "pipeline.js").is_file(), "pipeline.js not found"


def test_pipeline_html_exists():
    assert (TEMPLATES / "components" / "pipeline.html").is_file(), "pipeline.html not found"


# ── pipeline.js content ────────────────────────────────────────

class TestPipelineJS:
    """Verify pipeline.js has the expected API surface."""

    @classmethod
    def setup_class(cls):
        cls.content = (STATIC / "pipeline.js").read_text()

    def test_has_tusk_pipeline_function(self):
        assert "function tuskPipeline(" in self.content

    def test_has_alpine_component(self):
        assert "tuskPipelineCanvas" in self.content

    def test_has_transforms_to_pipeline(self):
        assert "function transformsToPipeline(" in self.content

    def test_has_pipeline_to_transforms(self):
        assert "function pipelineToTransforms(" in self.content

    def test_has_transform_label(self):
        assert "function _transformLabel(" in self.content

    def test_has_node_width_constant(self):
        assert "PIPELINE_NODE_WIDTH" in self.content

    def test_has_node_height_constant(self):
        assert "PIPELINE_NODE_HEIGHT" in self.content

    def test_has_zoom_constants(self):
        assert "PIPELINE_MIN_ZOOM" in self.content
        assert "PIPELINE_MAX_ZOOM" in self.content

    def test_has_color_palette(self):
        assert "PIPELINE_COLORS" in self.content
        for color in ["green", "blue", "purple", "orange", "red", "gray"]:
            # Keys can be bare identifiers (green:) or quoted ('green':)
            assert f"{color}:" in self.content or f"'{color}'" in self.content

    def test_api_methods(self):
        """Public API methods should exist."""
        methods = [
            "addNode(", "removeNode(", "addEdge(", "removeEdge(",
            "autoLayout()", "fitView()", "getState()", "setState(",
            "toTransforms()", "fromTransforms(",  "clear()",
        ]
        for method in methods:
            assert method in self.content, f"Missing API method: {method}"

    def test_canvas_interactions(self):
        """Canvas interaction handlers should exist."""
        handlers = [
            "onNodeMouseDown(", "onNodeDoubleClick(",
            "onPortMouseDown(", "onPortMouseUp(",
            "onCanvasMouseDown(", "onCanvasMouseMove(", "onCanvasMouseUp(",
            "onCanvasClick(", "onWheel(",
        ]
        for handler in handlers:
            assert handler in self.content, f"Missing handler: {handler}"

    def test_dagre_integration(self):
        """Should integrate with dagre.js for auto-layout."""
        assert "dagre.graphlib.Graph" in self.content
        assert "dagre.layout" in self.content
        assert "rankdir" in self.content

    def test_edge_path_computation(self):
        """Should compute bezier edge paths."""
        assert "_bezierPath(" in self.content
        assert "getEdgePath(" in self.content
        assert "getEdgeMidpoint(" in self.content

    def test_selection_support(self):
        """Should support node and edge selection."""
        assert "selectedNodes" in self.content
        assert "selectedEdges" in self.content
        assert "selectNode(" in self.content
        assert "selectEdge(" in self.content
        assert "clearSelection()" in self.content
        assert "deleteSelected()" in self.content

    def test_no_eval(self):
        """Must not use eval() — security rule."""
        # Check that eval is not used as a function call
        import re
        matches = re.findall(r'\beval\s*\(', self.content)
        assert len(matches) == 0, f"Found eval() usage: {matches}"

    def test_no_inline_styles(self):
        """Should not contain inline <style> blocks."""
        assert "<style>" not in self.content


# ── pipeline.html content ──────────────────────────────────────

class TestPipelineHTML:
    """Verify pipeline.html template has expected macros."""

    @classmethod
    def setup_class(cls):
        cls.content = (TEMPLATES / "components" / "pipeline.html").read_text()

    def test_has_pipeline_assets_macro(self):
        assert "macro pipeline_assets()" in self.content

    def test_has_pipeline_canvas_macro(self):
        assert "macro pipeline_canvas(" in self.content

    def test_has_pipeline_toolbar_macro(self):
        assert "macro pipeline_toolbar(" in self.content

    def test_has_pipeline_minimap_macro(self):
        assert "macro pipeline_minimap(" in self.content

    def test_loads_dagre_js(self):
        assert "dagre.min.js" in self.content

    def test_loads_pipeline_js(self):
        assert "pipeline.js" in self.content

    def test_has_svg_canvas(self):
        assert "<svg" in self.content

    def test_has_arrow_markers(self):
        assert "arrow" in self.content
        assert "<marker" in self.content

    def test_has_grid_pattern(self):
        assert "<pattern" in self.content
        assert "grid" in self.content

    def test_has_viewport_group(self):
        assert "viewport" in self.content

    def test_has_nodes_layer(self):
        assert "nodes-layer" in self.content

    def test_has_edges_layer(self):
        assert "edges-layer" in self.content

    def test_has_toolbar_buttons(self):
        for action in ["autoLayout()", "fitView()", "zoomIn()", "zoomOut()", "deleteSelected()"]:
            assert action in self.content, f"Missing toolbar action: {action}"

    def test_has_cdn_vendor_toggle(self):
        """Should support both CDN and vendor modes."""
        assert "use_cdn" in self.content
        assert "cdn.jsdelivr.net" in self.content
        assert "vendor/dagre.min.js" in self.content


# ── Data tab integration ───────────────────────────────────────

class TestDataTabIntegration:
    """Verify data.html properly integrates pipeline canvas."""

    @classmethod
    def setup_class(cls):
        cls.html = (TEMPLATES / "data.html").read_text()
        cls.js = (STATIC / "data.js").read_text()

    def test_imports_pipeline_macros(self):
        assert "pipeline_assets" in self.html
        assert "pipeline_canvas" in self.html

    def test_loads_pipeline_assets_in_head(self):
        assert "pipeline_assets()" in self.html

    def test_has_canvas_container(self):
        assert 'id="pipeline-canvas-container"' in self.html
        assert "data-pipeline" in self.html

    def test_has_toggle_button(self):
        assert "toggle-canvas-btn" in self.html
        assert "togglePipelineCanvas" in self.html

    def test_pipeline_js_loaded_before_data_js(self):
        """pipeline.js must be loaded before data.js (via pipeline_assets macro in head block)."""
        # pipeline.js is loaded via {{ pipeline_assets() }} macro in {% block head %}
        # data.js is loaded in {% block scripts %} — head always comes before scripts
        assets_idx = self.html.index("pipeline_assets()")
        data_idx = self.html.index("data.js")
        assert assets_idx < data_idx

    def test_data_js_has_node_types(self):
        assert "DATA_NODE_TYPES" in self.js

    def test_data_js_has_canvas_init(self):
        assert "initPipelineCanvas" in self.js

    def test_data_js_has_sync_function(self):
        assert "syncCanvasFromTransforms" in self.js

    def test_data_js_has_toggle_function(self):
        assert "togglePipelineCanvas" in self.js

    def test_data_js_defines_all_transform_nodes(self):
        """All transform types should have matching node type definitions."""
        for node_type in ["source", "filter", "select", "sort", "group_by",
                          "rename", "drop_nulls", "limit", "join"]:
            assert f"'{node_type}'" in self.js or f'"{node_type}"' in self.js or \
                   f"{node_type}:" in self.js, f"Missing node type: {node_type}"


# ── Vendor script ──────────────────────────────────────────────

def test_vendor_script_includes_dagre():
    vendor_sh = Path(__file__).parent.parent / "scripts" / "vendor.sh"
    content = vendor_sh.read_text()
    assert "dagre.min.js" in content
    assert "@dagrejs/dagre" in content
