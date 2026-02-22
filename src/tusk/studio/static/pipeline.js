/**
 * tuskPipeline — Reusable visual DAG pipeline canvas
 *
 * Alpine.js component for rendering directed acyclic graphs (pipelines)
 * with SVG nodes, edges, and interactive editing.
 *
 * Nodes and edges are rendered IMPERATIVELY via innerHTML on <g> containers
 * because <template x-for> does not work inside SVG (browsers parse <template>
 * as an SVGElement, not HTMLTemplateElement, breaking Alpine's loop scoping).
 *
 * Consumers: Data tab, tusk-ci, tusk-bi
 *
 * Usage:
 *   const canvas = tuskPipeline('my-canvas', {
 *       nodeTypes: { source: { icon: 'database', color: 'green', ports: { in: [], out: ['output'] } } },
 *       onNodeClick: (node) => { ... },
 *       onNodeDoubleClick: (node) => { ... },
 *       onPipelineChange: (state) => { ... },
 *   });
 */

// ============================================================================
// Configuration & Constants
// ============================================================================

const PIPELINE_NODE_WIDTH = 180;
const PIPELINE_NODE_HEIGHT = 72;
const PIPELINE_MIN_ZOOM = 0.2;
const PIPELINE_MAX_ZOOM = 3.0;
const PIPELINE_ZOOM_STEP = 0.1;

// Color palette for node types
const PIPELINE_COLORS = {
    green:  '#2ea043',
    blue:   '#58a6ff',
    purple: '#bc8cff',
    orange: '#d29922',
    red:    '#f85149',
    cyan:   '#39d4e8',
    yellow: '#e3b341',
    gray:   '#8b949e',
    indigo: '#818cf8',
    pink:   '#f778ba',
};

// ============================================================================
// Pipeline Instance Registry
// ============================================================================

const _pipelineInstances = {};

/**
 * Create or get a pipeline canvas instance.
 *
 * @param {string} canvasId - The SVG element ID
 * @param {object} config - Configuration with nodeTypes, callbacks
 * @returns {object} Pipeline API object
 */
function tuskPipeline(canvasId, config = {}) {
    if (_pipelineInstances[canvasId]) {
        // Update config on existing instance
        Object.assign(_pipelineInstances[canvasId]._config, config);
        return _pipelineInstances[canvasId];
    }

    const api = {
        _config: {
            nodeTypes: {},
            direction: 'LR',
            onNodeClick: null,
            onNodeDoubleClick: null,
            onNodeDelete: null,
            onEdgeCreate: null,
            onPipelineChange: null,
            ...config,
        },
        _canvasId: canvasId,
        _alpine: null, // Set when Alpine component initializes

        // ── Public API ──────────────────────────────────────

        addNode(opts) {
            if (!this._alpine) return null;
            return this._alpine.addNode(opts);
        },

        removeNode(nodeId) {
            if (!this._alpine) return;
            this._alpine.removeNodeById(nodeId);
        },

        addEdge(sourceId, targetId, sourcePort = 'output', targetPort = 'input') {
            if (!this._alpine) return null;
            return this._alpine.addEdge(sourceId, targetId, sourcePort, targetPort);
        },

        removeEdge(edgeId) {
            if (!this._alpine) return;
            this._alpine.removeEdgeById(edgeId);
        },

        autoLayout() {
            if (!this._alpine) return;
            this._alpine.autoLayout();
        },

        fitView() {
            if (!this._alpine) return;
            this._alpine.fitView();
        },

        getState() {
            if (!this._alpine) return { nodes: [], edges: [], viewport: { x: 0, y: 0, zoom: 1 } };
            return {
                nodes: JSON.parse(JSON.stringify(this._alpine.nodes)),
                edges: JSON.parse(JSON.stringify(this._alpine.edges)),
                viewport: { ...this._alpine.viewport },
            };
        },

        setState(state) {
            if (!this._alpine) return;
            this._alpine.nodes = state.nodes || [];
            this._alpine.edges = state.edges || [];
            if (state.viewport) this._alpine.viewport = state.viewport;
            this._alpine._render();
        },

        getNodes() {
            return this._alpine ? [...this._alpine.nodes] : [];
        },

        getEdges() {
            return this._alpine ? [...this._alpine.edges] : [];
        },

        /**
         * Convert canvas nodes/edges → flat transforms array
         * (for backward compat with Data tab pipeline format)
         */
        toTransforms() {
            if (!this._alpine) return [];
            return pipelineToTransforms(this._alpine.nodes, this._alpine.edges);
        },

        /**
         * Convert flat transforms array → canvas nodes/edges
         * @param {object} source - The data source object
         * @param {Array} transforms - Flat transforms array
         */
        fromTransforms(source, transforms) {
            if (!this._alpine) return;
            const state = transformsToPipeline(source, transforms, this._config.nodeTypes);
            this._alpine.nodes = state.nodes;
            this._alpine.edges = state.edges;
            this._alpine.autoLayout();
            this._alpine._render();
        },

        clear() {
            if (!this._alpine) return;
            this._alpine.nodes = [];
            this._alpine.edges = [];
            this._alpine.selectedNodes = [];
            this._alpine.selectedEdges = [];
            this._alpine._render();
        },
    };

    _pipelineInstances[canvasId] = api;
    return api;
}

// ============================================================================
// Alpine.js Component
// ============================================================================

document.addEventListener('alpine:init', () => {
    Alpine.data('tuskPipelineCanvas', (canvasId, direction) => ({
        // State
        nodes: [],
        edges: [],
        viewport: { x: 40, y: 40, zoom: 1 },
        selectedNodes: [],
        selectedEdges: [],
        focused: false,

        // Drag state
        dragging: null,       // { nodeId, offsetX, offsetY }
        panning: false,
        panStart: null,
        draggingEdge: null,   // { sourceId, sourcePort, x, y }
        tempEdgePath: '',

        // Render throttle
        _renderScheduled: false,

        // Computed
        get minimapTransform() {
            if (this.nodes.length === 0) return '';
            const bounds = this._getBounds();
            const scale = Math.min(140 / (bounds.width + 40), 90 / (bounds.height + 40));
            return `translate(${-bounds.minX * scale + 5} ${-bounds.minY * scale + 5}) scale(${scale})`;
        },

        get minimapViewport() {
            if (this.nodes.length === 0) return { x: 0, y: 0, w: 140, h: 90 };
            const bounds = this._getBounds();
            const scale = Math.min(140 / (bounds.width + 40), 90 / (bounds.height + 40));
            const svg = document.getElementById(canvasId);
            if (!svg) return { x: 0, y: 0, w: 140, h: 90 };
            const rect = svg.getBoundingClientRect();
            return {
                x: (-this.viewport.x / this.viewport.zoom - bounds.minX) * scale + 5,
                y: (-this.viewport.y / this.viewport.zoom - bounds.minY) * scale + 5,
                w: (rect.width / this.viewport.zoom) * scale,
                h: (rect.height / this.viewport.zoom) * scale,
            };
        },

        // ── Init ─────────────────────────────────────────

        init() {
            const api = _pipelineInstances[canvasId];
            if (api) {
                api._alpine = this;
                if (api._config.direction) direction = api._config.direction;
            }
            // Initial render
            this.$nextTick(() => this._render());
        },

        // ── Imperative SVG Rendering ─────────────────────
        // <template x-for> does NOT work inside SVG.
        // We render nodes/edges as HTML strings and set innerHTML.

        _render() {
            const svg = document.getElementById(canvasId);
            if (!svg) return;

            const nodesLayer = svg.querySelector('.nodes-layer');
            const edgesLayer = svg.querySelector('.edges-layer');
            if (!nodesLayer || !edgesLayer) return;

            edgesLayer.innerHTML = this._edgesHTML();
            nodesLayer.innerHTML = this._nodesHTML();

            // Also render minimap if present
            this._renderMinimap(svg);
        },

        _scheduleRender() {
            if (this._renderScheduled) return;
            this._renderScheduled = true;
            requestAnimationFrame(() => {
                this._renderScheduled = false;
                this._render();
            });
        },

        _escSVG(str) {
            if (!str) return '';
            return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
        },

        _nodesHTML() {
            return this.nodes.map(node => {
                const color = this.getNodeColor(node);
                const icon = this.getNodeIcon(node);
                const subtitle = this.getNodeSubtitle(node);
                const ports = this.getNodePorts(node);
                const selected = this.selectedNodes.includes(node.id);

                const bgFill = selected ? '#1c2333' : '#161b22';
                const borderStroke = selected ? '#6366f1' : '#30363d';
                const borderWidth = selected ? 2 : 1;

                // Input ports
                const inPorts = (ports.in || []).map((p, i) => {
                    const cy = 24 + i * 20;
                    const highlight = this.draggingEdge ? '#6366f1' : '#484f58';
                    return `<circle data-port-node="${node.id}" data-port-name="${this._escSVG(p)}" data-port-side="in"
                                cx="0" cy="${cy}" r="5"
                                fill="#0d1117" stroke="${highlight}" stroke-width="1.5"
                                class="cursor-crosshair" />`
                        + (ports.in.length > 1 ? `<text x="-12" y="${cy + 4}" text-anchor="end" fill="#484f58" font-size="8" class="select-none pointer-events-none">${this._escSVG(p)}</text>` : '');
                }).join('');

                // Output ports
                const outPorts = (ports.out || []).map((p, i) => {
                    const cy = 24 + i * 20;
                    return `<circle data-port-node="${node.id}" data-port-name="${this._escSVG(p)}" data-port-side="out"
                                cx="${PIPELINE_NODE_WIDTH}" cy="${cy}" r="5"
                                fill="#0d1117" stroke="#484f58" stroke-width="1.5"
                                class="cursor-crosshair" />`
                        + (ports.out.length > 1 ? `<text x="${PIPELINE_NODE_WIDTH + 12}" y="${cy + 4}" fill="#484f58" font-size="8" class="select-none pointer-events-none">${this._escSVG(p)}</text>` : '');
                }).join('');

                // Badge
                const badge = node.meta?.badge
                    ? `<text x="48" y="62" fill="#58a6ff" font-size="9" class="select-none">${this._escSVG(node.meta.badge)}</text>`
                    : '';

                return `<g data-node-id="${node.id}" transform="translate(${node.x} ${node.y})" class="cursor-move">
                    <rect width="${PIPELINE_NODE_WIDTH}" height="${PIPELINE_NODE_HEIGHT}"
                          rx="8" ry="8" fill="${bgFill}"
                          stroke="${borderStroke}" stroke-width="${borderWidth}" />
                    <rect width="${PIPELINE_NODE_WIDTH}" height="3" rx="1.5" ry="1.5" fill="${color}" />
                    <rect y="2" width="${PIPELINE_NODE_WIDTH}" height="1" fill="${color}" opacity="0.3" />
                    <circle cx="26" cy="36" r="14" fill="${color}" fill-opacity="0.15" />
                    <text x="26" y="40" text-anchor="middle" fill="${color}" font-size="14">${icon}</text>
                    <text x="48" y="32" fill="#e6edf3" font-size="12" font-weight="500" class="select-none">${this._escSVG(node.label || node.type)}</text>
                    <text x="48" y="48" fill="#8b949e" font-size="10" class="select-none">${this._escSVG(subtitle)}</text>
                    ${badge}
                    ${inPorts}
                    ${outPorts}
                </g>`;
            }).join('');
        },

        _edgesHTML() {
            return this.edges.map(edge => {
                const path = this.getEdgePath(edge);
                const mid = this.getEdgeMidpoint(edge);
                const selected = this.selectedEdges.includes(edge.id);
                const arrowId = selected ? `${canvasId}-arrow-selected` : `${canvasId}-arrow`;
                const stroke = selected ? '#818cf8' : '#484f58';
                const sw = selected ? 2.5 : 1.5;

                const labelSVG = edge.label
                    ? `<text x="${mid.x}" y="${mid.y - 6}" text-anchor="middle" fill="#8b949e" font-size="10">${this._escSVG(edge.label)}</text>`
                    : '';

                return `<g data-edge-id="${edge.id}" class="cursor-pointer">
                    <path d="${path}" fill="none" stroke="transparent" stroke-width="12" />
                    <path d="${path}" fill="none" stroke="${stroke}" stroke-width="${sw}" marker-end="url(#${arrowId})" />
                    ${labelSVG}
                </g>`;
            }).join('');
        },

        _renderMinimap(svg) {
            const wrapper = svg.closest('.pipeline-canvas-wrapper');
            if (!wrapper) return;
            const mmNodes = wrapper.querySelector('.minimap-nodes');
            if (!mmNodes) return;

            mmNodes.innerHTML = this.nodes.map(node => {
                const color = this.getNodeColor(node);
                return `<rect x="${node.x}" y="${node.y}" width="${PIPELINE_NODE_WIDTH}" height="${PIPELINE_NODE_HEIGHT}"
                              rx="4" ry="4" fill="${color}" fill-opacity="0.3" stroke="none" />`;
            }).join('');
        },

        // ── Node helpers ─────────────────────────────────

        getNodeColor(node) {
            const api = _pipelineInstances[canvasId];
            const nt = api?._config?.nodeTypes?.[node.type];
            const colorName = nt?.color || node.meta?.color || 'blue';
            return PIPELINE_COLORS[colorName] || colorName;
        },

        getNodeIcon(node) {
            const api = _pipelineInstances[canvasId];
            const nt = api?._config?.nodeTypes?.[node.type];
            // Return a unicode symbol for SVG text (Lucide icons can't render in <text>)
            const iconMap = {
                'database': '\u25C6', 'filter': '\u25BC', 'git-merge': '\u2443', 'group': '\u25EB',
                'download': '\u2193', 'columns': '\u25A5', 'arrow-up-down': '\u2195', 'shuffle': '\u21CC',
                'terminal': '\u2318', 'git-branch': '\u2442', 'check-circle': '\u2713', 'rocket': '\u25B2',
                'bell': '\u25C9', 'shield': '\u25C8', 'bar-chart': '\u258A', 'table': '\u25A6',
                'trending-up': '\u2197', 'play': '\u25B8', 'save': '\u25AA', 'upload': '\u2191',
                'file': '\u25AB', 'zap': '\u26A1', 'code': '<>', 'settings': '\u2699',
                'pencil': '\u270E', 'trash-2': '\u2717', 'hash': '#',
            };
            const iconName = nt?.icon || node.meta?.icon || 'file';
            return iconMap[iconName] || '\u25CF';
        },

        getNodeSubtitle(node) {
            if (node.config) {
                const c = node.config;
                if (node.type === 'filter' && c.column) return `${c.column} ${c.operator || '='} ${c.value ?? ''}`;
                if (node.type === 'select' && c.columns) return `${c.columns.length} columns`;
                if (node.type === 'sort' && c.columns) return c.columns.join(', ');
                if (node.type === 'group_by' && c.by) return `by: ${c.by.join(', ')}`;
                if (node.type === 'limit' && c.n) return `${c.n} rows`;
                if (node.type === 'join' && c.how) return `${c.how} join`;
                if (node.type === 'rename' && c.mapping) return Object.entries(c.mapping).map(([k, v]) => `${k}\u2192${v}`).join(', ');
                if (node.type === 'source' && c.path) return c.path.split('/').pop();
                if (node.type === 'run' && c.command) return c.command.substring(0, 30);
                if (node.type === 'deploy' && c.target) return c.target;
            }
            return '';
        },

        getNodePorts(node) {
            const api = _pipelineInstances[canvasId];
            const nt = api?._config?.nodeTypes?.[node.type];
            if (nt?.ports) return nt.ports;
            return node.ports || { in: ['input'], out: ['output'] };
        },

        // ── Edge path computation ────────────────────────

        _getPortPos(nodeId, portName, side) {
            const node = this.nodes.find(n => n.id === nodeId);
            if (!node) return { x: 0, y: 0 };
            const ports = this.getNodePorts(node);
            const portList = side === 'out' ? (ports.out || []) : (ports.in || []);
            const idx = portList.indexOf(portName);
            const portIdx = idx >= 0 ? idx : 0;

            if (side === 'out') {
                return { x: node.x + PIPELINE_NODE_WIDTH, y: node.y + 24 + portIdx * 20 };
            } else {
                return { x: node.x, y: node.y + 24 + portIdx * 20 };
            }
        },

        getEdgePath(edge) {
            const src = this._getPortPos(edge.source, edge.sourcePort, 'out');
            const tgt = this._getPortPos(edge.target, edge.targetPort, 'in');
            return this._bezierPath(src.x, src.y, tgt.x, tgt.y);
        },

        getEdgeMidpoint(edge) {
            const src = this._getPortPos(edge.source, edge.sourcePort, 'out');
            const tgt = this._getPortPos(edge.target, edge.targetPort, 'in');
            return { x: (src.x + tgt.x) / 2, y: (src.y + tgt.y) / 2 };
        },

        _bezierPath(x1, y1, x2, y2) {
            const dx = Math.abs(x2 - x1) * 0.5;
            return `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;
        },

        // ── Node CRUD ────────────────────────────────────

        addNode(opts) {
            const id = opts.id || ('node_' + Math.random().toString(36).substring(2, 10));
            const node = {
                id,
                type: opts.type || 'unknown',
                label: opts.label || opts.type || 'Node',
                config: opts.config || {},
                x: opts.x ?? (this.nodes.length * 220 + 40),
                y: opts.y ?? 40,
                ports: opts.ports || undefined,
                meta: opts.meta || {},
            };
            this.nodes.push(node);
            this._render();
            this._emitChange();
            return node;
        },

        removeNodeById(nodeId) {
            this.edges = this.edges.filter(e => e.source !== nodeId && e.target !== nodeId);
            this.nodes = this.nodes.filter(n => n.id !== nodeId);
            this.selectedNodes = this.selectedNodes.filter(id => id !== nodeId);
            this._render();
            this._emitChange();
        },

        addEdge(sourceId, targetId, sourcePort = 'output', targetPort = 'input', label = '') {
            // Prevent duplicate edges
            const exists = this.edges.some(e =>
                e.source === sourceId && e.target === targetId &&
                e.sourcePort === sourcePort && e.targetPort === targetPort
            );
            if (exists) return null;

            // Prevent self-loops
            if (sourceId === targetId) return null;

            const edge = {
                id: 'edge_' + Math.random().toString(36).substring(2, 10),
                source: sourceId,
                sourcePort,
                target: targetId,
                targetPort,
                label,
            };

            const api = _pipelineInstances[canvasId];
            if (api?._config?.onEdgeCreate) {
                const allowed = api._config.onEdgeCreate(edge);
                if (allowed === false) return null;
            }

            this.edges.push(edge);
            this._render();
            this._emitChange();
            return edge;
        },

        removeEdgeById(edgeId) {
            this.edges = this.edges.filter(e => e.id !== edgeId);
            this.selectedEdges = this.selectedEdges.filter(id => id !== edgeId);
            this._render();
            this._emitChange();
        },

        // ── Selection ────────────────────────────────────

        selectNode(nodeId, event) {
            if (event?.shiftKey) {
                const idx = this.selectedNodes.indexOf(nodeId);
                if (idx >= 0) this.selectedNodes.splice(idx, 1);
                else this.selectedNodes.push(nodeId);
            } else {
                this.selectedNodes = [nodeId];
                this.selectedEdges = [];
            }
            this._render();
            const api = _pipelineInstances[canvasId];
            const node = this.nodes.find(n => n.id === nodeId);
            if (api?._config?.onNodeClick && node) api._config.onNodeClick(node);
        },

        selectEdge(edgeId, event) {
            if (event?.shiftKey) {
                const idx = this.selectedEdges.indexOf(edgeId);
                if (idx >= 0) this.selectedEdges.splice(idx, 1);
                else this.selectedEdges.push(edgeId);
            } else {
                this.selectedEdges = [edgeId];
                this.selectedNodes = [];
            }
            this._render();
        },

        clearSelection() {
            if (this.selectedNodes.length === 0 && this.selectedEdges.length === 0) return;
            this.selectedNodes = [];
            this.selectedEdges = [];
            this._render();
        },

        deleteSelected() {
            if (this.selectedNodes.length > 0) {
                const api = _pipelineInstances[canvasId];
                for (const nodeId of [...this.selectedNodes]) {
                    const node = this.nodes.find(n => n.id === nodeId);
                    if (api?._config?.onNodeDelete && node) {
                        const allowed = api._config.onNodeDelete(node);
                        if (allowed === false) continue;
                    }
                    this.edges = this.edges.filter(e => e.source !== nodeId && e.target !== nodeId);
                    this.nodes = this.nodes.filter(n => n.id !== nodeId);
                }
                this.selectedNodes = [];
            }
            if (this.selectedEdges.length > 0) {
                for (const edgeId of [...this.selectedEdges]) {
                    this.edges = this.edges.filter(e => e.id !== edgeId);
                }
                this.selectedEdges = [];
            }
            this._render();
            this._emitChange();
        },

        // ── Mouse interactions (event delegation) ────────

        onCanvasMouseDown(event) {
            if (event.button !== 0) return;

            // Check port click (output → start edge drag)
            const portEl = event.target.closest('[data-port-node]');
            if (portEl) {
                const nodeId = portEl.getAttribute('data-port-node');
                const portName = portEl.getAttribute('data-port-name');
                const side = portEl.getAttribute('data-port-side');
                if (side === 'out') {
                    event.preventDefault();
                    event.stopPropagation();
                    const pos = this._getPortPos(nodeId, portName, 'out');
                    this.draggingEdge = { sourceId: nodeId, sourcePort: portName, x: pos.x, y: pos.y };
                }
                return;
            }

            // Check node click
            const nodeEl = event.target.closest('[data-node-id]');
            if (nodeEl) {
                const nodeId = nodeEl.getAttribute('data-node-id');
                this.selectNode(nodeId, event);
                const node = this.nodes.find(n => n.id === nodeId);
                if (!node) return;
                const svgPt = this._svgPoint(event);
                this.dragging = {
                    nodeId,
                    offsetX: svgPt.x - node.x,
                    offsetY: svgPt.y - node.y,
                };
                return;
            }

            // Check edge click
            const edgeEl = event.target.closest('[data-edge-id]');
            if (edgeEl) {
                const edgeId = edgeEl.getAttribute('data-edge-id');
                this.selectEdge(edgeId, event);
                return;
            }

            // Empty canvas → start panning
            this.clearSelection();
            this.panning = true;
            this.panStart = { x: event.clientX - this.viewport.x, y: event.clientY - this.viewport.y };
            const svg = document.getElementById(canvasId);
            if (svg) svg.style.cursor = 'grabbing';
        },

        onCanvasMouseMove(event) {
            // Dragging a node
            if (this.dragging) {
                const svgPt = this._svgPoint(event);
                const node = this.nodes.find(n => n.id === this.dragging.nodeId);
                if (node) {
                    node.x = svgPt.x - this.dragging.offsetX;
                    node.y = svgPt.y - this.dragging.offsetY;
                    // Update node position directly for performance (no full re-render)
                    const svg = document.getElementById(canvasId);
                    if (svg) {
                        const nodeEl = svg.querySelector(`[data-node-id="${this.dragging.nodeId}"]`);
                        if (nodeEl) nodeEl.setAttribute('transform', `translate(${node.x} ${node.y})`);
                        // Update connected edges
                        for (const edge of this.edges) {
                            if (edge.source === node.id || edge.target === node.id) {
                                const edgeEl = svg.querySelector(`[data-edge-id="${edge.id}"]`);
                                if (edgeEl) {
                                    const paths = edgeEl.querySelectorAll('path');
                                    const newPath = this.getEdgePath(edge);
                                    paths.forEach(p => p.setAttribute('d', newPath));
                                    // Update label position
                                    const label = edgeEl.querySelector('text');
                                    if (label) {
                                        const mid = this.getEdgeMidpoint(edge);
                                        label.setAttribute('x', mid.x);
                                        label.setAttribute('y', mid.y - 6);
                                    }
                                }
                            }
                        }
                    }
                }
                return;
            }

            // Panning
            if (this.panning && this.panStart) {
                this.viewport.x = event.clientX - this.panStart.x;
                this.viewport.y = event.clientY - this.panStart.y;
                return;
            }

            // Dragging edge from port
            if (this.draggingEdge) {
                const svgPt = this._svgPoint(event);
                const src = this._getPortPos(this.draggingEdge.sourceId, this.draggingEdge.sourcePort, 'out');
                this.tempEdgePath = this._bezierPath(src.x, src.y, svgPt.x, svgPt.y);
            }
        },

        onCanvasMouseUp(event) {
            if (this.dragging) {
                this.dragging = null;
                this._render(); // Full re-render to sync everything
                this._emitChange();
            }
            if (this.panning) {
                this.panning = false;
                this.panStart = null;
                const svg = document.getElementById(canvasId);
                if (svg) svg.style.cursor = 'grab';
            }
            if (this.draggingEdge) {
                // Check if dropped on an input port
                const portEl = event.target.closest('[data-port-node]');
                if (portEl && portEl.getAttribute('data-port-side') === 'in') {
                    const targetId = portEl.getAttribute('data-port-node');
                    const targetPort = portEl.getAttribute('data-port-name');
                    this.addEdge(
                        this.draggingEdge.sourceId,
                        targetId,
                        this.draggingEdge.sourcePort,
                        targetPort
                    );
                }
                this.draggingEdge = null;
                this.tempEdgePath = '';
            }
        },

        onCanvasDblClick(event) {
            const nodeEl = event.target.closest('[data-node-id]');
            if (nodeEl) {
                const nodeId = nodeEl.getAttribute('data-node-id');
                const api = _pipelineInstances[canvasId];
                const node = this.nodes.find(n => n.id === nodeId);
                if (api?._config?.onNodeDoubleClick && node) {
                    api._config.onNodeDoubleClick(node);
                }
            }
        },

        onCanvasClick(event) {
            // Not needed — selection handled in onCanvasMouseDown
        },

        onWheel(event) {
            const delta = event.deltaY > 0 ? -PIPELINE_ZOOM_STEP : PIPELINE_ZOOM_STEP;
            const newZoom = Math.max(PIPELINE_MIN_ZOOM, Math.min(PIPELINE_MAX_ZOOM, this.viewport.zoom + delta));

            const svg = document.getElementById(canvasId);
            if (!svg) return;
            const rect = svg.getBoundingClientRect();
            const mx = event.clientX - rect.left;
            const my = event.clientY - rect.top;

            const zoomRatio = newZoom / this.viewport.zoom;
            this.viewport.x = mx - (mx - this.viewport.x) * zoomRatio;
            this.viewport.y = my - (my - this.viewport.y) * zoomRatio;
            this.viewport.zoom = newZoom;
        },

        // ── Legacy handlers (kept for API compat) ────────

        onNodeMouseDown(nodeId, event) {
            this.selectNode(nodeId, event);
        },

        onNodeDoubleClick(nodeId) {
            const api = _pipelineInstances[canvasId];
            const node = this.nodes.find(n => n.id === nodeId);
            if (api?._config?.onNodeDoubleClick && node) {
                api._config.onNodeDoubleClick(node);
            }
        },

        onPortMouseDown(nodeId, portName, event) {
            event.preventDefault();
            const pos = this._getPortPos(nodeId, portName, 'out');
            this.draggingEdge = { sourceId: nodeId, sourcePort: portName, x: pos.x, y: pos.y };
        },

        onPortMouseUp(nodeId, portName, side) {
            if (!this.draggingEdge) return;
            if (side === 'in') {
                this.addEdge(this.draggingEdge.sourceId, nodeId, this.draggingEdge.sourcePort, portName);
            }
            this.draggingEdge = null;
            this.tempEdgePath = '';
        },

        // ── Layout ───────────────────────────────────────

        autoLayout() {
            if (typeof dagre === 'undefined') {
                console.warn('dagre.js not loaded — auto-layout unavailable');
                return;
            }
            if (this.nodes.length === 0) return;

            const g = new dagre.graphlib.Graph();
            g.setGraph({
                rankdir: direction || 'LR',
                nodesep: 40,
                ranksep: 80,
                marginx: 40,
                marginy: 40,
            });
            g.setDefaultEdgeLabel(() => ({}));

            for (const node of this.nodes) {
                g.setNode(node.id, { width: PIPELINE_NODE_WIDTH, height: PIPELINE_NODE_HEIGHT });
            }
            for (const edge of this.edges) {
                g.setEdge(edge.source, edge.target);
            }

            dagre.layout(g);

            for (const node of this.nodes) {
                const pos = g.node(node.id);
                if (pos) {
                    node.x = pos.x - PIPELINE_NODE_WIDTH / 2;
                    node.y = pos.y - PIPELINE_NODE_HEIGHT / 2;
                }
            }

            this._render();
            this._emitChange();
        },

        fitView() {
            if (this.nodes.length === 0) return;
            const svg = document.getElementById(canvasId);
            if (!svg) return;
            const rect = svg.getBoundingClientRect();
            const bounds = this._getBounds();
            const padding = 60;

            const scaleX = rect.width / (bounds.width + padding * 2);
            const scaleY = rect.height / (bounds.height + padding * 2);
            const zoom = Math.min(scaleX, scaleY, 1.5);

            this.viewport.zoom = zoom;
            this.viewport.x = -bounds.minX * zoom + (rect.width - bounds.width * zoom) / 2;
            this.viewport.y = -bounds.minY * zoom + (rect.height - bounds.height * zoom) / 2;
        },

        zoomIn() {
            this.viewport.zoom = Math.min(PIPELINE_MAX_ZOOM, this.viewport.zoom + PIPELINE_ZOOM_STEP * 2);
        },

        zoomOut() {
            this.viewport.zoom = Math.max(PIPELINE_MIN_ZOOM, this.viewport.zoom - PIPELINE_ZOOM_STEP * 2);
        },

        // ── Utilities ────────────────────────────────────

        _svgPoint(event) {
            const svg = document.getElementById(canvasId);
            if (!svg) return { x: 0, y: 0 };
            const rect = svg.getBoundingClientRect();
            return {
                x: (event.clientX - rect.left - this.viewport.x) / this.viewport.zoom,
                y: (event.clientY - rect.top - this.viewport.y) / this.viewport.zoom,
            };
        },

        _getBounds() {
            if (this.nodes.length === 0) return { minX: 0, minY: 0, maxX: 0, maxY: 0, width: 0, height: 0 };
            let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
            for (const n of this.nodes) {
                if (n.x < minX) minX = n.x;
                if (n.y < minY) minY = n.y;
                if (n.x + PIPELINE_NODE_WIDTH > maxX) maxX = n.x + PIPELINE_NODE_WIDTH;
                if (n.y + PIPELINE_NODE_HEIGHT > maxY) maxY = n.y + PIPELINE_NODE_HEIGHT;
            }
            return { minX, minY, maxX, maxY, width: maxX - minX, height: maxY - minY };
        },

        _emitChange() {
            const api = _pipelineInstances[canvasId];
            if (api?._config?.onPipelineChange) {
                api._config.onPipelineChange({
                    nodes: this.nodes,
                    edges: this.edges,
                    viewport: this.viewport,
                });
            }
        },

        _newId(prefix = 'node') {
            return prefix + '_' + Math.random().toString(36).substring(2, 10);
        },
    }));
});


// ============================================================================
// Transform Adapters (Data tab backward compatibility)
// ============================================================================

/**
 * Convert flat transforms array → pipeline nodes + edges (linear chain).
 *
 * @param {object} source - Data source object { id, name, source_type, path, ... }
 * @param {Array} transforms - Array of transform objects [{ type, ... }]
 * @param {object} nodeTypes - Node type definitions (for port info)
 * @returns {{ nodes: Array, edges: Array }}
 */
function transformsToPipeline(source, transforms, nodeTypes = {}) {
    const nodes = [];
    const edges = [];

    // Source node
    const sourceNode = {
        id: source.id || 'source_0',
        type: 'source',
        label: source.name || 'Source',
        config: { ...source },
        x: 40,
        y: 40,
        meta: {},
    };
    nodes.push(sourceNode);

    let prevId = sourceNode.id;

    for (let i = 0; i < transforms.length; i++) {
        const t = transforms[i];
        const nodeId = 'transform_' + i;
        const label = _transformLabel(t);

        nodes.push({
            id: nodeId,
            type: t.type,
            label: label,
            config: { ...t },
            x: 40 + (i + 1) * 240,
            y: 40,
            meta: {},
        });

        // Handle join: add right source as separate node
        if (t.type === 'join' && t.right_source_id) {
            const joinSourceId = 'join_source_' + i;
            nodes.push({
                id: joinSourceId,
                type: 'source',
                label: t.right_source_id,
                config: { id: t.right_source_id },
                x: 40 + (i + 1) * 240,
                y: 140,
                meta: {},
            });
            edges.push({
                id: 'edge_join_' + i,
                source: joinSourceId,
                sourcePort: 'output',
                target: nodeId,
                targetPort: 'right',
                label: (t.right_on || []).join(', '),
            });
        }

        edges.push({
            id: 'edge_' + i,
            source: prevId,
            sourcePort: 'output',
            target: nodeId,
            targetPort: t.type === 'join' ? 'left' : 'input',
            label: '',
        });

        prevId = nodeId;
    }

    return { nodes, edges };
}


/**
 * Convert pipeline nodes + edges → flat transforms array (topological order).
 *
 * @param {Array} nodes - Pipeline nodes
 * @param {Array} edges - Pipeline edges
 * @returns {Array} Flat transforms array (excluding source nodes)
 */
function pipelineToTransforms(nodes, edges) {
    if (nodes.length === 0) return [];

    // Find source nodes (no incoming edges)
    const targetIds = new Set(edges.map(e => e.target));
    const sourceNodeIds = new Set(nodes.filter(n => !targetIds.has(n.id) || n.type === 'source').map(n => n.id));

    // Topological sort (Kahn's algorithm)
    const inDegree = {};
    const adjList = {};
    for (const n of nodes) {
        inDegree[n.id] = 0;
        adjList[n.id] = [];
    }
    for (const e of edges) {
        if (adjList[e.source]) adjList[e.source].push(e.target);
        if (inDegree[e.target] !== undefined) inDegree[e.target]++;
    }

    const queue = [];
    for (const id of Object.keys(inDegree)) {
        if (inDegree[id] === 0) queue.push(id);
    }

    const sorted = [];
    while (queue.length > 0) {
        const id = queue.shift();
        sorted.push(id);
        for (const next of (adjList[id] || [])) {
            inDegree[next]--;
            if (inDegree[next] === 0) queue.push(next);
        }
    }

    // Extract transforms in topological order, skip source nodes
    const transforms = [];
    for (const nodeId of sorted) {
        const node = nodes.find(n => n.id === nodeId);
        if (!node || node.type === 'source') continue;
        transforms.push({ ...node.config, type: node.type });
    }

    return transforms;
}


/**
 * Generate a short label for a transform.
 */
function _transformLabel(t) {
    switch (t.type) {
        case 'filter': return `Filter: ${t.column || ''}`;
        case 'select': return `Select (${(t.columns || []).length} cols)`;
        case 'sort': return `Sort: ${(t.columns || []).join(', ')}`;
        case 'group_by': return `Group: ${(t.by || []).join(', ')}`;
        case 'rename': return 'Rename';
        case 'drop_nulls': return 'Drop Nulls';
        case 'limit': return `Limit ${t.n || ''}`;
        case 'join': return `${(t.how || 'inner').charAt(0).toUpperCase() + (t.how || 'inner').slice(1)} Join`;
        case 'add_column': return `Add: ${t.name || ''}`;
        default: return t.type;
    }
}
