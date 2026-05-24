// Tusk Schema Viewer (v0.4.4)
// Real ER diagram for a chosen Postgres connection.
//
// Responsibilities:
//   • Fetch graph (tables + FKs + saved layout) per connection
//   • Render draggable .entity boxes
//   • Draw FK lines as cubic-bezier paths between entity edges
//   • Persist drag-stop layout (debounced 500ms)
//   • Pan + zoom via wheel + space-drag
//   • Click entity → highlight related (FK neighbors)

(function () {
    'use strict';

    // ─── State ───────────────────────────────────────────
    const state = {
        connId: null,
        tables: [],          // [{name, schema, row_count, columns: [...]}]
        fks: [],             // [{from_table, from_column, to_table, to_column, ...}]
        layout: {},          // {tableName: {x, y}}
        sizes: {},           // {tableName: {w, h}} — measured after render
        pan: { x: 0, y: 0 },
        zoom: 1,
        spaceDown: false,
        selected: null,      // currently focused table name
        saveTimer: null,
    };

    // Refs (assigned after DOMContentLoaded)
    const els = {};

    // ─── Boot ────────────────────────────────────────────
    document.addEventListener('DOMContentLoaded', init);

    function init() {
        els.page = document.getElementById('schema-page');
        if (!els.page) return;

        els.picker = document.getElementById('schema-conn-picker');
        els.summary = document.getElementById('schema-summary');
        els.truncateBadge = document.getElementById('schema-truncate-badge');
        els.viewport = document.getElementById('schema-viewport');
        els.svg = document.getElementById('schema-svg');
        els.entitiesLayer = document.getElementById('schema-entities');
        els.zoomLabel = document.getElementById('schema-zoom-label');
        els.statusText = document.getElementById('schema-status-text');
        els.btnAuto = document.getElementById('schema-autolayout');
        els.btnFit = document.getElementById('schema-fit');
        els.btnZoomIn = document.getElementById('schema-zoom-in');
        els.btnZoomOut = document.getElementById('schema-zoom-out');
        els.btnReset = document.getElementById('schema-reset');

        // Initial connection from server-rendered data attribute or first
        // option in the picker.
        const initialId = els.page.dataset.connId || (els.picker && els.picker.value) || '';
        state.connId = initialId || null;

        wireEvents();
        showNavHintIfFirstVisit();

        if (state.connId) {
            loadGraph(state.connId);
        } else {
            setStatus('No PostgreSQL connections configured');
        }
    }

    // The legend hint at the bottom-right was too small for users
    // to notice (B6 in 0.4.26). Show a centered toast-style banner
    // the first time the page is opened; dismiss persists per-browser.
    function showNavHintIfFirstVisit() {
        try {
            if (localStorage.getItem('tusk_schema_nav_dismissed')) return;
        } catch (_) { /* ignore */ }
        const hint = document.getElementById('schema-nav-hint');
        const closeBtn = document.getElementById('schema-nav-hint-close');
        if (!hint) return;
        hint.style.display = 'flex';
        if (window.lucide) window.lucide.createIcons();
        const dismiss = () => {
            hint.style.display = 'none';
            try { localStorage.setItem('tusk_schema_nav_dismissed', '1'); } catch (_) {}
        };
        if (closeBtn) closeBtn.addEventListener('click', dismiss);
        // Auto-dismiss on first canvas interaction so the user isn't
        // staring at it forever.
        if (els.page) {
            els.page.addEventListener('wheel', dismiss, { once: true });
            els.page.addEventListener('mousedown', dismiss, { once: true });
        }
    }

    function wireEvents() {
        if (els.picker) {
            els.picker.addEventListener('change', () => {
                state.connId = els.picker.value || null;
                if (state.connId) loadGraph(state.connId);
            });
        }

        if (els.btnAuto) els.btnAuto.addEventListener('click', autoLayout);
        if (els.btnFit) els.btnFit.addEventListener('click', fitToViewport);
        if (els.btnZoomIn) els.btnZoomIn.addEventListener('click', () => zoomBy(1.2));
        if (els.btnZoomOut) els.btnZoomOut.addEventListener('click', () => zoomBy(1 / 1.2));
        if (els.btnReset) els.btnReset.addEventListener('click', resetView);

        // Wheel zoom (anchored to cursor)
        els.page.addEventListener('wheel', onWheel, { passive: false });

        // Space-drag to pan
        document.addEventListener('keydown', (e) => {
            // Don't hijack space when typing in form fields
            const tag = (e.target && e.target.tagName) || '';
            if (e.code === 'Space' && tag !== 'INPUT' && tag !== 'TEXTAREA' && tag !== 'SELECT') {
                state.spaceDown = true;
                els.page.classList.add('pannable');
                e.preventDefault();
            }
        });
        document.addEventListener('keyup', (e) => {
            if (e.code === 'Space') {
                state.spaceDown = false;
                els.page.classList.remove('pannable', 'panning');
            }
        });

        els.page.addEventListener('mousedown', onPanStart);

        // Background click clears selection
        els.viewport.addEventListener('mousedown', (e) => {
            if (e.target === els.viewport || e.target === els.entitiesLayer) {
                clearSelection();
            }
        });
    }

    // ─── Networking ──────────────────────────────────────
    async function loadGraph(connId) {
        setStatus('Loading schema…');
        const data = await tuskFetchJSON(`/api/connections/${encodeURIComponent(connId)}/schema-graph`, {
            timeoutMs: 60000,
        });
        if (data.error) {
            setStatus(`Error: ${data.error}`);
            tuskToast(data.error, 'error');
            els.entitiesLayer.innerHTML = '';
            els.svg.innerHTML = '';
            return;
        }
        state.tables = data.tables || [];
        state.fks = data.fks || [];
        state.layout = data.layout || {};
        state.truncated = !!data.truncated;
        state.totalTables = data.total_tables || state.tables.length;
        state.sizes = {};
        state.selected = null;
        render();
        if (els.summary) {
            const summary = `${state.tables.length} tables · ${state.fks.length} FKs`;
            els.summary.textContent = summary;
        }
        // Render the truncate badge in the toolbar. Backend caps the
        // schema-graph response at 500 tables (v0.4.8.2 audit #10);
        // when that fires we tell the user how many were dropped so
        // they don't think they have a bug.
        if (els.truncateBadge) {
            if (state.truncated) {
                els.truncateBadge.textContent =
                    `Showing ${state.tables.length} of ${state.totalTables} tables`;
                els.truncateBadge.style.display = '';
            } else {
                els.truncateBadge.style.display = 'none';
            }
        }
        setStatus(`Loaded ${state.tables.length} tables, ${state.fks.length} foreign keys`);
    }

    function saveLayoutDebounced() {
        if (state.saveTimer) clearTimeout(state.saveTimer);
        state.saveTimer = setTimeout(saveLayout, 500);
    }

    async function saveLayout() {
        if (!state.connId) return;
        const result = await tuskFetchJSON(`/api/connections/${encodeURIComponent(state.connId)}/schema-layout`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ layout: state.layout }),
            timeoutMs: 5000,
        });
        if (result.error) {
            tuskToast(`Save failed: ${result.error}`, 'error');
        }
    }

    // ─── Render ──────────────────────────────────────────
    function render() {
        renderEntities();
        // Wait one frame so we can measure box sizes before drawing edges
        requestAnimationFrame(() => {
            measureSizes();
            redrawEdges();
            applyTransform();
        });
    }

    function renderEntities() {
        const layer = els.entitiesLayer;
        layer.innerHTML = '';
        for (const t of state.tables) {
            const pos = state.layout[t.name] || { x: 0, y: 0 };
            const div = document.createElement('div');
            div.className = 'entity';
            div.dataset.table = t.name;
            div.style.left = pos.x + 'px';
            div.style.top = pos.y + 'px';

            const head = document.createElement('div');
            head.className = 'entity-head';
            const icon = document.createElement('i');
            icon.setAttribute('data-lucide', 'table-2');
            head.appendChild(icon);
            const nameSpan = document.createElement('span');
            nameSpan.textContent = t.name;
            head.appendChild(nameSpan);
            const count = document.createElement('span');
            count.className = 'row-count';
            count.textContent = formatCount(t.row_count);
            head.appendChild(count);
            div.appendChild(head);

            for (const col of t.columns) {
                const row = document.createElement('div');
                row.className = 'entity-row';
                const dot = document.createElement('span');
                if (col.is_pk) {
                    dot.className = 'pk';
                    dot.textContent = '●';
                } else if (col.is_fk) {
                    dot.className = 'fk';
                    dot.textContent = '●';
                } else {
                    dot.style.opacity = '0';
                    dot.textContent = '●';
                }
                row.appendChild(dot);
                const cn = document.createElement('span');
                cn.className = 'col-name';
                cn.textContent = col.name;
                row.appendChild(cn);
                const ct = document.createElement('span');
                ct.className = 'col-type';
                ct.textContent = col.type;
                row.appendChild(ct);
                div.appendChild(row);
            }

            attachDrag(div);
            div.addEventListener('click', (e) => {
                e.stopPropagation();
                selectTable(t.name);
            });
            layer.appendChild(div);
        }
        if (window.lucide) lucide.createIcons();
    }

    function measureSizes() {
        const nodes = els.entitiesLayer.querySelectorAll('.entity');
        nodes.forEach((node) => {
            const name = node.dataset.table;
            state.sizes[name] = { w: node.offsetWidth, h: node.offsetHeight };
        });
    }

    // ─── Edge rendering ──────────────────────────────────
    function redrawEdges() {
        const svg = els.svg;
        // Size SVG to bounding box of entities so paths route inside it.
        const bbox = computeContentBBox();
        svg.setAttribute('width', String(bbox.w));
        svg.setAttribute('height', String(bbox.h));
        svg.style.width = bbox.w + 'px';
        svg.style.height = bbox.h + 'px';

        const related = state.selected ? relatedTables(state.selected) : null;
        const parts = [];
        for (let i = 0; i < state.fks.length; i++) {
            const fk = state.fks[i];
            const from = anchorPoint(fk.from_table, 'right');
            const to = anchorPoint(fk.to_table, 'left');
            if (!from || !to) continue;
            const dx = Math.max(40, Math.abs(to.x - from.x) * 0.5);
            const c1x = from.x + dx;
            const c1y = from.y;
            const c2x = to.x - dx;
            const c2y = to.y;
            const d = `M ${from.x} ${from.y} C ${c1x} ${c1y}, ${c2x} ${c2y}, ${to.x} ${to.y}`;
            let cls = '';
            if (related) {
                if (related.has(fk.from_table) && related.has(fk.to_table)
                    && (fk.from_table === state.selected || fk.to_table === state.selected)) {
                    cls = 'related';
                } else {
                    cls = 'dim';
                }
            }
            parts.push(`<path d="${d}" class="${cls}" data-from="${escapeAttr(fk.from_table)}" data-to="${escapeAttr(fk.to_table)}"/>`);
        }
        svg.innerHTML = parts.join('');
    }

    function anchorPoint(tableName, side) {
        const pos = state.layout[tableName];
        const size = state.sizes[tableName];
        if (!pos || !size) return null;
        const y = pos.y + size.h / 2;
        const x = side === 'right' ? pos.x + size.w : pos.x;
        return { x, y };
    }

    function computeContentBBox() {
        let maxX = 0, maxY = 0;
        for (const t of state.tables) {
            const pos = state.layout[t.name];
            const size = state.sizes[t.name] || { w: 240, h: 120 };
            if (!pos) continue;
            if (pos.x + size.w > maxX) maxX = pos.x + size.w;
            if (pos.y + size.h > maxY) maxY = pos.y + size.h;
        }
        return { w: maxX + 200, h: maxY + 200 };
    }

    // ─── Selection / highlight ───────────────────────────
    function selectTable(name) {
        state.selected = name;
        const related = relatedTables(name);
        els.entitiesLayer.querySelectorAll('.entity').forEach((node) => {
            const t = node.dataset.table;
            node.classList.remove('focus', 'related', 'dim');
            if (t === name) {
                node.classList.add('focus');
            } else if (related.has(t)) {
                node.classList.add('related');
            } else {
                node.classList.add('dim');
            }
        });
        redrawEdges();
    }

    function clearSelection() {
        state.selected = null;
        els.entitiesLayer.querySelectorAll('.entity').forEach((node) => {
            node.classList.remove('focus', 'related', 'dim');
        });
        redrawEdges();
    }

    function relatedTables(name) {
        const set = new Set([name]);
        for (const fk of state.fks) {
            if (fk.from_table === name) set.add(fk.to_table);
            if (fk.to_table === name) set.add(fk.from_table);
        }
        return set;
    }

    // ─── Dragging ────────────────────────────────────────
    function attachDrag(node) {
        let startX = 0, startY = 0, origX = 0, origY = 0, dragging = false;
        const onDown = (e) => {
            // Space-drag pans the canvas instead of moving the node
            if (state.spaceDown || e.button !== 0) return;
            e.preventDefault();
            e.stopPropagation();
            dragging = true;
            node.classList.add('dragging');
            startX = e.clientX;
            startY = e.clientY;
            const name = node.dataset.table;
            const pos = state.layout[name] || { x: 0, y: 0 };
            origX = pos.x;
            origY = pos.y;
            document.addEventListener('mousemove', onMove);
            document.addEventListener('mouseup', onUp);
        };
        const onMove = (e) => {
            if (!dragging) return;
            const dx = (e.clientX - startX) / state.zoom;
            const dy = (e.clientY - startY) / state.zoom;
            const name = node.dataset.table;
            const nx = origX + dx;
            const ny = origY + dy;
            state.layout[name] = { x: nx, y: ny };
            node.style.left = nx + 'px';
            node.style.top = ny + 'px';
            redrawEdges();
        };
        const onUp = () => {
            if (!dragging) return;
            dragging = false;
            node.classList.remove('dragging');
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
            saveLayoutDebounced();
        };
        node.addEventListener('mousedown', onDown);
    }

    // ─── Pan + zoom ──────────────────────────────────────
    function onWheel(e) {
        e.preventDefault();
        const rect = els.page.getBoundingClientRect();
        const cx = e.clientX - rect.left;
        const cy = e.clientY - rect.top;

        const factor = e.deltaY < 0 ? 1.1 : 1 / 1.1;
        const newZoom = clamp(state.zoom * factor, 0.2, 3);
        // Anchor zoom to cursor: keep the world-point under the cursor stable
        const worldX = (cx - state.pan.x) / state.zoom;
        const worldY = (cy - state.pan.y) / state.zoom;
        state.zoom = newZoom;
        state.pan.x = cx - worldX * newZoom;
        state.pan.y = cy - worldY * newZoom;
        applyTransform();
    }

    function onPanStart(e) {
        if (!state.spaceDown) return;
        if (e.button !== 0) return;
        e.preventDefault();
        const startX = e.clientX;
        const startY = e.clientY;
        const origX = state.pan.x;
        const origY = state.pan.y;
        els.page.classList.add('panning');
        const onMove = (ev) => {
            state.pan.x = origX + (ev.clientX - startX);
            state.pan.y = origY + (ev.clientY - startY);
            applyTransform();
        };
        const onUp = () => {
            els.page.classList.remove('panning');
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
        };
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
    }

    function zoomBy(factor) {
        const newZoom = clamp(state.zoom * factor, 0.2, 3);
        // Zoom toward viewport center
        const rect = els.page.getBoundingClientRect();
        const cx = rect.width / 2;
        const cy = rect.height / 2;
        const worldX = (cx - state.pan.x) / state.zoom;
        const worldY = (cy - state.pan.y) / state.zoom;
        state.zoom = newZoom;
        state.pan.x = cx - worldX * newZoom;
        state.pan.y = cy - worldY * newZoom;
        applyTransform();
    }

    function resetView() {
        state.pan = { x: 0, y: 0 };
        state.zoom = 1;
        applyTransform();
    }

    function fitToViewport() {
        const bbox = computeContentBBox();
        if (!bbox.w || !bbox.h) return;
        const rect = els.page.getBoundingClientRect();
        const padding = 60;
        const sx = (rect.width - padding * 2) / bbox.w;
        const sy = (rect.height - padding * 2) / bbox.h;
        const z = clamp(Math.min(sx, sy), 0.2, 3);
        state.zoom = z;
        state.pan.x = padding;
        state.pan.y = padding;
        applyTransform();
    }

    function applyTransform() {
        els.viewport.style.transform =
            `translate(${state.pan.x}px, ${state.pan.y}px) scale(${state.zoom})`;
        if (els.zoomLabel) {
            els.zoomLabel.textContent = `${Math.round(state.zoom * 100)}%`;
        }
    }

    // ─── Auto-layout ─────────────────────────────────────
    function autoLayout() {
        // Deterministic grid sorted by FK degree (high → low) then name.
        const degree = {};
        for (const fk of state.fks) {
            degree[fk.from_table] = (degree[fk.from_table] || 0) + 1;
            degree[fk.to_table] = (degree[fk.to_table] || 0) + 1;
        }
        const sorted = state.tables.slice().sort((a, b) => {
            const da = degree[a.name] || 0;
            const db = degree[b.name] || 0;
            if (da !== db) return db - da;
            return a.name.localeCompare(b.name);
        });
        const cols = Math.max(1, Math.ceil(Math.sqrt(Math.max(1, sorted.length))));
        const cellW = 280, cellH = 240, mx = 80, my = 80;
        for (let i = 0; i < sorted.length; i++) {
            const r = Math.floor(i / cols);
            const c = i % cols;
            const name = sorted[i].name;
            const pos = { x: mx + c * cellW, y: my + r * cellH };
            state.layout[name] = pos;
            const node = els.entitiesLayer.querySelector(`.entity[data-table="${cssEscape(name)}"]`);
            if (node) {
                node.style.left = pos.x + 'px';
                node.style.top = pos.y + 'px';
            }
        }
        redrawEdges();
        saveLayoutDebounced();
    }

    // ─── Helpers ─────────────────────────────────────────
    function clamp(v, lo, hi) { return Math.max(lo, Math.min(hi, v)); }
    function setStatus(msg) { if (els.statusText) els.statusText.textContent = msg; }
    function escapeAttr(s) { return String(s).replace(/"/g, '&quot;').replace(/</g, '&lt;'); }
    function cssEscape(s) {
        if (window.CSS && CSS.escape) return CSS.escape(s);
        return String(s).replace(/"/g, '\\"');
    }
    function formatCount(n) {
        if (!n || n < 1) return '';
        if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
        if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
        return String(n);
    }
})();
