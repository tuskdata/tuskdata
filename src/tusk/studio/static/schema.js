// Tusk Schema Viewer (v0.4.37)
// Real ER diagram for a chosen Postgres connection.
//
// Responsibilities:
//   • Fetch graph (tables + FKs + saved layout) per connection
//   • Render draggable .entity boxes
//   • Draw FK lines as cubic-bezier paths between entity edges
//   • Persist drag-stop layout (debounced 500ms)
//   • Pan + zoom via wheel + space-drag
//   • Click entity → highlight related (FK neighbors)
//   • Compact mode (keys only) for big schemas; double-click expands a table
//   • Auto-layout: Dagre over the FK graph using measured box sizes, isolated
//     tables packed in a grid below; masonry fallback when Dagre is missing

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
        compact: false,      // keys-only boxes (auto when many tables)
        expanded: new Set(), // tables shown in full while compact
        layoutSource: 'grid',
        hubs: new Set(),     // tables referenced by a large share of the schema
        groups: [],          // [{label, tables}] for the block labels
    };

    const COMPACT_THRESHOLD = 25;   // tables; above this compact mode is the default
    const DAGRE_MAX_TABLES = 250;
    const MIN_ZOOM = 0.08;   // above this Dagre gets slow; use the masonry packer

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
        els.btnCompact = document.getElementById('schema-compact');
        els.compactLabel = document.getElementById('schema-compact-label');
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
                // Point the Schema Watch panel at the newly selected connection.
                const watch = document.getElementById('schema-watch-host');
                if (watch && state.connId && window.htmx) {
                    watch.setAttribute('hx-get', `/api/schema-watch/${encodeURIComponent(state.connId)}/panel`);
                    htmx.process(watch);
                    htmx.trigger(watch, 'refresh');
                }
            });
        }

        if (els.btnAuto) els.btnAuto.addEventListener('click', () => { autoLayout(); fitToViewport(); });
        if (els.btnCompact) els.btnCompact.addEventListener('click', toggleCompact);
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
        state.expanded = new Set();
        state.layoutSource = data.layout_source || 'saved';
        computeHubs();
        state.compact = readCompactPref(connId, state.tables.length > COMPACT_THRESHOLD);
        updateCompactButton();
        render(() => {
            // Nobody has arranged this schema yet: place it by its FKs
            // instead of the server's placeholder grid, and show it whole.
            if (state.layoutSource !== 'saved') {
                autoLayout();
            }
            fitToViewport();
        });
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
    function render(afterMeasure) {
        renderEntities();
        // Wait one frame so we can measure box sizes before drawing edges
        requestAnimationFrame(() => {
            measureSizes();
            redrawEdges();
            renderGroupLabels();
            applyTransform();
            if (afterMeasure) afterMeasure();
        });
    }

    // ─── Compact mode ────────────────────────────────────
    function compactKey(connId) { return `tusk_schema_compact_${connId}`; }

    function readCompactPref(connId, fallback) {
        try {
            const v = localStorage.getItem(compactKey(connId));
            if (v === '1') return true;
            if (v === '0') return false;
        } catch (_) { /* ignore */ }
        return fallback;
    }

    function toggleCompact() {
        state.compact = !state.compact;
        state.expanded = new Set();
        try { localStorage.setItem(compactKey(state.connId), state.compact ? '1' : '0'); } catch (_) {}
        updateCompactButton();
        // Box heights change a lot between modes; re-arrange so nothing overlaps.
        render(() => { autoLayout(); fitToViewport(); });
    }

    function updateCompactButton() {
        if (!els.btnCompact) return;
        els.btnCompact.setAttribute('aria-pressed', state.compact ? 'true' : 'false');
        if (els.compactLabel) els.compactLabel.textContent = state.compact ? 'Compact' : 'Full';
    }

    function isCollapsed(tableName) {
        return state.compact && !state.expanded.has(tableName);
    }

    function toggleExpanded(tableName) {
        if (!state.compact) return;
        if (state.expanded.has(tableName)) state.expanded.delete(tableName);
        else state.expanded.add(tableName);
        // Re-render just this box in place; its neighbours keep their spots.
        const old = els.entitiesLayer.querySelector(`.entity[data-table="${cssEscape(tableName)}"]`);
        const t = state.tables.find((x) => x.name === tableName);
        if (!old || !t) return;
        const fresh = buildEntity(t);
        old.replaceWith(fresh);
        if (window.lucide) lucide.createIcons();
        requestAnimationFrame(() => {
            state.sizes[tableName] = { w: fresh.offsetWidth, h: fresh.offsetHeight };
            if (state.selected) selectTable(state.selected); else redrawEdges();
        });
    }

    function renderEntities() {
        const layer = els.entitiesLayer;
        layer.innerHTML = '';
        for (const t of state.tables) {
            layer.appendChild(buildEntity(t));
        }
        if (window.lucide) lucide.createIcons();
    }

    function buildEntity(t) {
        const pos = state.layout[t.name] || { x: 0, y: 0 };
        const div = document.createElement('div');
        div.className = 'entity';
        if (state.compact && state.expanded.has(t.name)) div.classList.add('expanded');
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
        if (state.hubs.has(t.name)) {
            const refs = document.createElement('span');
            refs.className = 'chip chip-violet hub-chip';
            refs.textContent = `${state.hubRefs[t.name]} refs`;
            refs.title = `Referenced by ${state.hubRefs[t.name]} tables — its lines show when you select a table`;
            head.appendChild(refs);
        }
        div.appendChild(head);

        const collapsed = isCollapsed(t.name);
        const shown = collapsed ? t.columns.filter((c) => c.is_pk || c.is_fk) : t.columns;
        for (const col of shown) {
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
        if (collapsed) {
            const hidden = t.columns.length - shown.length;
            const more = document.createElement('div');
            more.className = 'entity-more';
            more.innerHTML = `<i data-lucide="chevron-down"></i>${hidden > 0 ? `${hidden} more column${hidden === 1 ? '' : 's'}` : 'no other columns'}`;
            more.title = 'Show all columns';
            more.addEventListener('click', (e) => { e.stopPropagation(); toggleExpanded(t.name); });
            div.appendChild(more);
        }

        attachDrag(div);
        div.addEventListener('click', (e) => {
            e.stopPropagation();
            selectTable(t.name);
        });
        head.addEventListener('dblclick', (e) => {
            e.stopPropagation();
            toggleExpanded(t.name);
        });
        return div;
    }

    function measureSizes() {
        const nodes = els.entitiesLayer.querySelectorAll('.entity');
        nodes.forEach((node) => {
            const name = node.dataset.table;
            state.sizes[name] = { w: node.offsetWidth, h: node.offsetHeight };
        });
    }

    // Tables referenced by a large share of the schema (users, companies,
    // tenants…). Their FK lines are drawn only when they are selected or
    // related; at the overview they would cross everything and say nothing.
    function computeHubs() {
        const degree = {};
        for (const fk of state.fks) {
            if (fk.from_table === fk.to_table) continue;
            degree[fk.to_table] = (degree[fk.to_table] || 0) + 1;
        }
        const n = state.tables.length;
        const hubMin = Math.max(12, Math.round(n * 0.08));
        state.hubs = new Set(Object.keys(degree).filter((t) => degree[t] >= hubMin));
        state.hubRefs = degree;
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
            const touchesHub = state.hubs.has(fk.to_table) || state.hubs.has(fk.from_table);
            if (touchesHub && !(related && (fk.from_table === state.selected || fk.to_table === state.selected))) continue;
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
        const newZoom = clamp(state.zoom * factor, MIN_ZOOM, 3);
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
        const newZoom = clamp(state.zoom * factor, MIN_ZOOM, 3);
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
        // The toolbar sits at the top and the Schema watch panel at the
        // bottom-left; keep the diagram out from under both.
        const topPad = 72;
        const watch = document.getElementById('schema-watch-host');
        const bottomPad = watch && watch.offsetHeight ? watch.offsetHeight + 40 : padding;
        const availW = rect.width - padding * 2;
        const availH = rect.height - topPad - bottomPad;
        const z = clamp(Math.min(availW / bbox.w, availH / bbox.h), MIN_ZOOM, 3);
        state.zoom = z;
        // Centre the diagram in whichever axis has room left over.
        state.pan.x = Math.max(padding, (rect.width - bbox.w * z) / 2);
        state.pan.y = Math.max(topPad, topPad + (availH - bbox.h * z) / 2);
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
    const GAP_X = 60, GAP_Y = 40, MARGIN = 80;

    function nodeSize(name) {
        const t = state.tables.find((x) => x.name === name);
        const measured = state.sizes[name];
        if (measured && measured.w && measured.h) return measured;
        // Estimate before the first paint: header 38px + 29px per visible row.
        const rows = t ? (isCollapsed(name) ? t.columns.filter((c) => c.is_pk || c.is_fk).length + 1 : t.columns.length) : 4;
        return { w: 240, h: 38 + rows * 29 };
    }

    function autoLayout() {
        const names = state.tables.map((t) => t.name);
        if (!names.length) return;
        const degree = {};
        for (const fk of state.fks) {
            if (fk.from_table === fk.to_table) continue;
            degree[fk.from_table] = (degree[fk.from_table] || 0) + 1;
            degree[fk.to_table] = (degree[fk.to_table] || 0) + 1;
        }
        // Hubs (users, companies, tenants…) are referenced by half the schema.
        // Ranking through them stacks every referrer in one giant column, so
        // their edges are ignored for placement; the lines are still drawn.
        const hubMin = Math.max(12, Math.round(names.length * 0.08));
        const hubs = new Set(names.filter((n) => (degree[n] || 0) >= hubMin));

        // One block per group (prefix such as `leasing_`, or connected
        // component when there are no prefixes), laid out independently and
        // then packed into rows. Keeps related tables together and the whole
        // thing roughly screen-shaped instead of 30 screens tall.
        const blocks = [];
        for (const group of groupTables(names, degree)) {
            // Only edges that survive inside this block count: a table whose
            // sole relation is to a hub, or to another block, is "loose" here
            // and goes to the grid instead of becoming a one-node Dagre rank.
            const keepHubEdges = group.length <= 12;
            const inBlock = new Set(group);
            const localDegree = {};
            for (const fk of state.fks) {
                if (fk.from_table === fk.to_table) continue;
                if (!inBlock.has(fk.from_table) || !inBlock.has(fk.to_table)) continue;
                if (!keepHubEdges && (hubs.has(fk.to_table) || hubs.has(fk.from_table))) continue;
                localDegree[fk.from_table] = (localDegree[fk.from_table] || 0) + 1;
                localDegree[fk.to_table] = (localDegree[fk.to_table] || 0) + 1;
            }
            const linked = group.filter((n) => localDegree[n]);
            const loose = group.filter((n) => !localDegree[n]).sort();
            const canDagre = typeof dagre !== 'undefined' && linked.length && linked.length <= DAGRE_MAX_TABLES;
            let block;
            if (canDagre) {
                block = dagreBlock(linked, hubs, keepHubEdges);
            } else if (linked.length) {
                block = masonryBlock(linked.sort((a, b) => (degree[b] || 0) - (degree[a] || 0) || a.localeCompare(b)));
            } else {
                block = { pos: {}, w: 0, h: 0 };
            }
            if (loose.length) {
                const grid = masonryBlock(loose);
                for (const [n, p] of Object.entries(grid.pos)) block.pos[n] = { x: p.x, y: p.y + block.h + (block.h ? GAP_Y : 0) };
                block.w = Math.max(block.w, grid.w);
                block.h += (block.h ? GAP_Y : 0) + grid.h;
            }
            blocks.push(block);
        }
        packBlocks(blocks);

        for (const name of names) {
            const pos = state.layout[name];
            const node = els.entitiesLayer.querySelector(`.entity[data-table="${cssEscape(name)}"]`);
            if (node && pos) {
                node.style.left = pos.x + 'px';
                node.style.top = pos.y + 'px';
            }
        }
        redrawEdges();
        renderGroupLabels();
        saveLayoutDebounced();
    }

    // A small caption above each prefix group ("leasing · 24 tables"). Placed
    // at the group's top-left, so it survives reloads and most drags.
    function renderGroupLabels() {
        els.entitiesLayer.querySelectorAll('.entity-group-label').forEach((n) => n.remove());
        const names = state.tables.map((t) => t.name);
        if (names.length <= COMPACT_THRESHOLD) return;
        for (const group of groupTables(names, {})) {
            const label = commonPrefix(group);
            if (!label || group.length < 3) continue;
            let minX = Infinity, minY = Infinity;
            for (const n of group) {
                const pos = state.layout[n];
                if (!pos) continue;
                minX = Math.min(minX, pos.x);
                minY = Math.min(minY, pos.y);
            }
            if (!isFinite(minX)) continue;
            const el = document.createElement('div');
            el.className = 'entity-group-label';
            el.textContent = `${label} · ${group.length} tables`;
            el.style.left = minX + 'px';
            el.style.top = (minY - 26) + 'px';
            els.entitiesLayer.appendChild(el);
        }
    }

    function commonPrefix(names) {
        const keys = new Set(names.map((n) => {
            const bare = n.includes('.') ? n.split('.').pop() : n;
            const i = bare.indexOf('_');
            return i > 0 ? bare.slice(0, i) : '';
        }));
        return keys.size === 1 ? [...keys][0] : '';
    }

    // Groups: by name prefix when the schema uses them (Django apps, Rails
    // engines, `billing_*`), otherwise by connected component. Prefix groups
    // need at least 3 tables; the rest fall into one shared group.
    function groupTables(names, degree) {
        const byPrefix = new Map();
        for (const n of names) {
            const bare = n.includes('.') ? n.split('.').pop() : n;
            const i = bare.indexOf('_');
            const key = i > 0 ? bare.slice(0, i) : '';
            if (!byPrefix.has(key)) byPrefix.set(key, []);
            byPrefix.get(key).push(n);
        }
        const groups = [];
        const rest = [];
        for (const [key, list] of byPrefix) {
            if (key && list.length >= 3) groups.push(list);
            else rest.push(...list);
        }
        if (groups.length >= 2) {
            if (rest.length) groups.push(rest);
            return groups.sort((a, b) => b.length - a.length);
        }
        // No usable prefixes: connected components, big ones first.
        const adj = new Map();
        for (const fk of state.fks) {
            if (fk.from_table === fk.to_table) continue;
            if (!adj.has(fk.from_table)) adj.set(fk.from_table, new Set());
            if (!adj.has(fk.to_table)) adj.set(fk.to_table, new Set());
            adj.get(fk.from_table).add(fk.to_table);
            adj.get(fk.to_table).add(fk.from_table);
        }
        const seen = new Set();
        const comps = [];
        const loose = [];
        for (const n of names) {
            if (seen.has(n)) continue;
            if (!degree[n]) { loose.push(n); seen.add(n); continue; }
            const comp = [];
            const stack = [n];
            while (stack.length) {
                const x = stack.pop();
                if (seen.has(x)) continue;
                seen.add(x);
                comp.push(x);
                for (const y of adj.get(x) || []) if (!seen.has(y)) stack.push(y);
            }
            comps.push(comp);
        }
        comps.sort((a, b) => b.length - a.length);
        if (loose.length) comps.push(loose);
        return comps;
    }

    // Layered layout of one block: referencing tables left, referenced right.
    // Returns {pos: {name: {x, y}} relative to the block origin, w, h}.
    function dagreBlock(names, hubs, keepHubEdges) {
        const g = new dagre.graphlib.Graph({ multigraph: false });
        g.setGraph({ rankdir: 'LR', nodesep: GAP_Y, ranksep: GAP_X + 30, marginx: 0, marginy: 0 });
        g.setDefaultEdgeLabel(() => ({}));
        const set = new Set(names);
        for (const n of names) {
            const size = nodeSize(n);
            g.setNode(n, { width: size.w, height: size.h });
        }
        for (const fk of state.fks) {
            if (fk.from_table === fk.to_table) continue;
            if (!set.has(fk.from_table) || !set.has(fk.to_table)) continue;
            if (!keepHubEdges && (hubs.has(fk.to_table) || hubs.has(fk.from_table))) continue;
            g.setEdge(fk.from_table, fk.to_table);
        }
        dagre.layout(g);
        const pos = {};
        let w = 0, h = 0;
        for (const n of names) {
            const nd = g.node(n);
            if (!nd) continue;
            // Dagre gives centres; we position by top-left.
            pos[n] = { x: Math.round(nd.x - nd.width / 2), y: Math.round(nd.y - nd.height / 2) };
            w = Math.max(w, pos[n].x + nd.width);
            h = Math.max(h, pos[n].y + nd.height);
        }
        return { pos, w, h };
    }

    // Column packer for one block: fixed column width, each column grows by
    // the real height of what it holds. Never overlaps, whatever the sizes.
    function masonryBlock(names) {
        const cols = Math.max(1, Math.ceil(Math.sqrt(names.length * 1.6)));
        // Column width follows the widest card: long table names make cards
        // wider than the 220px minimum, and a fixed column would overlap them.
        const maxW = Math.max(240, ...names.map((n) => nodeSize(n).w));
        const colW = maxW + GAP_X;
        const heights = new Array(cols).fill(0);
        const pos = {};
        for (const n of names) {
            let c = 0;
            for (let i = 1; i < cols; i++) if (heights[i] < heights[c]) c = i;
            const size = nodeSize(n);
            pos[n] = { x: c * colW, y: Math.round(heights[c]) };
            heights[c] += size.h + GAP_Y;
        }
        return { pos, w: Math.min(names.length, cols) * colW - GAP_X, h: Math.max(0, Math.max(...heights) - GAP_Y) };
    }

    // Shelf-pack the blocks into rows so the canvas ends up roughly 16:9.
    function packBlocks(blocks) {
        const gap = GAP_X * 2;
        const area = blocks.reduce((a, b) => a + (b.w + gap) * (b.h + gap), 0);
        const rect = els.page.getBoundingClientRect();
        const aspect = rect.height > 0 ? Math.max(1.2, rect.width / rect.height) : 16 / 9;
        const widest = Math.max(...blocks.map((b) => b.w));
        const rowLimit = Math.max(widest + MARGIN, Math.sqrt(area * aspect));
        let x = MARGIN, y = MARGIN, rowH = 0;
        for (const b of blocks) {
            if (x > MARGIN && x + b.w > rowLimit) {
                x = MARGIN;
                y += rowH + gap;
                rowH = 0;
            }
            for (const [n, p] of Object.entries(b.pos)) state.layout[n] = { x: x + p.x, y: y + p.y };
            x += b.w + gap;
            rowH = Math.max(rowH, b.h);
        }
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
