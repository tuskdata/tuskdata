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
        prefix: '',          // group filter ('' = all)
        onlyRelated: false,  // hide everything but the selection's neighbourhood
        searchIndex: -1,
        savedLayout: null,   // positions to restore when leaving only-related
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
        els.search = document.getElementById('schema-search');
        els.searchResults = document.getElementById('schema-search-results');
        els.prefix = document.getElementById('schema-prefix');
        els.btnRelated = document.getElementById('schema-related');
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
        if (els.btnRelated) els.btnRelated.addEventListener('click', toggleOnlyRelated);
        if (els.prefix) els.prefix.addEventListener('change', () => { state.prefix = els.prefix.value; applyVisibility(); fitToViewport(); });
        if (els.search) {
            els.search.addEventListener('input', renderSearch);
            els.search.addEventListener('focus', renderSearch);
            els.search.addEventListener('keydown', onSearchKey);
            els.search.addEventListener('blur', () => setTimeout(hideSearch, 150));
            document.addEventListener('keydown', (e) => {
                const tag = (e.target && e.target.tagName) || '';
                if (e.key === '/' && tag !== 'INPUT' && tag !== 'TEXTAREA' && tag !== 'SELECT') {
                    e.preventDefault();
                    els.search.focus();
                    els.search.select();
                }
            });
        }
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
        state.prefix = '';
        state.onlyRelated = false;
        state.savedLayout = null;
        computeHubs();
        populatePrefixFilter();
        updateRelatedButton();
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
        if (state.savedLayout) return;  // temporary neighbourhood layout, never persisted
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
        const extentW = bbox.x + bbox.w + 200;
        const extentH = bbox.y + bbox.h + 200;
        svg.setAttribute('width', String(extentW));
        svg.setAttribute('height', String(extentH));
        svg.style.width = extentW + 'px';
        svg.style.height = extentH + 'px';

        const related = state.selected ? relatedTables(state.selected) : null;
        const parts = [];
        for (let i = 0; i < state.fks.length; i++) {
            const fk = state.fks[i];
            if (!isVisible(fk.from_table) || !isVisible(fk.to_table)) continue;
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
        let minX = Infinity, minY = Infinity, maxX = 0, maxY = 0;
        for (const t of state.tables) {
            if (!isVisible(t.name)) continue;
            const pos = state.layout[t.name];
            const size = state.sizes[t.name] || { w: 240, h: 120 };
            if (!pos) continue;
            minX = Math.min(minX, pos.x);
            minY = Math.min(minY, pos.y);
            if (pos.x + size.w > maxX) maxX = pos.x + size.w;
            if (pos.y + size.h > maxY) maxY = pos.y + size.h;
        }
        if (!isFinite(minX)) return { x: 0, y: 0, w: 0, h: 0 };
        return { x: minX, y: minY, w: maxX - minX, h: maxY - minY };
    }

    // ─── Selection / highlight ───────────────────────────
    function selectTable(name) {
        const changed = state.selected !== name;
        state.selected = name;
        if (state.onlyRelated) {
            applyVisibility();
            if (changed) layoutNeighbourhood();
        }
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
        if (state.onlyRelated) applyVisibility();
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
        state.pan.x = Math.max(padding, (rect.width - bbox.w * z) / 2) - bbox.x * z;
        state.pan.y = Math.max(topPad, topPad + (availH - bbox.h * z) / 2) - bbox.y * z;
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

    // ─── Navigation: search, group filter, only-related ──
    function tablePrefix(name) {
        const bare = name.includes('.') ? name.split('.').pop() : name;
        const i = bare.indexOf('_');
        return i > 0 ? bare.slice(0, i) : '';
    }

    function populatePrefixFilter() {
        if (!els.prefix) return;
        const counts = {};
        for (const t of state.tables) {
            const k = tablePrefix(t.name);
            if (k) counts[k] = (counts[k] || 0) + 1;
        }
        const groups = Object.keys(counts).filter((k) => counts[k] >= 3).sort();
        els.prefix.innerHTML = '<option value="">All groups</option>' +
            groups.map((g) => `<option value="${escapeAttr(g)}">${escapeAttr(g)} · ${counts[g]}</option>`).join('');
        els.prefix.value = '';
        els.prefix.hidden = groups.length < 2;
    }

    function isVisible(name) {
        if (state.prefix && tablePrefix(name) !== state.prefix) return false;
        if (state.onlyRelated && state.selected) return relatedTables(state.selected).has(name);
        return true;
    }

    function applyVisibility() {
        els.entitiesLayer.querySelectorAll('.entity').forEach((node) => {
            node.classList.toggle('hidden-table', !isVisible(node.dataset.table));
        });
        els.entitiesLayer.querySelectorAll('.entity-group-label').forEach((el) => {
            const key = el.textContent.split(' · ')[0];
            el.hidden = !!state.prefix && key !== state.prefix;
        });
        redrawEdges();
    }

    function toggleOnlyRelated() {
        state.onlyRelated = !state.onlyRelated;
        updateRelatedButton();
        if (state.onlyRelated) {
            applyVisibility();
            if (state.selected) layoutNeighbourhood();
            else setStatus('Only related: click a table to see its neighbourhood');
        } else {
            restoreLayout();
            applyVisibility();
            fitToViewport();
        }
    }

    // Arrange just the visible neighbourhood around the selection so it reads
    // like a small diagram instead of a few boxes scattered over the whole
    // canvas. Positions are temporary: leaving the mode restores the real
    // layout and nothing is saved meanwhile.
    function layoutNeighbourhood() {
        if (!state.selected) return;
        if (!state.savedLayout) state.savedLayout = { ...state.layout };
        const names = state.tables.map((t) => t.name).filter(isVisible);
        // Star layout: tables pointing at the selection on the left, the
        // selection in the middle, tables it points at on the right. Each
        // side is a grid, so a table referenced by 40 others stays one
        // screen wide instead of a 40-box column.
        const sel = state.selected;
        const left = new Set(), right = new Set();
        for (const fk of state.fks) {
            if (fk.to_table === sel && fk.from_table !== sel) left.add(fk.from_table);
            if (fk.from_table === sel && fk.to_table !== sel) right.add(fk.to_table);
        }
        for (const n of left) if (right.has(n)) right.delete(n);
        const leftNames = names.filter((n) => left.has(n)).sort();
        const rightNames = names.filter((n) => right.has(n)).sort();
        const lb = leftNames.length ? masonryBlock(leftNames) : { pos: {}, w: 0, h: 0 };
        const rb = rightNames.length ? masonryBlock(rightNames) : { pos: {}, w: 0, h: 0 };
        const selSize = nodeSize(sel);
        const totalH = Math.max(lb.h, rb.h, selSize.h);
        let x = MARGIN;
        for (const [n, pos] of Object.entries(lb.pos)) state.layout[n] = { x: x + pos.x, y: MARGIN + (totalH - lb.h) / 2 + pos.y };
        x += lb.w + (lb.w ? GAP_X * 2 : 0);
        state.layout[sel] = { x, y: MARGIN + (totalH - selSize.h) / 2 };
        x += selSize.w + GAP_X * 2;
        for (const [n, pos] of Object.entries(rb.pos)) state.layout[n] = { x: x + pos.x, y: MARGIN + (totalH - rb.h) / 2 + pos.y };
        placeNodes(names);
        redrawEdges();
        fitToViewport();
    }

    function restoreLayout() {
        if (!state.savedLayout) return;
        state.layout = state.savedLayout;
        state.savedLayout = null;
        placeNodes(state.tables.map((t) => t.name));
        redrawEdges();
        renderGroupLabels();
    }

    function placeNodes(names) {
        for (const name of names) {
            const pos = state.layout[name];
            const node = els.entitiesLayer.querySelector(`.entity[data-table="${cssEscape(name)}"]`);
            if (node && pos) {
                node.style.left = pos.x + 'px';
                node.style.top = pos.y + 'px';
            }
        }
    }

    function updateRelatedButton() {
        if (els.btnRelated) els.btnRelated.setAttribute('aria-pressed', state.onlyRelated ? 'true' : 'false');
    }

    // Select a table and bring it to the middle of the screen, readable.
    function focusTable(name) {
        const pos = state.layout[name];
        const size = state.sizes[name] || { w: 240, h: 120 };
        if (!pos) return;
        if (state.prefix && tablePrefix(name) !== state.prefix) {
            state.prefix = '';
            if (els.prefix) els.prefix.value = '';
        }
        selectTable(name);
        if (state.onlyRelated) return;  // layoutNeighbourhood() already framed it
        const rect = els.page.getBoundingClientRect();
        const z = state.zoom < 0.6 ? 0.9 : state.zoom;
        state.zoom = z;
        state.pan.x = rect.width / 2 - (pos.x + size.w / 2) * z;
        state.pan.y = rect.height / 2 - (pos.y + size.h / 2) * z;
        applyTransform();
    }

    function searchMatches() {
        const q = (els.search.value || '').trim().toLowerCase();
        if (!q) return [];
        const starts = [], contains = [];
        for (const t of state.tables) {
            const n = t.name.toLowerCase();
            if (n.startsWith(q) || n.split('.').pop().startsWith(q)) starts.push(t);
            else if (n.includes(q)) contains.push(t);
        }
        return starts.concat(contains).slice(0, 10);
    }

    function renderSearch() {
        if (!els.searchResults) return;
        const matches = searchMatches();
        if (!matches.length) { hideSearch(); return; }
        state.searchIndex = Math.min(Math.max(state.searchIndex, 0), matches.length - 1);
        els.searchResults.innerHTML = matches.map((t, i) =>
            `<div class="schema-search-item ${i === state.searchIndex ? 'active' : ''}" data-table="${escapeAttr(t.name)}">` +
            `<span>${escapeAttr(t.name)}</span><span class="muted">${t.columns.length} cols${t.row_count ? ' · ' + formatCount(t.row_count) : ''}</span></div>`
        ).join('');
        els.searchResults.hidden = false;
        els.searchResults.querySelectorAll('.schema-search-item').forEach((el) => {
            el.addEventListener('mousedown', (e) => { e.preventDefault(); pickSearch(el.dataset.table); });
        });
    }

    function onSearchKey(e) {
        const matches = searchMatches();
        if (e.key === 'ArrowDown') { e.preventDefault(); state.searchIndex = Math.min(state.searchIndex + 1, matches.length - 1); renderSearch(); }
        else if (e.key === 'ArrowUp') { e.preventDefault(); state.searchIndex = Math.max(state.searchIndex - 1, 0); renderSearch(); }
        else if (e.key === 'Enter') { e.preventDefault(); if (matches.length) pickSearch(matches[Math.max(0, state.searchIndex)].name); }
        else if (e.key === 'Escape') { hideSearch(); els.search.blur(); }
    }

    function pickSearch(name) {
        hideSearch();
        els.search.value = name;
        focusTable(name);
    }

    function hideSearch() {
        if (els.searchResults) els.searchResults.hidden = true;
        state.searchIndex = -1;
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
