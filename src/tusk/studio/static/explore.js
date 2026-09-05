// Tusk Explore — per-column data profile.
// Single Alpine component bound on #explore-page. Owns connection picker,
// table picker, sample size, profile fetch, drill modal.

function exploreApp() {
    return {
        // ── State ──────────────────────────────────────────────
        selectedConn: '',
        selectedTable: '',          // "schema.table"
        sampleSize: 10000,
        tables: [],                 // [{schema, table, qualified}]
        loadingTables: false,
        profile: null,              // {schema, table, sampled_rows, sample_size, columns}
        loading: false,
        error: '',
        drill: null,                // currently-opened column

        // ── Lifecycle ─────────────────────────────────────────
        init() {
            // Re-render Lucide icons after Alpine paints — both initial paint and
            // every x-show toggle of cards.
            this.$nextTick(() => window.lucide?.createIcons());
            this.$watch('profile', () => this.$nextTick(() => window.lucide?.createIcons()));
            this.$watch('drill', () => this.$nextTick(() => window.lucide?.createIcons()));
            this.$watch('tables', () => this.$nextTick(() => window.lucide?.createIcons()));
        },

        // ── Pickers ───────────────────────────────────────────
        async onConnChange() {
            this.selectedTable = '';
            this.tables = [];
            this.profile = null;
            this.h3 = null;
            this.error = '';
            if (!this.selectedConn) return;
            this.loadingTables = true;
            const data = await window.tuskFetchJSON(`/api/connections/${encodeURIComponent(this.selectedConn)}/schema`);
            this.loadingTables = false;
            if (data.error) {
                this.error = `Failed to load tables: ${data.error}`;
                return;
            }
            // Schema endpoint returns {schema_name: {table_name: [columns]}}
            const flat = [];
            for (const [schemaName, tables] of Object.entries(data || {})) {
                if (typeof tables !== 'object' || tables === null) continue;
                for (const tableName of Object.keys(tables)) {
                    flat.push({
                        schema: schemaName,
                        table: tableName,
                        qualified: `${schemaName}.${tableName}`,
                    });
                }
            }
            flat.sort((a, b) => a.qualified.localeCompare(b.qualified));
            this.tables = flat;
        },

        onTableChange() {
            // Don't auto-run — wait for the user to click Auto-explore so they
            // can pick a sample size first. Just clear stale state.
            this.profile = null;
            this.error = '';
        },

        canProfile() {
            return Boolean(this.selectedConn && this.selectedTable);
        },

        // ── Profile ───────────────────────────────────────────
        async runProfile() {
            if (!this.canProfile()) return;
            const t = this.tables.find(x => x.qualified === this.selectedTable);
            if (!t) { this.error = 'Pick a valid table'; return; }
            this.loading = true;
            this.error = '';
            this.profile = null;

            const body = {
                connection_id: this.selectedConn,
                schema: t.schema,
                table: t.table,
                sample_size: this.sampleSize,
            };

            const data = await window.tuskFetchJSON('/api/explore/profile', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
                timeoutMs: 180000, // big tables can take a while
            });
            this.loading = false;

            if (data.error) {
                this.error = data.error;
                return;
            }
            this.profile = data;
        },

        // ── H3 density grid (Spatial card) ───────────────────
        h3: null,
        h3Res: 8,
        h3Loading: false,
        _h3Map: null,
        async loadH3() {
            const t = this.tables.find(x => x.qualified === this.selectedTable);
            if (!t || !this.selectedConn) return;
            this.h3Loading = true;
            try {
                const data = await window.tuskFetchJSON('/api/explore/h3', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ connection_id: this.selectedConn, schema: t.schema, table: t.table, resolution: this.h3Res }),
                    timeoutMs: 120000,
                });
                if (data.error) { tuskToast(data.error, 'error'); return; }
                this.h3 = data;
                this.$nextTick(() => this.renderH3(data));
            } catch (e) {
                tuskToast('Density grid failed: ' + e.message, 'error');
            } finally {
                this.h3Loading = false;
            }
        },
        renderH3(fc) {
            if (typeof maplibregl === 'undefined') { tuskToast('Map library not loaded', 'warning'); return; }
            const el = document.getElementById('explore-h3-map');
            if (!el) return;
            const paint = {
                'fill-color': ['interpolate', ['linear'], ['get', 'count'], 1, '#fde68a', Math.max(2, fc.max_count / 2), '#f97316', Math.max(3, fc.max_count), '#7f1d1d'],
                'fill-opacity': 0.65,
            };
            const apply = (map) => {
                if (map.getSource('h3')) {
                    map.getSource('h3').setData(fc);
                    map.setPaintProperty('h3-fill', 'fill-color', paint['fill-color']);
                } else {
                    map.addSource('h3', { type: 'geojson', data: fc });
                    map.addLayer({ id: 'h3-fill', type: 'fill', source: 'h3', paint });
                    map.addLayer({ id: 'h3-line', type: 'line', source: 'h3', paint: { 'line-color': '#7f1d1d', 'line-opacity': 0.25, 'line-width': 0.5 } });
                    map.on('click', 'h3-fill', (e) => {
                        const p = e.features[0].properties;
                        new maplibregl.Popup({ closeButton: false }).setLngLat(e.lngLat).setHTML(`<div style="font:12px var(--font-mono)">${p.count} rows<br>${p.h3}</div>`).addTo(map);
                    });
                }
                if (fc.features.length) {
                    const b = new maplibregl.LngLatBounds();
                    for (const f of fc.features) for (const c of f.geometry.coordinates[0]) b.extend(c);
                    map.fitBounds(b, { padding: 30, duration: 0, maxZoom: 14 });
                }
            };
            if (this._h3Map && this._h3Map.getContainer() === el) {
                apply(this._h3Map);
                return;
            }
            const map = new maplibregl.Map({ container: el, style: window.tuskBasemapStyle(), center: [0, 20], zoom: 1, attributionControl: false });
            map.addControl(new maplibregl.NavigationControl({ showCompass: false }));
            map.on('load', () => apply(map));
            this._h3Map = map;
        },

        // ── Drill modal ───────────────────────────────────────
        openDrill(col) {
            this.drill = col;
        },

        // ── Helpers (used by template bindings) ───────────────
        isNumeric(col) {
            const dt = (col?.dtype || '').toLowerCase();
            return /int|float|decimal|number|double|i8|i16|i32|i64|u8|u16|u32|u64|f32|f64/.test(dt);
        },

        shortDtype(dt) {
            if (!dt) return 'unknown';
            // Strip Polars wrapping like 'Int64' → 'int64'; just lowercase + trim.
            return String(dt).replace(/^Class\s+/, '').toLowerCase();
        },

        iconForDtype(dt) {
            const d = this.shortDtype(dt);
            if (this.isNumeric({ dtype: dt })) return 'hash';
            if (/date|time/.test(d)) return 'calendar';
            if (/bool/.test(d)) return 'toggle-left';
            if (/list|array|struct|object/.test(d)) return 'braces';
            return 'type';
        },

        colorForDtype(dt) {
            const d = this.shortDtype(dt);
            if (this.isNumeric({ dtype: dt })) return 'var(--accent-amber)';
            if (/date|time/.test(d)) return 'var(--accent-teal)';
            if (/bool/.test(d)) return 'var(--accent-violet)';
            return 'var(--accent-violet)';
        },

        qualityChip(nullPct) {
            if (nullPct === 0) return 'chip-green';
            if (nullPct < 5) return 'chip-amber';
            return 'chip-rose';
        },

        qualityLabel(col) {
            if (col.null_pct === 0) return '100% complete';
            return `${col.null_pct}% nulls`;
        },

        pctOfMax(count, list) {
            if (!list || !list.length) return 0;
            const max = Math.max(...list.map(x => x.count));
            if (!max) return 0;
            return Math.max(2, (count / max) * 100);
        },

        fmtNum(v) {
            if (v === null || v === undefined) return '—';
            if (typeof v === 'number') {
                if (Number.isInteger(v)) return v.toLocaleString();
                return v.toLocaleString(undefined, { maximumFractionDigits: 4 });
            }
            return String(v);
        },

        fmtVal(v) {
            if (v === null || v === undefined) return '∅ null';
            if (typeof v === 'string') {
                return v.length > 40 ? v.slice(0, 40) + '…' : v;
            }
            if (typeof v === 'number') return v.toLocaleString();
            return String(v);
        },
    };
}

// Expose for Alpine templating
window.exploreApp = exploreApp;
