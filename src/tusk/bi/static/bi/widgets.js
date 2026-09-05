/**
 * Tusk BI — Widget renderers (chart / stat sparkline / table conditional / pivot / map / text markdown).
 *
 * Server-rendered partials emit `<script>` tags that call into this module
 * via globals. Keeping the per-render code here (instead of inlined in the
 * partial) reduces template noise and lets the script load once per page.
 */

(function () {
    function commonChartOptions(type) {
        const isPolar = type === 'pie' || type === 'doughnut' || type === 'radar';
        return {
            responsive: true,
            maintainAspectRatio: false,
            animation: { duration: 400, easing: 'easeOutQuart' },
            plugins: {
                legend: {
                    labels: {
                        color: 'rgba(139, 148, 158, 0.9)',
                        boxWidth: 10,
                        font: { size: 10, family: "'Inter', sans-serif" },
                        padding: 12,
                    },
                },
                tooltip: {
                    backgroundColor: 'rgba(22, 27, 34, 0.95)',
                    borderColor: 'rgba(48, 54, 61, 0.8)',
                    borderWidth: 1,
                    titleFont: { size: 11, family: "'Inter', sans-serif" },
                    bodyFont: { size: 11, family: "'Inter', sans-serif" },
                    padding: 8,
                    cornerRadius: 6,
                },
            },
            scales: isPolar ? {} : {
                x: {
                    ticks: { color: 'rgba(139, 148, 158, 0.7)', font: { size: 10 } },
                    grid: { color: 'rgba(48, 54, 61, 0.4)', drawBorder: false },
                },
                y: {
                    ticks: { color: 'rgba(139, 148, 158, 0.7)', font: { size: 10 } },
                    grid: { color: 'rgba(48, 54, 61, 0.4)', drawBorder: false },
                },
            },
        };
    }

    /** Render a chart widget. Called from chart.html partial. */
    window.biRenderChart = function (widgetId, config) {
        const ctx = document.getElementById('widget-chart-' + widgetId);
        if (!ctx || typeof Chart === 'undefined') return;
        const existing = Chart.getChart(ctx);
        if (existing) existing.destroy();

        const opts = Object.assign(commonChartOptions(config.type || 'bar'), config.options || {});
        new Chart(ctx.getContext('2d'), {
            type: config.type || 'bar',
            data: config.data || { labels: [], datasets: [] },
            options: opts,
        });
    };

    /** Render a sparkline beneath the stat value (full card width).
     *  v0.3.0 layout: line tinted to the brand color for visual weight,
     *  filled with a soft brand wash so the trend reads at a glance.
     *  Falls back to a CSS-tinted line if Chart.js is unavailable. */
    window.biRenderSparkline = function (widgetId, values) {
        const ctx = document.getElementById('sparkline-' + widgetId);
        if (!ctx || typeof Chart === 'undefined') return;
        const existing = Chart.getChart(ctx);
        if (existing) existing.destroy();

        // Color hint: rising vs falling end-to-end. Green for up, rose
        // for down. Default brand orange for flat / unknown.
        let line = 'var(--brand, #d4502b)';
        let fill = 'rgba(212,80,43,0.10)';
        if (values.length >= 2) {
            const first = Number(values[0]);
            const last = Number(values[values.length - 1]);
            if (Number.isFinite(first) && Number.isFinite(last)) {
                if (last > first) { line = 'rgba(31,138,76,0.95)'; fill = 'rgba(31,138,76,0.10)'; }
                else if (last < first) { line = 'rgba(200,64,100,0.95)'; fill = 'rgba(200,64,100,0.10)'; }
            }
        }

        new Chart(ctx.getContext('2d'), {
            type: 'line',
            data: {
                labels: values.map((_, i) => i),
                datasets: [{
                    data: values,
                    borderColor: line,
                    borderWidth: 1.5,
                    fill: true,
                    backgroundColor: fill,
                    pointRadius: 0,
                    tension: 0.35,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false }, tooltip: { enabled: false } },
                scales: { x: { display: false }, y: { display: false } },
            },
        });
    };

    /** Apply threshold colors to a stat value element. */
    window.biApplyStatThresholds = function (widgetId, thresholds) {
        const el = document.getElementById('stat-val-' + widgetId);
        if (!el || !thresholds) return;
        const raw = parseFloat(el.dataset.rawValue);
        if (isNaN(raw)) { el.style.color = 'var(--fg)'; return; }
        const warning = thresholds.warning != null ? parseFloat(thresholds.warning) : null;
        const critical = thresholds.critical != null ? parseFloat(thresholds.critical) : null;
        if (critical != null && raw > critical) el.style.color = 'var(--red)';
        else if (warning != null && raw > warning) el.style.color = 'var(--accent-amber)';
        else el.style.color = 'var(--green)';
    };

    /** Apply conditional formatting rules to a table widget. */
    window.biApplyTableRules = function (widgetId, rules) {
        if (!rules || !rules.length) return;
        const opFuncs = {
            '==': (a, b) => String(a) === String(b),
            '!=': (a, b) => String(a) !== String(b),
            '>': (a, b) => parseFloat(a) > parseFloat(b),
            '<': (a, b) => parseFloat(a) < parseFloat(b),
            '>=': (a, b) => parseFloat(a) >= parseFloat(b),
            '<=': (a, b) => parseFloat(a) <= parseFloat(b),
            'contains': (a, b) => String(a).toLowerCase().includes(String(b).toLowerCase()),
        };
        const widget = document.getElementById('widget-' + widgetId);
        if (!widget) return;
        widget.querySelectorAll('td[data-col]').forEach(td => {
            const col = td.getAttribute('data-col');
            const val = td.getAttribute('data-val');
            for (const rule of rules) {
                if (rule.column !== col) continue;
                const fn = opFuncs[rule.operator];
                if (fn && fn(val, rule.value)) {
                    (rule.color || '').split(' ').filter(Boolean).forEach(c => td.classList.add(c));
                }
            }
        });
    };

    /** Render a map widget. Supports three styles, picked from widget
     *  config.map_style or auto-detected from columns:
     *    - 'points'    (default): same-size circles with brand color
     *    - 'bubbles'  (v0.3.0): circles sized by value + inline labels
     *    - 'choropleth' (future): region polygons colored by value
     *  SQL columns expected: (lat|latitude), (lng|lon|longitude),
     *  optionally (value|count|total|amount) and (label|name|city|region).
     */
    window.biRenderMap = function (widgetId, raw, config) {
        const mapEl = document.getElementById('map-' + widgetId);
        if (!mapEl || typeof maplibregl === 'undefined') return;

        const cfg = config || {};
        const style = (cfg.map_style || 'points').toLowerCase();

        const cols = raw.columns || [];
        const rows = raw.rows || [];

        const latIdx = cols.findIndex(c => /^(lat|latitude)$/i.test(c));
        const lngIdx = cols.findIndex(c => /^(lng|lon|longitude)$/i.test(c));
        const valIdx = cols.findIndex(c => /^(value|count|total|amount)$/i.test(c));
        const labelIdx = cols.findIndex(c => /^(name|label|title|city|region)$/i.test(c));

        const map = new maplibregl.Map({
            container: mapEl,
            style: {
                version: 8,
                glyphs: 'https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf',
                sources: {
                    'carto-dark': {
                        type: 'raster',
                        tiles: ['https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png'],
                        tileSize: 256,
                    }
                },
                layers: [{ id: 'carto-dark-layer', type: 'raster', source: 'carto-dark' }]
            },
            center: [0, 20],
            zoom: 1.5,
            attributionControl: false,
        });

        map.on('load', () => {
            if (latIdx < 0 || lngIdx < 0) return;

            const features = rows
                .filter(r => r[latIdx] != null && r[lngIdx] != null)
                .map(r => ({
                    type: 'Feature',
                    geometry: { type: 'Point', coordinates: [parseFloat(r[lngIdx]), parseFloat(r[latIdx])] },
                    properties: {
                        value: valIdx >= 0 ? (parseFloat(r[valIdx]) || 0) : 1,
                        label: labelIdx >= 0 ? String(r[labelIdx] || '') : '',
                    },
                }));

            map.addSource('points', { type: 'geojson', data: { type: 'FeatureCollection', features } });

            if (style === 'bubbles') {
                // Compute max value so the radius scale matches the data.
                const maxV = features.reduce((m, f) => Math.max(m, f.properties.value || 0), 1);
                // Brand orange bubbles with a translucent halo, like the
                // "Santo Domingo" panel in the mockup.
                map.addLayer({
                    id: 'bubble-halo',
                    type: 'circle',
                    source: 'points',
                    paint: {
                        'circle-radius': ['interpolate', ['linear'], ['get', 'value'], 0, 6, maxV, 28],
                        'circle-color': 'rgba(212, 80, 43, 0.18)',
                    },
                });
                map.addLayer({
                    id: 'bubble-core',
                    type: 'circle',
                    source: 'points',
                    paint: {
                        'circle-radius': ['interpolate', ['linear'], ['get', 'value'], 0, 3, maxV, 9],
                        'circle-color': 'rgba(212, 80, 43, 0.95)',
                        'circle-stroke-color': 'rgba(255, 255, 255, 0.6)',
                        'circle-stroke-width': 1,
                    },
                });
                // Inline labels next to the bubble. Skipped if no glyphs
                // server is reachable (some offline deployments may strip
                // it); the fallback is hover popups below.
                try {
                    map.addLayer({
                        id: 'bubble-label',
                        type: 'symbol',
                        source: 'points',
                        layout: {
                            'text-field': ['concat', ['get', 'label'], ['case', ['>', ['get', 'value'], 0], ['concat', ' · ', ['to-string', ['get', 'value']]], '']],
                            'text-size': 11,
                            'text-offset': [1.2, 0],
                            'text-anchor': 'left',
                            'text-allow-overlap': false,
                            'text-ignore-placement': false,
                        },
                        paint: {
                            'text-color': 'rgba(240, 236, 226, 0.95)',
                            'text-halo-color': 'rgba(14, 13, 10, 0.85)',
                            'text-halo-width': 1.2,
                        },
                    });
                } catch (e) {
                    console.warn('bi map: label layer not added (glyphs unavailable)', e);
                }
            } else {
                map.addLayer({
                    id: 'points-layer',
                    type: 'circle',
                    source: 'points',
                    paint: {
                        'circle-radius': ['interpolate', ['linear'], ['get', 'value'], 0, 4, 100, 20],
                        'circle-color': 'rgba(59, 130, 246, 0.7)',
                        'circle-stroke-color': 'rgba(59, 130, 246, 1)',
                        'circle-stroke-width': 1,
                    },
                });
            }

            // Hover popup carries the label/value reliably whether or
            // not the symbol layer rendered.
            map.on('mouseenter', style === 'bubbles' ? 'bubble-core' : 'points-layer', (e) => {
                if (!e.features || !e.features.length) return;
                const f = e.features[0];
                const lbl = f.properties.label || '';
                const v = f.properties.value;
                new maplibregl.Popup({ closeButton: false, offset: 10 })
                    .setLngLat(f.geometry.coordinates)
                    .setHTML(`<div style="font:11px/1.4 system-ui;color:#111"><b>${lbl}</b>${v ? '<br>' + v : ''}</div>`)
                    .addTo(map);
            });

            if (features.length > 0) {
                const bounds = new maplibregl.LngLatBounds();
                features.forEach(f => bounds.extend(f.geometry.coordinates));
                map.fitBounds(bounds, { padding: 40, maxZoom: 12 });
            }
        });
    };

    /** Render a markdown text widget into the given container. */
    window.biRenderMarkdown = function (widgetId) {
        const container = document.querySelector('#text-content-' + widgetId + ' .bi-markdown-content');
        if (!container) return;
        let text = container.textContent || '';
        text = text.replace(/^### (.+)$/gm, '<h3 style="font-size:1rem;font-weight:600;color:var(--fg);margin:0.75rem 0 0.25rem;">$1</h3>');
        text = text.replace(/^## (.+)$/gm, '<h2 style="font-size:1.125rem;font-weight:600;color:var(--fg);margin:1rem 0 0.25rem;">$1</h2>');
        text = text.replace(/^# (.+)$/gm, '<h1 style="font-size:1.25rem;font-weight:700;color:var(--fg);margin:1rem 0 0.5rem;">$1</h1>');
        text = text.replace(/\*\*(.+?)\*\*/g, '<strong style="color:var(--fg);">$1</strong>');
        text = text.replace(/\*(.+?)\*/g, '<em>$1</em>');
        text = text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a href="$2" style="color:var(--brand);" target="_blank" rel="noopener">$1</a>');
        text = text.replace(/\n/g, '<br>');
        container.innerHTML = text;
    };
})();
