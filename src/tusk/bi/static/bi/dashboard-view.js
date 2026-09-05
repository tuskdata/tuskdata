/**
 * Tusk BI — Dashboard view Alpine component.
 *
 * Initialized from dashboard.html with the dashboard ID and variable
 * defaults via window._biDashboardCtx (set inline by the template).
 */

function dashboardView() {
    const ctx = window._biDashboardCtx || { dashboardId: '', vars: {} };
    return {
        timeRange: '',
        autoRefresh: '0',
        _refreshInterval: null,
        showEmbedModal: false,
        embedForm: { app_id: '', expires_in: '86400', rls_json: '' },
        embedResult: { url: '', iframe: '' },
        vars: Object.assign({}, ctx.vars || {}),

        init() {
            // v0.3.0: the viewer no longer uses GridStack — widgets are
            // laid out via CSS grid (.dash-grid + .span-N). Editor still
            // uses GridStack for drag/resize. Auto-refresh from the
            // dashboard's saved refresh_interval_seconds (Live mode).
            this._setupDrillDown();

            const saved = parseInt(ctx.refreshIntervalSeconds || 0) || 0;
            if (saved > 0) {
                // Seed the manual dropdown so the user sees the active
                // interval, then start the timer.
                this.autoRefresh = String(saved);
                this.setupAutoRefresh();
            }

            this._startLastRefreshTicker();
        },

        _lastRefreshAt: Date.now(),

        _startLastRefreshTicker() {
            const el = document.getElementById('bi-last-refresh-meta');
            const elFooter = document.getElementById('bi-last-refresh');
            const fmt = () => {
                const sec = Math.max(0, Math.floor((Date.now() - this._lastRefreshAt) / 1000));
                if (sec < 5) return 'just now';
                if (sec < 60) return sec + 's ago';
                if (sec < 3600) return Math.floor(sec / 60) + 'm ago';
                return Math.floor(sec / 3600) + 'h ago';
            };
            setInterval(() => {
                const txt = fmt();
                if (el) el.textContent = txt;
                if (elFooter) elFooter.textContent = 'Last refresh: ' + txt;
            }, 5000);
        },

        _buildParams() {
            const params = {};
            for (const [k, v] of Object.entries(this.vars)) {
                if (v) params['var_' + k] = v;
            }
            if (this.timeRange) {
                const now = new Date();
                const ranges = { '1h': 3600e3, '6h': 21600e3, '24h': 86400e3, '7d': 604800e3, '30d': 2592000e3 };
                const from = new Date(now - (ranges[this.timeRange] || 0));
                params.time_from = from.toISOString();
                params.time_to = now.toISOString();
            }
            return params;
        },

        onTimeRangeChange() { this.refreshAll(); },

        refreshAll() {
            const params = this._buildParams();
            window._biWidgetParams = params;
            document.querySelectorAll('.bi-widget-body').forEach(el => {
                const url = new URL(el.getAttribute('hx-get') || '', window.location.origin);
                for (const [k, v] of Object.entries(params)) url.searchParams.set(k, v);
                el.setAttribute('hx-get', url.pathname + url.search);
                htmx.trigger(el, 'refresh');
            });
            this._lastRefreshAt = Date.now();
            const el = document.getElementById('bi-last-refresh-meta');
            if (el) el.textContent = 'just now';
            const elFooter = document.getElementById('bi-last-refresh');
            if (elFooter) elFooter.textContent = 'Last refresh: just now';
        },

        setupAutoRefresh() {
            if (this._refreshInterval) { clearInterval(this._refreshInterval); this._refreshInterval = null; }
            const seconds = parseInt(this.autoRefresh);
            if (seconds > 0) this._refreshInterval = setInterval(() => this.refreshAll(), seconds * 1000);
        },

        exportChartPng(widgetId) {
            const canvas = document.querySelector(`#widget-${widgetId} canvas`);
            if (!canvas) { tuskToast('No chart canvas found', 'error'); return; }
            const url = canvas.toDataURL('image/png');
            const link = document.createElement('a');
            link.download = `chart-${widgetId}.png`;
            link.href = url;
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            tuskToast('Chart exported', 'success');
        },

        toggleExpand(widgetId) {
            const el = document.getElementById('widget-' + widgetId);
            if (!el) return;
            el.classList.toggle('fixed');
            el.classList.toggle('inset-4');
            el.classList.toggle('z-50');
            if (el.classList.contains('fixed')) {
                el.style.height = '';
                el.style.background = 'var(--surface)';
                el.style.borderRadius = 'var(--r-lg)';
                el.style.boxShadow = 'var(--shadow-xl)';
            } else {
                el.style.boxShadow = '';
            }
        },

        async generateEmbed() {
            const dashboardId = (window._biDashboardCtx || {}).dashboardId;
            let rls = {};
            if (this.embedForm.rls_json.trim()) {
                try { rls = JSON.parse(this.embedForm.rls_json); }
                catch { tuskToast('Invalid JSON in RLS clauses', 'error'); return; }
            }
            const data = await tuskFetchJSON('/api/bi/embed-tokens/' + dashboardId, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    app_id: this.embedForm.app_id,
                    expires_in_seconds: parseInt(this.embedForm.expires_in),
                    rls_clauses: rls,
                }),
            });
            if (data.error) {
                tuskToast(data.error || 'Failed', 'error');
                return;
            }
            this.embedResult.url = data.embed_url;
            this.embedResult.iframe = `<iframe src="${data.embed_url}" width="100%" height="600" frameborder="0" allow="fullscreen"></iframe>`;
            tuskToast('Embed token generated', 'success');
        },

        _setupDrillDown() {
            document.body.addEventListener('htmx:afterSettle', (e) => {
                if (typeof updateLastRefresh === 'function') updateLastRefresh();
                const widgetBody = e.target.closest('.bi-widget-body');
                if (!widgetBody) return;
                const canvas = widgetBody.querySelector('canvas');
                if (!canvas) return;
                const chartInstance = Chart.getChart(canvas);
                if (!chartInstance) return;
                canvas.onclick = (evt) => {
                    const points = chartInstance.getElementsAtEventForMode(evt, 'nearest', { intersect: true }, true);
                    if (points.length === 0) return;
                    const idx = points[0].index;
                    const label = chartInstance.data.labels ? chartInstance.data.labels[idx] : null;
                    const ds = chartInstance.data.datasets[points[0].datasetIndex];
                    if (label) this.vars['_drill_label'] = String(label);
                    if (ds?.label) this.vars['_drill_series'] = String(ds.label);
                    this.refreshAll();
                    tuskToast(`Drill: ${label || 'value'}`, 'info');
                };
            });
        },
    };
}
