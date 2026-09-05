/**
 * Tusk BI — Embed dashboard Alpine component (used by embed_dashboard.html).
 */

function embedDashboard() {
    return {
        autoRefresh: '0',
        _refreshInterval: null,

        init() {
            lucide.createIcons();
            GridStack.init({ column: 12, cellHeight: 60, staticGrid: true, disableOneColumnMode: true }, '#embed-grid');

            const urlParams = new URLSearchParams(window.location.search);
            const vars = {};
            for (const [k, v] of urlParams) { if (k.startsWith('var_')) vars[k] = v; }
            if (Object.keys(vars).length > 0) {
                document.querySelectorAll('.bi-widget-body').forEach(el => {
                    const url = new URL(el.getAttribute('hx-get'), window.location.origin);
                    for (const [k, v] of Object.entries(vars)) url.searchParams.set(k, v);
                    el.setAttribute('hx-get', url.pathname + url.search);
                });
            }

            const refresh = urlParams.get('refresh');
            if (refresh && parseInt(refresh) > 0) { this.autoRefresh = refresh; this.setupAutoRefresh(); }
        },

        refreshAll() {
            document.querySelectorAll('.bi-widget-body').forEach(el => htmx.trigger(el, 'refresh'));
        },

        setupAutoRefresh() {
            if (this._refreshInterval) { clearInterval(this._refreshInterval); this._refreshInterval = null; }
            const s = parseInt(this.autoRefresh);
            if (s > 0) this._refreshInterval = setInterval(() => this.refreshAll(), s * 1000);
        },

        switchTab(btn, tabId) {
            btn.closest('div').querySelectorAll('.nav-tab').forEach(t => t.classList.remove('active'));
            btn.classList.add('active');
            document.querySelectorAll('.embed-tab-content').forEach(el => {
                el.style.display = tabId === 'all' || el.dataset.tabId == tabId ? '' : 'none';
            });
        },
    };
}
