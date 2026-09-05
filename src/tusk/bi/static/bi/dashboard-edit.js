/**
 * Tusk BI — Dashboard Editor Alpine component.
 * Three-panel editor (widget library / canvas / config) backed by GridStack.
 */

function dashboardEditor(initial) {
    initial = initial || {};
    return {
        dashboardId: '',
        dashName: initial.dashName || '',
        grid: null,
        dirty: false,
        saving: false,
        justSaved: false,
        zoomLevel: 100,
        showGrid: true,
        widgetCount: initial.widgetCount || 0,
        firstQueryId: initial.firstQueryId || null,
        firstQueryName: initial.firstQueryName || '',

        // Panels
        leftTab: 'widgets',
        rightTab: 'data',
        showRightPanel: false,
        showSettings: false,
        settingsTab: 'tabs',
        editingDesc: false,
        showAddVar: false,

        // Right panel — selected widget
        selectedWidgetId: null,
        selectedWidgetTitle: 'Widget Config',
        selectedConfig: { query_id: '', widget_type: 'chart', tab_id: '', color: 'var(--green)' },

        // Widget add
        newWidgetType: 'chart',
        newWidgetTab: '',

        // Settings
        newTabName: '',
        newVar: { name: '', var_type: 'text', default_value: '', options: '' },
        newSchedule: { query_id: '', cron_expr: '', max_snapshots: 100 },
        embedTokens: [],
        newEmbed: { app_id: '', expires_in: '86400', rls_json: '' },
        lastEmbedUrl: '',
        // v0.3.0 — dashboard-level settings persisted via PUT /api/bi/dashboards/{id}
        dashboardSettings: {
            is_public: Boolean(initial.isPublic),
            refresh_interval_seconds: Number(initial.refreshIntervalSeconds || 0),
        },

        // Inline query
        showSaveInline: false,
        inlineQuery: {
            source_id: '', sql: '', name: '', tables: [],
            running: false, error: '', hasResult: false,
            rowCount: 0, colCount: 0, columns: [], preview: [],
        },

        init() {
            this.dashboardId = this.$el.dataset.dashboardId;
            this.grid = GridStack.init({
                column: 12,
                cellHeight: 60,
                animate: true,
                disableOneColumnMode: true,
                float: true,
            }, '#edit-grid');

            this.grid.on('change', () => { this.dirty = true; });

            document.addEventListener('keydown', (e) => {
                if ((e.metaKey || e.ctrlKey) && e.key === 's') {
                    e.preventDefault();
                    this.saveLayout();
                }
                if (e.key === 'Delete' || e.key === 'Backspace') {
                    const active = document.activeElement;
                    if (active && (active.tagName === 'INPUT' || active.tagName === 'TEXTAREA')) return;
                    if (this.selectedWidgetId) this.removeWidget(this.selectedWidgetId);
                }
                if (e.key === 'Escape') {
                    this.deselectAll();
                    this.showSettings = false;
                }
            });
        },

        applyZoom() {
            const canvas = document.getElementById('edit-grid');
            if (canvas) {
                canvas.style.transform = 'scale(' + (this.zoomLevel / 100) + ')';
                canvas.style.transformOrigin = 'top left';
            }
        },

        selectWidget(e, widgetId, widgetType) {
            e.stopPropagation();
            document.querySelectorAll('.ed-widget').forEach(el => el.classList.remove('selected'));
            const el = e.currentTarget;
            if (el) el.classList.add('selected');
            this.selectedWidgetId = widgetId;
            this.selectedWidgetTitle = widgetType.charAt(0).toUpperCase() + widgetType.slice(1) + ' Widget';
            this.selectedConfig.widget_type = widgetType;
            this.showRightPanel = true;
        },

        deselectAll() {
            document.querySelectorAll('.ed-widget').forEach(el => el.classList.remove('selected'));
            this.selectedWidgetId = null;
        },

        async renameDashboard() {
            if (!this.dashName.trim()) return;
            const data = await tuskFetchJSON('/api/bi/dashboards/' + this.dashboardId, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: this.dashName }),
            });
            if (data.error) { tuskToast(data.error, 'error'); return; }
            tuskToast('Dashboard renamed', 'success');
        },

        async saveLayout() {
            this.saving = true;
            const items = this.grid.getGridItems();
            const widgets = items.map(el => {
                const node = el.gridstackNode;
                return {
                    id: parseInt(node.id || el.getAttribute('gs-id')),
                    col_start: (node.x || 0) + 1,
                    col_span: node.w || 6,
                    row_start: (node.y || 0) + 1,
                    row_span: node.h || 4,
                };
            });
            const data = await tuskFetchJSON('/api/bi/dashboards/' + this.dashboardId + '/layout', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ widgets }),
            });
            this.saving = false;
            if (data.error) {
                tuskToast('Failed to save layout', 'error');
                return;
            }
            this.dirty = false;
            this.justSaved = true;
            tuskToast('Layout saved', 'success');
            setTimeout(() => { this.justSaved = false; }, 2000);
        },

        addWidgetByType(type) {
            this.newWidgetType = type;
            if (this.firstQueryId) {
                this.addWidget(this.firstQueryId, this.firstQueryName);
            } else {
                tuskToast('Create a saved query first', 'warning');
                this.leftTab = 'queries';
            }
        },

        async addWidget(queryId, title) {
            const wt = this.newWidgetType || 'chart';
            const payload = {
                query_id: queryId, widget_type: wt, title: title,
                col_start: 1, col_span: wt === 'stat' ? 3 : 6,
                row_start: 1, row_span: wt === 'stat' ? 2 : 4,
            };
            if (this.newWidgetTab) {
                payload.tab_id = parseInt(this.newWidgetTab);
            }
            const data = await tuskFetchJSON('/api/bi/dashboards/' + this.dashboardId + '/widgets', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            if (!data.error) window.location.reload();
            else tuskToast(data.error, 'error');
        },

        async removeWidget(widgetId) {
            if (!await tuskConfirm('Remove this widget?')) return;
            const data = await tuskFetchJSON('/api/bi/widgets/' + widgetId, { method: 'DELETE' });
            if (!data.error) window.location.reload();
            else tuskToast(data.error, 'error');
        },

        async addVariable() {
            if (!this.newVar.name) return;
            const data = await tuskFetchJSON('/api/bi/dashboards/' + this.dashboardId + '/variables', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(this.newVar),
            });
            if (!data.error) { tuskToast('Variable added', 'success'); window.location.reload(); }
            else tuskToast(data.error, 'error');
        },

        async deleteVariable(varId, varName) {
            if (!await tuskConfirm('Delete variable $' + varName + '?')) return;
            const data = await tuskFetchJSON('/api/bi/variables/' + varId, { method: 'DELETE' });
            if (!data.error) window.location.reload();
            else tuskToast(data.error, 'error');
        },

        async addTab() {
            if (!this.newTabName.trim()) return;
            const data = await tuskFetchJSON('/api/bi/dashboards/' + this.dashboardId + '/tabs', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: this.newTabName.trim() }),
            });
            if (!data.error) { tuskToast('Tab added', 'success'); window.location.reload(); }
            else tuskToast(data.error, 'error');
        },

        async deleteTab(tabId) {
            if (!await tuskConfirm('Delete this tab?')) return;
            const data = await tuskFetchJSON('/api/bi/tabs/' + tabId, { method: 'DELETE' });
            if (!data.error) window.location.reload();
            else tuskToast(data.error, 'error');
        },

        async addSchedule() {
            if (!this.newSchedule.query_id || !this.newSchedule.cron_expr.trim()) {
                tuskToast('Query and cron expression are required', 'warning'); return;
            }
            const data = await tuskFetchJSON('/api/bi/queries/' + this.newSchedule.query_id + '/schedule', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    cron_expr: this.newSchedule.cron_expr.trim(),
                    max_snapshots: parseInt(this.newSchedule.max_snapshots) || 100,
                }),
            });
            if (!data.error) { tuskToast('Schedule created', 'success'); window.location.reload(); }
            else tuskToast(data.error || 'Failed', 'error');
        },

        async toggleSchedule(scheduleId, enabled) {
            const data = await tuskFetchJSON('/api/bi/schedules/' + scheduleId + '/toggle', {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled }),
            });
            if (!data.error) window.location.reload();
            else tuskToast(data.error, 'error');
        },

        async deleteSchedule(scheduleId) {
            if (!await tuskConfirm('Delete this schedule?')) return;
            const data = await tuskFetchJSON('/api/bi/schedules/' + scheduleId, { method: 'DELETE' });
            if (!data.error) window.location.reload();
            else tuskToast(data.error, 'error');
        },

        async saveDashboardSettings() {
            // Persist is_public + refresh_interval_seconds. Called on
            // every toggle/select change in the General tab — no save
            // button so the user gets instant feedback.
            const payload = {
                is_public: Boolean(this.dashboardSettings.is_public),
                refresh_interval_seconds: Number(this.dashboardSettings.refresh_interval_seconds || 0),
            };
            const data = await tuskFetchJSON('/api/bi/dashboards/' + this.dashboardId, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
            });
            if (data && data.error) { tuskToast(data.error, 'error'); return; }
            tuskToast('Settings saved', 'success');
        },

        async createPublicLink() {
            const data = await tuskFetchJSON('/api/bi/dashboards/' + this.dashboardId + '/public-link', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
            });
            if (data.error) { tuskToast(data.error, 'error'); return; }
            const url = window.location.origin + '/bi/public/' + data.token;
            await navigator.clipboard.writeText(url);
            tuskToast('Public link copied!', 'success');
        },

        async loadEmbedTokens() {
            const data = await tuskFetchJSON('/api/bi/embed-tokens/' + this.dashboardId);
            if (data.error) return;
            this.embedTokens = data.tokens || [];
        },

        async createEmbedToken() {
            let rls = {};
            if (this.newEmbed.rls_json.trim()) {
                try { rls = JSON.parse(this.newEmbed.rls_json); }
                catch { tuskToast('Invalid JSON in RLS', 'error'); return; }
            }
            const data = await tuskFetchJSON('/api/bi/embed-tokens/' + this.dashboardId, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    app_id: this.newEmbed.app_id,
                    expires_in_seconds: parseInt(this.newEmbed.expires_in),
                    rls_clauses: rls,
                }),
            });
            if (data.error) {
                tuskToast(data.error || 'Failed', 'error');
                return;
            }
            this.lastEmbedUrl = data.embed_url;
            tuskToast('Embed token created', 'success');
            this.loadEmbedTokens();
        },

        async revokeEmbedToken(tokenId) {
            if (!await tuskConfirm('Revoke this embed token?')) return;
            const data = await tuskFetchJSON('/api/bi/embed-tokens/' + tokenId, { method: 'DELETE' });
            if (!data.error) { tuskToast('Token revoked', 'success'); this.loadEmbedTokens(); }
            else tuskToast(data.error, 'error');
        },

        copyEmbedUrl(et) {
            const url = window.location.origin + '/embed/dashboard/' + this.dashboardId + '?token=' + et.token;
            navigator.clipboard.writeText(url);
            tuskToast('Embed URL copied', 'success');
        },

        copyIframeCode(et) {
            const url = window.location.origin + '/embed/dashboard/' + this.dashboardId + '?token=' + et.token;
            navigator.clipboard.writeText('<iframe src="' + url + '" width="100%" height="600" frameborder="0" allow="fullscreen"></iframe>');
            tuskToast('iframe code copied', 'success');
        },

        copyLastIframe() {
            if (!this.lastEmbedUrl) return;
            navigator.clipboard.writeText('<iframe src="' + this.lastEmbedUrl + '" width="100%" height="600" frameborder="0" allow="fullscreen"></iframe>');
            tuskToast('iframe code copied', 'success');
        },

        async loadSourceTables() {
            this.inlineQuery.tables = [];
            if (!this.inlineQuery.source_id) return;
            const data = await tuskFetchJSON('/api/bi/sources/' + this.inlineQuery.source_id + '/tables');
            if (data.error) return;
            this.inlineQuery.tables = (data.tables || []).map(t => typeof t === 'string' ? t : t.name || t);
        },

        insertTableName(table) {
            if (!table) return;
            const ta = this.$refs.inlineSql;
            const sql = this.inlineQuery.sql.trim();
            if (!sql) {
                this.inlineQuery.sql = 'SELECT * FROM ' + table + ' LIMIT 100';
                this.$nextTick(() => { ta.focus(); ta.setSelectionRange(0, 0); });
                tuskToast('Query generated for ' + table, 'info');
                return;
            }
            const rawSql = this.inlineQuery.sql;
            const pos = ta.selectionStart ?? rawSql.length;
            const before = rawSql.slice(0, pos);
            const after = rawSql.slice(ta.selectionEnd ?? pos);
            const needSpace = before.length > 0 && !before.endsWith(' ') && !before.endsWith('\n');
            this.inlineQuery.sql = before + (needSpace ? ' ' : '') + table + after;
            this.$nextTick(() => {
                const newPos = before.length + (needSpace ? 1 : 0) + table.length;
                ta.focus();
                ta.setSelectionRange(newPos, newPos);
            });
        },

        async runInlineQuery() {
            if (!this.inlineQuery.sql.trim() || !this.inlineQuery.source_id) return;
            this.inlineQuery.running = true;
            this.inlineQuery.error = '';
            this.inlineQuery.hasResult = false;
            const data = await tuskFetchJSON('/api/bi/queries/run', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ source_id: parseInt(this.inlineQuery.source_id), sql: this.inlineQuery.sql }),
            });
            if (data.error) {
                this.inlineQuery.error = data.error;
            } else {
                this.inlineQuery.hasResult = true;
                this.inlineQuery.columns = data.columns || [];
                this.inlineQuery.colCount = (data.columns || []).length;
                this.inlineQuery.rowCount = data.row_count || (data.rows || []).length;
                this.inlineQuery.preview = (data.rows || []).slice(0, 5);
            }
            this.inlineQuery.running = false;
        },

        async saveAndAddWidget() {
            if (!this.inlineQuery.name.trim()) return;
            const data = await tuskFetchJSON('/api/bi/queries', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    name: this.inlineQuery.name.trim(),
                    source_id: parseInt(this.inlineQuery.source_id),
                    sql: this.inlineQuery.sql,
                }),
            });
            if (data.error) { tuskToast('Failed to save query', 'error'); return; }
            const queryId = data.id || data.query_id;
            if (!queryId) { tuskToast('Query saved but no ID returned', 'error'); return; }
            tuskToast('Query saved', 'success');
            this.showSaveInline = false;
            await this.addWidget(queryId, this.inlineQuery.name.trim());
        },
    };
}
