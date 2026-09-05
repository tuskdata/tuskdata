/**
 * Tusk BI — SQL Query Editor.
 *
 * CodeMirror init is done inline in the template (it imports ESM modules
 * via type="module"); this file owns the Alpine component used by the
 * surrounding shell.
 */

function queryEditor(initial) {
    initial = initial || {};
    return {
        name: initial.name || '',
        sourceId: initial.sourceId || '',
        queryId: initial.queryId || null,
        resultTab: 'table',
        loading: false,
        error: '',
        columns: [],
        rows: [],
        truncated: false,
        chartType: initial.chartType || 'bar',
        xColumn: '',
        yColumn: '',
        _chart: null,

        getSQL() {
            return window._cmEditor ? window._cmEditor.state.doc.toString() : '';
        },

        async runQuery() {
            const sqlText = this.getSQL().trim();
            if (!sqlText || !this.sourceId) return;

            this.loading = true;
            this.error = '';
            this.columns = [];
            this.rows = [];

            const data = await tuskFetchJSON('/api/bi/queries/run', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ source_id: parseInt(this.sourceId), sql: sqlText }),
            });
            if (data.error) {
                this.error = data.error;
            } else {
                this.columns = data.columns || [];
                this.rows = data.rows || [];
                this.truncated = data.truncated || false;
                if (this.columns.length > 0) {
                    this.xColumn = this.xColumn || this.columns[0];
                    this.yColumn = this.yColumn || (this.columns[1] || this.columns[0]);
                }
                if (this.resultTab === 'chart') this.renderChart();
            }
            this.loading = false;
        },

        async saveQuery() {
            const sqlText = this.getSQL().trim();
            if (!this.name || !this.sourceId || !sqlText) {
                tuskToast('Name, source, and SQL are required', 'warning');
                return;
            }
            const body = {
                name: this.name,
                source_id: parseInt(this.sourceId),
                sql: sqlText,
                chart_type: this.chartType,
                chart_config: { x_column: this.xColumn, y_column: this.yColumn },
            };
            const url = this.queryId ? `/api/bi/queries/${this.queryId}` : '/api/bi/queries';
            const method = this.queryId ? 'PUT' : 'POST';
            const data = await tuskFetchJSON(url, {
                method,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body),
            });
            if (data.error) { tuskToast('Failed to save: ' + data.error, 'error'); return; }
            if (data.id) this.queryId = data.id;
            tuskToast('Query saved', 'success');
        },

        async renderChart() {
            if (!this.columns.length || !this.rows.length) return;
            if (this._chart) this._chart.destroy();

            const data = await tuskFetchJSON('/api/bi/queries/run', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ source_id: parseInt(this.sourceId), sql: this.getSQL() }),
            });
            if (data.error) return;

            const cs = getComputedStyle(document.documentElement);
            const brandColor = cs.getPropertyValue('--brand').trim() || '#d4502b';

            const labels = data.rows.map(r => r[data.columns.indexOf(this.xColumn)]);
            const values = data.rows.map(r => {
                const v = r[data.columns.indexOf(this.yColumn)];
                return typeof v === 'number' ? v : parseFloat(v) || 0;
            });

            const ctx = document.getElementById('result-chart').getContext('2d');
            this._chart = new Chart(ctx, {
                type: this.chartType === 'horizontal_bar' ? 'bar' : (this.chartType === 'area' ? 'line' : this.chartType),
                data: {
                    labels,
                    datasets: [{
                        label: this.yColumn,
                        data: values,
                        backgroundColor: brandColor,
                        borderColor: brandColor,
                        borderWidth: 1,
                        fill: this.chartType === 'area',
                    }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    indexAxis: this.chartType === 'horizontal_bar' ? 'y' : 'x',
                    scales: this.chartType === 'pie' || this.chartType === 'doughnut' ? {} : {
                        x: { ticks: { color: cs.getPropertyValue('--fg-3').trim() }, grid: { color: cs.getPropertyValue('--border').trim() } },
                        y: { ticks: { color: cs.getPropertyValue('--fg-3').trim() }, grid: { color: cs.getPropertyValue('--border').trim() } },
                    },
                },
            });
        },
    };
}
