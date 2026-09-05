/**
 * Tusk BI — Chart Builder Alpine.js Component
 *
 * Interactive chart configuration UI for the query editor.
 * Allows selecting chart type, axes, and options with live preview.
 */

function chartBuilder() {
    return {
        chartType: 'bar',
        xColumn: '',
        yColumn: '',
        groupColumn: '',
        stacked: false,
        showLegend: true,
        columns: [],
        rows: [],
        _chart: null,

        chartTypes: [
            { value: 'bar', icon: 'bar-chart-3', label: 'Bar' },
            { value: 'line', icon: 'trending-up', label: 'Line' },
            { value: 'area', icon: 'area-chart', label: 'Area' },
            { value: 'pie', icon: 'pie-chart', label: 'Pie' },
            { value: 'doughnut', icon: 'circle-dot', label: 'Donut' },
            { value: 'horizontal_bar', icon: 'bar-chart', label: 'H-Bar' },
            { value: 'scatter', icon: 'scatter-chart', label: 'Scatter' },
            { value: 'radar', icon: 'radar', label: 'Radar' },
        ],

        init() {
            this.$watch('chartType', () => this.render());
            this.$watch('xColumn', () => this.render());
            this.$watch('yColumn', () => this.render());
            this.$watch('groupColumn', () => this.render());
            this.$watch('stacked', () => this.render());
            this.$watch('showLegend', () => this.render());
        },

        setData(columns, rows) {
            this.columns = columns || [];
            this.rows = rows || [];
            // Auto-detect sensible axes the moment data arrives, same
            // zero-config feel as Explore. Ask the server (single source
            // of truth — same suggest_axes() the renderer uses) so the
            // dropdowns pre-fill with a real dimension + measure instead
            // of columns[0]/columns[1]. Only fills empty selections so
            // we never stomp a choice the user already made.
            if (this.columns.length && this.rows.length && (!this.xColumn || !this.yColumn)) {
                this._autoDetectAxes();
            } else {
                if (this.columns.length > 0 && !this.xColumn) this.xColumn = this.columns[0];
                if (this.columns.length > 1 && !this.yColumn) this.yColumn = this.columns[1];
                this.render();
            }
        },

        async _autoDetectAxes() {
            try {
                const res = await fetch('/api/bi/suggest-chart', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        columns: this.columns,
                        rows: this.rows.slice(0, 200),
                    }),
                });
                const s = await res.json();
                if (!s || s.error) throw new Error(s && s.error);
                if (!this.xColumn && s.x_column) this.xColumn = s.x_column;
                if (!this.yColumn && s.y_column) this.yColumn = s.y_column;
                if (!this.groupColumn && s.group_by) this.groupColumn = s.group_by;
                // Only adopt the suggested chart type if the user is still
                // on the default 'bar' — don't override an explicit pick.
                if (s.chart_type && this.chartType === 'bar' && s.chart_type !== 'stat') {
                    this.chartType = s.chart_type;
                }
            } catch (_) {
                // Network/parse failure → fall back to naive first-cols.
                if (this.columns.length > 0 && !this.xColumn) this.xColumn = this.columns[0];
                if (this.columns.length > 1 && !this.yColumn) this.yColumn = this.columns[1];
            }
            this.render();
        },

        render() {
            if (!this.columns.length || !this.rows.length || !this.xColumn || !this.yColumn) return;

            const canvas = document.getElementById('chart-builder-canvas');
            if (!canvas) return;

            if (this._chart) this._chart.destroy();

            const xIdx = this.columns.indexOf(this.xColumn);
            const yIdx = this.columns.indexOf(this.yColumn);
            if (xIdx < 0 || yIdx < 0) return;

            const colors = window.biColors || ['rgba(59,130,246,0.8)'];
            const borderColors = window.biBorderColors || ['rgba(59,130,246,1)'];

            let config;
            const groupIdx = this.groupColumn ? this.columns.indexOf(this.groupColumn) : -1;

            if (groupIdx >= 0 && this.chartType !== 'pie' && this.chartType !== 'doughnut') {
                config = this._buildGroupedConfig(xIdx, yIdx, groupIdx, colors, borderColors);
            } else if (this.chartType === 'pie' || this.chartType === 'doughnut') {
                config = this._buildPieConfig(xIdx, yIdx, colors);
            } else if (this.chartType === 'scatter') {
                config = this._buildScatterConfig(xIdx, yIdx, colors[0]);
            } else {
                config = this._buildSimpleConfig(xIdx, yIdx, colors[0], borderColors[0]);
            }

            this._chart = new Chart(canvas.getContext('2d'), config);
            this.$nextTick(() => { if (typeof lucide !== 'undefined') lucide.createIcons(); });
        },

        _buildSimpleConfig(xIdx, yIdx, color, borderColor) {
            const labels = this.rows.map(r => r[xIdx]);
            const values = this.rows.map(r => {
                const v = r[yIdx];
                return typeof v === 'number' ? v : parseFloat(v) || 0;
            });

            const chartType = this.chartType === 'horizontal_bar' ? 'bar' : (this.chartType === 'area' ? 'line' : this.chartType);

            return {
                type: chartType,
                data: {
                    labels,
                    datasets: [{
                        label: this.yColumn,
                        data: values,
                        backgroundColor: color,
                        borderColor: borderColor,
                        borderWidth: 1,
                        fill: this.chartType === 'area',
                    }],
                },
                options: this._getOptions(),
            };
        },

        _buildGroupedConfig(xIdx, yIdx, groupIdx, colors, borderColors) {
            const groups = {};
            const labelSet = new Set();

            this.rows.forEach(r => {
                const label = r[xIdx];
                const group = r[groupIdx] || 'Other';
                const value = typeof r[yIdx] === 'number' ? r[yIdx] : parseFloat(r[yIdx]) || 0;
                labelSet.add(label);
                if (!groups[group]) groups[group] = {};
                groups[group][label] = (groups[group][label] || 0) + value;
            });

            const labels = Array.from(labelSet);
            const groupNames = Object.keys(groups);
            const chartType = this.chartType === 'horizontal_bar' ? 'bar' : (this.chartType === 'area' ? 'line' : this.chartType);

            return {
                type: chartType,
                data: {
                    labels,
                    datasets: groupNames.map((g, i) => ({
                        label: g,
                        data: labels.map(l => groups[g][l] || 0),
                        backgroundColor: colors[i % colors.length],
                        borderColor: borderColors[i % borderColors.length],
                        borderWidth: 1,
                        fill: this.chartType === 'area',
                    })),
                },
                options: this._getOptions(),
            };
        },

        _buildPieConfig(xIdx, yIdx, colors) {
            const labels = this.rows.map(r => r[xIdx]);
            const values = this.rows.map(r => {
                const v = r[yIdx];
                return typeof v === 'number' ? v : parseFloat(v) || 0;
            });

            return {
                type: this.chartType,
                data: {
                    labels,
                    datasets: [{
                        data: values,
                        backgroundColor: labels.map((_, i) => colors[i % colors.length]),
                    }],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: this.showLegend, position: 'right', labels: { color: '#e5e7eb' } } },
                },
            };
        },

        _buildScatterConfig(xIdx, yIdx, color) {
            const data = this.rows.map(r => ({
                x: typeof r[xIdx] === 'number' ? r[xIdx] : parseFloat(r[xIdx]) || 0,
                y: typeof r[yIdx] === 'number' ? r[yIdx] : parseFloat(r[yIdx]) || 0,
            }));

            return {
                type: 'scatter',
                data: { datasets: [{ label: `${this.xColumn} vs ${this.yColumn}`, data, backgroundColor: color }] },
                options: this._getOptions(),
            };
        },

        _getOptions() {
            const isPolar = this.chartType === 'pie' || this.chartType === 'doughnut' || this.chartType === 'radar';
            return {
                responsive: true,
                maintainAspectRatio: false,
                indexAxis: this.chartType === 'horizontal_bar' ? 'y' : 'x',
                plugins: {
                    legend: { display: this.showLegend, labels: { color: '#e5e7eb' } },
                },
                scales: isPolar ? {} : {
                    x: {
                        stacked: this.stacked,
                        ticks: { color: '#9ca3af', font: { size: 10 } },
                        grid: { color: 'rgba(255,255,255,0.05)' },
                    },
                    y: {
                        stacked: this.stacked,
                        ticks: { color: '#9ca3af', font: { size: 10 } },
                        grid: { color: 'rgba(255,255,255,0.05)' },
                    },
                },
            };
        },

        async exportPNG() {
            const canvas = document.getElementById('chart-builder-canvas');
            if (!canvas) return;
            const url = canvas.toDataURL('image/png');
            const a = document.createElement('a');
            a.href = url;
            a.download = 'chart.png';
            a.click();
        },
    };
}
