/**
 * Tusk BI — Visual Query Builder Alpine component.
 */

function queryBuilder() {
    return {
        sourceId: '',
        table: '',
        tables: [],
        schema: [],
        selectedColumns: [],
        aggregates: [],
        filters: [],
        groupBy: [],
        limit: 1000,
        generatedSQL: '',

        async loadTables() {
            if (!this.sourceId) { this.tables = []; return; }
            const data = await tuskFetchJSON(`/api/bi/sources/${this.sourceId}/tables`);
            if (data.error) { tuskToast(data.error, 'error'); return; }
            this.tables = data.tables || [];
            this.table = '';
            this.schema = [];
            this.selectedColumns = [];
        },

        async loadSchema() {
            if (!this.sourceId || !this.table) { this.schema = []; return; }
            const data = await tuskFetchJSON(`/api/bi/sources/${this.sourceId}/schema/${this.table}`);
            if (data.error) { tuskToast(data.error, 'error'); return; }
            this.schema = data.columns || [];
            this.selectedColumns = [];
            this.$nextTick(() => { if (window.lucide) lucide.createIcons(); });
        },

        async generateSQL() {
            if (!this.table) { tuskToast('Select a table first', 'warning'); return; }
            const data = await tuskFetchJSON('/api/bi/query-builder/generate-sql', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    source_id: parseInt(this.sourceId),
                    table: this.table,
                    columns: this.selectedColumns,
                    aggregates: this.aggregates,
                    filters: this.filters,
                    group_by: this.groupBy,
                    limit: this.limit,
                }),
            });
            if (data.sql) {
                this.generatedSQL = data.sql;
                tuskToast('SQL generated', 'success');
            } else if (data.error) {
                tuskToast(data.error, 'error');
            }
        },

        openInEditor() {
            const params = new URLSearchParams({ sql: this.generatedSQL, source_id: this.sourceId });
            window.location.href = `/bi/queries/new?${params}`;
        },
    };
}
