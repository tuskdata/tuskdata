/**
 * Tusk BI — Data Explorer Alpine component.
 * Browses sources/tables/schema/preview using core tuskFetch helpers.
 */

function dataExplorer() {
    return {
        sources: [],
        selectedSource: null,
        sourceTables: [],
        selectedTable: null,
        loadingTables: false,
        explorerTab: 'schema',
        tableSchema: [],
        loadingSchema: false,
        previewColumns: [],
        previewRows: [],
        loadingPreview: false,
        previewError: '',

        async init() {
            await this.refreshSources();
        },

        async refreshSources() {
            const data = await tuskFetchJSON('/api/bi/sources');
            if (data.error) { console.error('sources:', data.error); return; }
            this.sources = data.sources || [];
            this.$nextTick(() => { if (window.lucide) lucide.createIcons(); });
        },

        async discoverSources() {
            await tuskFetch('/api/bi/sources/discover', { method: 'POST' });
            await this.refreshSources();
            tuskToast('Sources discovered', 'success');
        },

        async toggleSource(source) {
            if (this.selectedSource?.id === source.id) {
                this.selectedSource = null;
                this.sourceTables = [];
                this.selectedTable = null;
                return;
            }
            this.selectedSource = source;
            this.selectedTable = null;
            this.sourceTables = [];
            this.loadingTables = true;
            const data = await tuskFetchJSON(`/api/bi/sources/${source.id}/tables`);
            if (!data.error) {
                this.sourceTables = data.tables || [];
            } else {
                console.error('tables:', data.error);
            }
            this.loadingTables = false;
        },

        async selectTable(source, table) {
            this.selectedTable = table;
            this.explorerTab = 'schema';
            this.tableSchema = [];
            this.previewColumns = [];
            this.previewRows = [];
            this.previewError = '';
            await this.loadSchema();
            this.$nextTick(() => { if (window.lucide) lucide.createIcons(); });
        },

        async loadSchema() {
            if (!this.selectedSource || !this.selectedTable) return;
            this.loadingSchema = true;
            const data = await tuskFetchJSON(`/api/bi/sources/${this.selectedSource.id}/schema/${this.selectedTable}`);
            if (!data.error) this.tableSchema = data.columns || [];
            this.loadingSchema = false;
        },

        async loadPreview() {
            if (!this.selectedSource || !this.selectedTable) return;
            if (this.previewColumns.length > 0) return;
            this.loadingPreview = true;
            this.previewError = '';
            const data = await tuskFetchJSON(`/api/bi/sources/${this.selectedSource.id}/preview/${this.selectedTable}`);
            if (data.error) {
                this.previewError = data.error;
            } else {
                this.previewColumns = data.columns || [];
                this.previewRows = data.rows || [];
            }
            this.loadingPreview = false;
        },
    };
}
