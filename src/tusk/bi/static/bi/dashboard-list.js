/**
 * Tusk BI — Dashboards list page Alpine component.
 */

function dashboardList() {
    return {
        showCreate: false,
        newName: '',
        newDesc: '',

        async createDashboard() {
            if (!this.newName.trim()) return;
            const data = await tuskFetchJSON('/api/bi/dashboards', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: this.newName, description: this.newDesc }),
            });
            if (data.error) { tuskToast(data.error, 'error'); return; }
            if (data.id) window.location.href = '/bi/dashboards/' + data.id + '/edit';
        },

        async deleteDashboard(id, name) {
            if (!await tuskConfirm(`Delete dashboard "${name}"? Its widgets go with it; saved queries stay.`)) return;
            const data = await tuskFetchJSON('/api/bi/dashboards/' + id, { method: 'DELETE' });
            if (data.error) { tuskToast(data.error, 'error'); return; }
            tuskToast('Dashboard deleted', 'success');
            window.location.reload();
        },
    };
}
