/**
 * Profile page — personal API tokens (Alpine component).
 *
 * The plaintext token only ever exists in `created.token` for the life of
 * the modal; the server stores a hash and never returns it again.
 */

function apiTokens() {
    let initial = [];
    try {
        initial = JSON.parse(document.getElementById('api-tokens-data').textContent || '[]');
    } catch (e) { /* no data tag (single-user mode) → empty list */ }
    return {
        tokens: initial,
        showCreate: false,
        newName: '',
        newExpires: '',
        created: null,

        openCreate() {
            this.newName = '';
            this.newExpires = '';
            this.created = null;
            this.showCreate = true;
            this.$nextTick(() => window.lucide && window.lucide.createIcons());
        },

        async create() {
            if (!this.newName.trim()) { tuskToast('Give the token a name', 'warning'); return; }
            const data = await tuskFetchJSON('/api/profile/tokens', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ name: this.newName.trim(), expires_days: this.newExpires || null }),
            });
            if (data.error) { tuskToast(data.error, 'error'); return; }
            this.created = data;
            const list = await tuskFetchJSON('/api/profile/tokens');
            this.tokens = list.tokens || [];
            this.$nextTick(() => window.lucide && window.lucide.createIcons());
        },

        async copy() {
            try {
                await navigator.clipboard.writeText(this.created.token);
                tuskToast('Token copied', 'success');
            } catch (e) {
                tuskToast('Copy failed — select the text and copy it manually', 'warning');
            }
        },

        async revoke(t) {
            if (!await tuskConfirm(`Revoke token "${t.name}"? Clients using it will stop working immediately.`)) return;
            const data = await tuskFetchJSON('/api/profile/tokens/' + t.id, { method: 'DELETE' });
            if (data.error) { tuskToast(data.error, 'error'); return; }
            this.tokens = this.tokens.filter(x => x.id !== t.id);
            tuskToast('Token revoked', 'success');
        },
    };
}
