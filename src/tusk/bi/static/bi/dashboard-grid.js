/**
 * Tusk BI — Dashboard Grid Manager (Legacy stub)
 *
 * The actual grid management is now handled by:
 * - dashboardEditGrid() in dashboard_edit.html (edit mode, uses Gridstack.js)
 * - dashboardView() in dashboard.html (view mode, uses Gridstack.js static)
 *
 * This file is kept for any pages that might still reference dashboardGrid().
 */

function dashboardGrid() {
    return {
        dashboardId: null,
        init() {
            this.dashboardId = this.$el.dataset.dashboardId;
            console.warn('Legacy dashboardGrid() — use dashboardEditGrid() instead');
        },
        saveLayout() {},
        addWidget() {},
        removeWidget() {},
        startDrag() {},
        startResize() {},
    };
}
