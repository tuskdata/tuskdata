/**
 * Tusk BI — Main JavaScript
 *
 * Chart.js theme defaults, widget refresh, HTMX integration.
 * Loaded in BI templates via /static/plugins/bi/bi.js
 */

// ─────────────────────────────────────────────────────────────
// Chart.js Dark Theme Defaults
// ─────────────────────────────────────────────────────────────

if (typeof Chart !== 'undefined') {
    Chart.defaults.color = '#9ca3af';
    Chart.defaults.borderColor = 'rgba(255, 255, 255, 0.06)';
    Chart.defaults.backgroundColor = 'rgba(59, 130, 246, 0.7)';

    Chart.defaults.plugins.legend.labels.color = '#e5e7eb';
    Chart.defaults.plugins.legend.labels.boxWidth = 12;
    Chart.defaults.plugins.legend.labels.font = { size: 11 };

    Chart.defaults.plugins.tooltip.backgroundColor = '#1f2937';
    Chart.defaults.plugins.tooltip.titleColor = '#f3f4f6';
    Chart.defaults.plugins.tooltip.bodyColor = '#d1d5db';
    Chart.defaults.plugins.tooltip.borderColor = '#374151';
    Chart.defaults.plugins.tooltip.borderWidth = 1;
}

// ─────────────────────────────────────────────────────────────
// BI Color Palette (10 dark-theme colors)
// ─────────────────────────────────────────────────────────────

window.biColors = [
    'rgba(59, 130, 246, 0.8)',   // blue
    'rgba(16, 185, 129, 0.8)',   // emerald
    'rgba(245, 158, 11, 0.8)',   // amber
    'rgba(239, 68, 68, 0.8)',    // red
    'rgba(139, 92, 246, 0.8)',   // violet
    'rgba(236, 72, 153, 0.8)',   // pink
    'rgba(6, 182, 212, 0.8)',    // cyan
    'rgba(251, 146, 60, 0.8)',   // orange
    'rgba(34, 197, 94, 0.8)',    // green
    'rgba(168, 162, 158, 0.8)',  // stone
];

window.biBorderColors = window.biColors.map(c => c.replace('0.8)', '1)'));

// ─────────────────────────────────────────────────────────────
// Number Formatting Helpers
// ─────────────────────────────────────────────────────────────
// Note: lucide re-init on htmx:afterSwap is owned by base.html — no
// listener needed here.

window.biFormatNumber = function (value) {
    if (value === null || value === undefined) return '—';
    const num = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(num)) return String(value);
    if (Math.abs(num) >= 1e9) return (num / 1e9).toFixed(1) + 'B';
    if (Math.abs(num) >= 1e6) return (num / 1e6).toFixed(1) + 'M';
    if (Math.abs(num) >= 1e3) return (num / 1e3).toFixed(1) + 'K';
    if (Number.isInteger(num)) return num.toLocaleString();
    return num.toFixed(2);
};

window.biFormatPercent = function (value) {
    const num = typeof value === 'number' ? value : parseFloat(value);
    if (isNaN(num)) return '—';
    return num.toFixed(1) + '%';
};

// ─────────────────────────────────────────────────────────────
// Last Refresh Timestamp
// ─────────────────────────────────────────────────────────────

function updateLastRefresh() {
    const el = document.getElementById('bi-last-refresh');
    if (!el) return;
    const now = new Date();
    const hh = String(now.getHours()).padStart(2, '0');
    const mm = String(now.getMinutes()).padStart(2, '0');
    const ss = String(now.getSeconds()).padStart(2, '0');
    el.textContent = 'Last refresh: ' + hh + ':' + mm + ':' + ss;
}

// ─────────────────────────────────────────────────────────────
// Init
// ─────────────────────────────────────────────────────────────

function initBIPage() {
    if (typeof lucide !== 'undefined') {
        lucide.createIcons();
    }
}

document.addEventListener('DOMContentLoaded', initBIPage);
