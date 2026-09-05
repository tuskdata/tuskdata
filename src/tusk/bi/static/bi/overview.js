/**
 * Tusk BI — Overview page chart (Query Volume sparkline).
 *
 * Reads the volume series from `window._biQueryVolume` (set inline by
 * the template) and renders a Chart.js line chart into #query-volume-chart.
 */

(function () {
    function render() {
        const volumeData = window._biQueryVolume || [];
        const ctx = document.getElementById('query-volume-chart');
        if (!ctx || typeof Chart === 'undefined') return;

        const cs = getComputedStyle(document.documentElement);
        const brandColor = cs.getPropertyValue('--brand').trim() || '#d4502b';

        new Chart(ctx.getContext('2d'), {
            type: 'line',
            data: {
                labels: volumeData.map(d => d.day ? d.day.slice(5) : ''),
                datasets: [{
                    label: 'Queries',
                    data: volumeData.map(d => d.count || 0),
                    borderColor: brandColor,
                    backgroundColor: 'rgba(212, 80, 43, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.3,
                    pointRadius: 3,
                    pointBackgroundColor: brandColor,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: { legend: { display: false } },
                scales: {
                    x: {
                        ticks: { color: cs.getPropertyValue('--fg-3').trim(), font: { size: 10 } },
                        grid: { display: false },
                    },
                    y: {
                        beginAtZero: true,
                        ticks: { color: cs.getPropertyValue('--fg-3').trim(), font: { size: 10 }, precision: 0 },
                        grid: { color: cs.getPropertyValue('--border').trim() },
                    },
                },
            },
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', render);
    } else {
        render();
    }
})();
