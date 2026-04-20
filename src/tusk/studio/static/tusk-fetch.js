// Shared fetch helpers for TuskData Studio.
// - tuskFetch: fetch with a timeout that aborts cleanly
// - tuskFetchJSON: tuskFetch + JSON parse + normalized error

(function () {
    const DEFAULT_TIMEOUT_MS = 120000; // 2 min; long queries override per-call

    function tuskFetch(url, options = {}) {
        const { timeoutMs = DEFAULT_TIMEOUT_MS, signal: externalSignal, ...rest } = options;
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(new DOMException('Timeout', 'TimeoutError')), timeoutMs);

        if (externalSignal) {
            if (externalSignal.aborted) {
                controller.abort(externalSignal.reason);
            } else {
                externalSignal.addEventListener('abort', () => controller.abort(externalSignal.reason), { once: true });
            }
        }

        return fetch(url, { ...rest, signal: controller.signal })
            .finally(() => clearTimeout(timer));
    }

    async function tuskFetchJSON(url, options = {}) {
        try {
            const res = await tuskFetch(url, options);
            if (!res.ok) {
                const text = await res.text().catch(() => '');
                return { error: `HTTP ${res.status}: ${text || res.statusText}` };
            }
            return await res.json();
        } catch (err) {
            if (err.name === 'AbortError' || err.name === 'TimeoutError') {
                return { error: 'Request timed out' };
            }
            return { error: err.message || String(err) };
        }
    }

    window.tuskFetch = tuskFetch;
    window.tuskFetchJSON = tuskFetchJSON;
})();
