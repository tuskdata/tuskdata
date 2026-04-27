/* Homepage interactions: timeAgo for recent rows + Ask AI shortcut.
   Stats and recent queries are server-rendered, so this file is small. */

function timeAgo(iso) {
  if (!iso) return "";
  const then = new Date(iso);
  const now = new Date();
  const sec = Math.floor((now - then) / 1000);
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min} min ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  const day = Math.floor(hr / 24);
  if (day < 7) return `${day}d ago`;
  return then.toLocaleDateString();
}

document.addEventListener("DOMContentLoaded", () => {
  document.querySelectorAll(".recent-time[data-iso]").forEach((el) => {
    el.textContent = timeAgo(el.dataset.iso);
  });
});

window.homePage = function () {
  return {
    async askAI() {
      // Open the cmdk palette with AI mode if available; otherwise jump
      // straight to settings to configure a provider.
      const status = await fetch("/api/ai/status").then((r) => r.json()).catch(() => null);
      if (!status || !status.configured) {
        window.location.href = "/settings/ai";
        return;
      }
      // cmdk palette is wired in v0.4.4 via base.html — fire its open event.
      window.dispatchEvent(new CustomEvent("tusk-cmdk-open", { detail: { mode: "ai" } }));
    },
  };
};
