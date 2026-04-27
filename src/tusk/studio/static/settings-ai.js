/* AI settings page — provider config + test/save/refresh-models. */

window.aiSettings = function () {
  const cfg = window.AI_CONFIG || {};
  const defaults = window.AI_PROVIDER_DEFAULTS || {};
  return {
    ai_config: cfg,
    form: {
      enabled: !!cfg.enabled,
      provider: cfg.provider || "ollama",
      base_url: cfg.base_url || "",
      model: cfg.model || "",
      // Plaintext only when user types a new key. Empty + has_api_key=true
      // means "keep what's saved".
      api_key: "",
    },
    availableModels: [],
    loadingModels: false,
    testing: false,
    saving: false,
    testResult: null,

    onProviderChange() {
      const d = defaults[this.form.provider];
      if (!d) return;
      // Switch base/model defaults but never wipe what the user already typed.
      if (!this.form.base_url) this.form.base_url = d.base_url;
      if (!this.form.model) this.form.model = d.model;
    },

    async loadModels() {
      this.loadingModels = true;
      try {
        const res = await tuskFetchJSON("/api/ai/models", {
          method: "POST",
          body: JSON.stringify({
            provider: this.form.provider,
            base_url: this.form.base_url,
            api_key: this.form.api_key,
          }),
        });
        if (res.error) {
          tuskToast(res.error, "error");
          this.availableModels = [];
        } else {
          this.availableModels = res.models || [];
          if (!this.availableModels.length) {
            tuskToast("No models found — pull one first (e.g. ollama pull qwen2.5-coder:3b)", "warning");
          }
        }
      } catch (e) {
        tuskToast("Failed to load models: " + e.message, "error");
      } finally {
        this.loadingModels = false;
      }
    },

    async testProvider() {
      this.testing = true;
      this.testResult = null;
      try {
        this.testResult = await tuskFetchJSON("/api/ai/test", {
          method: "POST",
          body: JSON.stringify({
            provider: this.form.provider,
            base_url: this.form.base_url,
            api_key: this.form.api_key,
            model: this.form.model,
          }),
        });
      } catch (e) {
        this.testResult = { ok: false, error: e.message };
      } finally {
        this.testing = false;
        // Re-render Lucide for the result icon
        if (window.lucide) lucide.createIcons();
      }
    },

    async saveConfig() {
      this.saving = true;
      try {
        const payload = {
          enabled: this.form.enabled,
          provider: this.form.provider,
          base_url: this.form.base_url,
          model: this.form.model,
        };
        // Only include api_key when the user typed something — empty
        // string preserves the saved encrypted value.
        if (this.form.api_key) payload.api_key = this.form.api_key;
        const res = await tuskFetchJSON("/api/ai/config", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        if (res.ok) {
          tuskToast("AI settings saved", "success");
          this.form.api_key = "";
          this.ai_config.has_api_key = this.ai_config.has_api_key || !!payload.api_key;
        } else {
          tuskToast(res.error || "Failed to save", "error");
        }
      } catch (e) {
        tuskToast("Save failed: " + e.message, "error");
      } finally {
        this.saving = false;
      }
    },
  };
};
