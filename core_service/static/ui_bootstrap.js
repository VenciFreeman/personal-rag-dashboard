(function () {
  "use strict";

  const existing = window.CoreUI || {};
  const modules = existing.modules || {};

  const coreUI = {
    modules,
    register(name, value) {
      const key = String(name || "").trim();
      if (!key) return value;
      modules[key] = value;
      return value;
    },
    get(name) {
      return modules[String(name || "").trim()] || null;
    },
    require(name) {
      return modules[String(name || "").trim()] || {};
    },
  };

  window.CoreUI = coreUI;
})();