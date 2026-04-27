/**
 * Lazy-load Chart.js from CDN after first paint (smaller initial blocking work than a sync tag in <head>).
 * Used by Coach Manager / Admin pages that render charts only after data loads.
 */
(function (g) {
  var CHART_SRC =
    "https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js";
  var inflight = null;

  g.ensureChartJs = function ensureChartJs() {
    if (typeof Chart !== "undefined") {
      return Promise.resolve();
    }
    if (inflight) {
      return inflight;
    }
    inflight = new Promise(function (resolve, reject) {
      var s = document.createElement("script");
      s.src = CHART_SRC;
      s.async = true;
      s.crossOrigin = "anonymous";
      s.onload = function () {
        inflight = null;
        if (typeof Chart === "undefined") {
          reject(new Error("Chart.js loaded but global Chart is missing."));
          return;
        }
        resolve();
      };
      s.onerror = function () {
        inflight = null;
        reject(new Error("Failed to load Chart.js."));
      };
      document.head.appendChild(s);
    });
    return inflight;
  };
})(typeof window !== "undefined" ? window : globalThis);
