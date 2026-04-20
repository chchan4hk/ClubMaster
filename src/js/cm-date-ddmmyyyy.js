/**
 * Text date fields showing DD/MM/YYYY while APIs use YYYY-MM-DD.
 * Add class `cm-date-ddmmyyyy` to inputs; use readIso / writeIso from scripts.
 */
(function (global) {
  function pad2(n) {
    return String(n).padStart(2, "0");
  }

  /** Parse various inputs → YYYY-MM-DD or "". */
  function parseToIso(raw) {
    var t = String(raw == null ? "" : raw).trim();
    if (!t) {
      return "";
    }
    var isoStart = /^(\d{4}-\d{2}-\d{2})/.exec(t);
    if (isoStart) {
      return isoStart[1];
    }
    if (/^\d{4}\/\d{2}\/\d{2}$/.test(t)) {
      return t.replace(/\//g, "-");
    }
    var m = /^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/.exec(t);
    if (m) {
      var d = parseInt(m[1], 10);
      var mo = parseInt(m[2], 10);
      var y = parseInt(m[3], 10);
      if (mo >= 1 && mo <= 12 && d >= 1 && d <= 31) {
        var iso = y + "-" + pad2(mo) + "-" + pad2(d);
        var dt = new Date(y, mo - 1, d);
        if (
          !isNaN(dt.getTime()) &&
          dt.getFullYear() === y &&
          dt.getMonth() === mo - 1 &&
          dt.getDate() === d
        ) {
          return iso;
        }
      }
    }
    return "";
  }

  function formatIsoToDdMmYyyy(iso) {
    var t = String(iso || "").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(t)) {
      return "";
    }
    var p = t.split("-");
    return pad2(parseInt(p[2], 10)) + "/" + pad2(parseInt(p[1], 10)) + "/" + p[0];
  }

  function formatAnyToDdMmYyyy(s) {
    var iso = parseToIso(s);
    return iso ? formatIsoToDdMmYyyy(iso) : String(s || "").trim();
  }

  function bindInput(el) {
    if (!el || el.getAttribute("data-cm-date-bound") === "1") {
      return;
    }
    el.setAttribute("data-cm-date-bound", "1");
    if (!el.getAttribute("placeholder")) {
      el.setAttribute("placeholder", "DD/MM/YYYY");
    }
    el.setAttribute("inputmode", "numeric");
    el.setAttribute("autocomplete", "off");
    el.setAttribute("maxlength", "10");
    el.addEventListener("blur", function () {
      var iso = parseToIso(el.value);
      el.value = iso ? formatIsoToDdMmYyyy(iso) : el.value.trim();
    });
  }

  function bindAll(root) {
    (root || document).querySelectorAll("input.cm-date-ddmmyyyy").forEach(bindInput);
  }

  function resolveEl(elOrId) {
    if (elOrId == null) {
      return null;
    }
    if (typeof elOrId === "string") {
      return document.getElementById(elOrId);
    }
    return elOrId;
  }

  function readIso(elOrId) {
    var el = resolveEl(elOrId);
    if (!el) {
      return "";
    }
    if (el.type === "date") {
      return String(el.value || "").trim();
    }
    return parseToIso(el.value);
  }

  function writeIso(elOrId, iso) {
    var el = resolveEl(elOrId);
    if (!el) {
      return;
    }
    var v = String(iso || "").trim();
    if (el.type === "date") {
      el.value = /^\d{4}-\d{2}-\d{2}$/.test(v) ? v : "";
      return;
    }
    el.value = v && /^\d{4}-\d{2}-\d{2}$/.test(v) ? formatIsoToDdMmYyyy(v) : "";
  }

  /** Show DD/MM/YYYY from API / stored value (ISO, datetime, or DD/MM). */
  function populateFromBackend(elOrId, raw) {
    writeIso(elOrId, parseToIso(raw));
  }

  global.cmDateDdMmYyyy = {
    parseToIso: parseToIso,
    formatIsoToDdMmYyyy: formatIsoToDdMmYyyy,
    formatAnyToDdMmYyyy: formatAnyToDdMmYyyy,
    bindAll: bindAll,
    readIso: readIso,
    writeIso: writeIso,
    populateFromBackend: populateFromBackend,
  };

  function runBind() {
    bindAll(document);
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", runBind);
  } else {
    runBind();
  }
})(typeof window !== "undefined" ? window : this);
