/**
 * Admin Sport Activation — lists sport types and countries; add/remove via Mongo
 * canonical basicInfo (GET/POST/DELETE on /api/basic-info/admin/sport-types and /admin/countries).
 */
(function () {
  "use strict";

  var lastSource = "";

  function el(id) {
    return document.getElementById(id);
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function escapeAttr(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/"/g, "&quot;")
      .replace(/</g, "&lt;");
  }

  function normalizeCountryRow(c) {
    if (c == null) {
      return null;
    }
    // Canonical Mongo shape: [country, country_code]
    if (Array.isArray(c)) {
      var nameA = String(c[0] == null ? "" : c[0]).trim();
      if (!nameA) return null;
      var codeA = String(c[1] == null ? "" : c[1]).trim();
      return { name: nameA, country_code: codeA };
    }
    if (typeof c === "string") {
      var ns = String(c).trim();
      return ns ? { name: ns, country_code: "" } : null;
    }
    if (typeof c === "object" && c.name != null) {
      var name = String(c.name).trim();
      if (!name) {
        return null;
      }
      var code =
        c.country_code != null
          ? String(c.country_code).trim()
          : c.CountryCode != null
            ? String(c.CountryCode).trim()
            : "";
      return { name: name, country_code: code };
    }
    return null;
  }

  function setBanner(msg) {
    var b = el("errorBanner");
    if (!b) return;
    if (msg) {
      b.textContent = msg;
      b.classList.remove("hidden");
    } else {
      b.textContent = "";
      b.classList.add("hidden");
    }
  }

  function setMongoControls(on) {
    var inputs = document.querySelectorAll(".btn-mongo-input");
    for (var i = 0; i < inputs.length; i++) {
      inputs[i].disabled = !on;
    }
    var btns = document.querySelectorAll(".btn-mongo");
    for (var j = 0; j < btns.length; j++) {
      btns[j].disabled = !on;
    }
    var removeBtns = document.querySelectorAll(".btn-remove-item");
    for (var k = 0; k < removeBtns.length; k++) {
      removeBtns[k].disabled = !on;
    }
  }

  function fillMeta(data) {
    el("metaSource").textContent = data.source || "—";
    el("metaDatabase").textContent = data.database != null ? String(data.database) : "—";
    el("metaCollection").textContent = data.collection != null ? String(data.collection) : "—";
    el("metaDocId").textContent = data.documentId != null ? String(data.documentId) : "—";
  }

  function renderSportList(sportTypes) {
    var ul = el("sportList");
    var countEl = el("sportCount");
    if (!ul) return;
    var list = Array.isArray(sportTypes) ? sportTypes : [];
    if (countEl) {
      countEl.textContent = list.length ? list.length + " items" : "0 items";
    }
    if (!list.length) {
      ul.innerHTML =
        '<li class="px-4 py-8 text-center text-slate-500 text-sm">No sport types.</li>';
      return;
    }
    ul.innerHTML = list
      .map(function (name) {
        var raw = String(name == null ? "" : name);
        var ea = escapeAttr(raw);
        return (
          '<li class="flex items-center justify-between gap-3 px-4 py-3 hover:bg-slate-800/40">' +
          '<span class="text-slate-100 text-sm">' +
          escapeHtml(raw) +
          "</span>" +
          '<button type="button" class="btn-remove-item btn-mongo shrink-0 rounded border border-slate-600 bg-slate-800/80 hover:bg-red-950/50 hover:border-red-800/60 text-slate-300 hover:text-red-200 text-xs font-medium px-2.5 py-1 disabled:opacity-40" data-kind="sport" data-name="' +
          ea +
          '">Remove</button>' +
          "</li>"
        );
      })
      .join("");
  }

  function renderCountryList(countries) {
    var ul = el("countryList");
    var countEl = el("countryCount");
    if (!ul) return;
    var list = [];
    if (Array.isArray(countries)) {
      for (var i = 0; i < countries.length; i++) {
        var row = normalizeCountryRow(countries[i]);
        if (row) {
          list.push(row);
        }
      }
    }
    if (countEl) {
      countEl.textContent = list.length ? list.length + " items" : "0 items";
    }
    if (!list.length) {
      ul.innerHTML =
        '<li class="px-4 py-8 text-center text-slate-500 text-sm">No countries.</li>';
      return;
    }
    ul.innerHTML = list
      .map(function (row) {
        var ea = escapeAttr(row.name);
        var sub = row.country_code
          ? '<span class="block text-xs text-slate-500 font-mono mt-0.5">' +
            escapeHtml(row.country_code) +
            "</span>"
          : "";
        return (
          '<li class="flex items-start justify-between gap-3 px-4 py-3 hover:bg-slate-800/40">' +
          '<div class="min-w-0">' +
          '<span class="text-slate-100 text-sm">' +
          escapeHtml(row.name) +
          "</span>" +
          sub +
          "</div>" +
          '<button type="button" class="btn-remove-item btn-mongo shrink-0 rounded border border-slate-600 bg-slate-800/80 hover:bg-red-950/50 hover:border-red-800/60 text-slate-300 hover:text-red-200 text-xs font-medium px-2.5 py-1 disabled:opacity-40 mt-0.5" data-kind="country" data-name="' +
          ea +
          '">Remove</button>' +
          "</li>"
        );
      })
      .join("");
  }

  function applyPayload(data) {
    fillMeta(data);
    lastSource = String(data.source || "");
    renderSportList(data.sportTypes);
    renderCountryList(data.countries);
    var mongo = lastSource === "mongodb";
    setMongoControls(mongo);
    if (!mongo) {
      setBanner(
        "Lists are from CSV — Mongo basicInfo is not in use. Add and remove require MongoDB.",
      );
    } else {
      setBanner("");
    }
  }

  async function loadLists() {
    setBanner("");
    var sportUl = el("sportList");
    var countryUl = el("countryList");
    if (sportUl) {
      sportUl.innerHTML =
        '<li class="px-4 py-6 text-center text-slate-500 text-sm">Loading…</li>';
    }
    if (countryUl) {
      countryUl.innerHTML =
        '<li class="px-4 py-6 text-center text-slate-500 text-sm">Loading…</li>';
    }
    if (el("sportCount")) el("sportCount").textContent = "";
    if (el("countryCount")) el("countryCount").textContent = "";
    try {
      if (!window.api || typeof window.api.api !== "function") {
        throw new Error("api.js not loaded");
      }
      var data = await window.api.api("/basic-info/admin/sport-types");
      if (!data || !data.ok) {
        throw new Error((data && data.error) || "Request failed");
      }
      applyPayload(data);
    } catch (e) {
      var msg = e && e.message ? e.message : String(e);
      setBanner(msg);
      if (sportUl) {
        sportUl.innerHTML =
          '<li class="px-4 py-6 text-center text-red-300/90 text-sm">Could not load. Sign in as Admin from the same origin.</li>';
      }
      if (countryUl) {
        countryUl.innerHTML =
          '<li class="px-4 py-6 text-center text-red-300/90 text-sm">Could not load.</li>';
      }
      setMongoControls(false);
    }
  }

  async function addSport() {
    setBanner("");
    var input = el("newSportName");
    var name = input ? String(input.value || "").trim() : "";
    if (!name) {
      setBanner("Enter a sport type name.");
      return;
    }
    try {
      var data = await window.api.api("/basic-info/admin/sport-types", {
        method: "POST",
        body: JSON.stringify({ name: name }),
      });
      if (!data || !data.ok) {
        throw new Error((data && data.error) || "Add failed");
      }
      if (input) input.value = "";
      applyPayload(data);
    } catch (e) {
      setBanner(e && e.message ? e.message : String(e));
    }
  }

  async function addCountry() {
    setBanner("");
    var input = el("newCountryName");
    var codeInput = el("newCountryCode");
    var name = input ? String(input.value || "").trim() : "";
    var code = codeInput ? String(codeInput.value || "").trim() : "";
    if (!name) {
      setBanner("Enter a country name.");
      return;
    }
    try {
      var data = await window.api.api("/basic-info/admin/countries", {
        method: "POST",
        body: JSON.stringify({ name: name, country_code: code }),
      });
      if (!data || !data.ok) {
        throw new Error((data && data.error) || "Add failed");
      }
      if (input) input.value = "";
      if (codeInput) codeInput.value = "";
      applyPayload(data);
    } catch (e) {
      setBanner(e && e.message ? e.message : String(e));
    }
  }

  async function removeItem(kind, name) {
    setBanner("");
    if (
      !confirm(
        kind === "sport"
          ? 'Remove sport type "' + name + '"?'
          : 'Remove country "' + name + '"?',
      )
    ) {
      return;
    }
    try {
      var path =
        kind === "sport"
          ? "/basic-info/admin/sport-types"
          : "/basic-info/admin/countries";
      var data = await window.api.api(path, {
        method: "DELETE",
        body: JSON.stringify({ name: name }),
      });
      if (!data || !data.ok) {
        throw new Error((data && data.error) || "Remove failed");
      }
      applyPayload(data);
    } catch (e) {
      setBanner(e && e.message ? e.message : String(e));
    }
  }

  document.addEventListener("DOMContentLoaded", function () {
    var reload = el("btnReload");
    if (reload) reload.addEventListener("click", loadLists);

    var addS = el("btnAddSport");
    if (addS) addS.addEventListener("click", addSport);
    var addC = el("btnAddCountry");
    if (addC) addC.addEventListener("click", addCountry);

    var sportName = el("newSportName");
    if (sportName) {
      sportName.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter") {
          ev.preventDefault();
          addSport();
        }
      });
    }
    var countryName = el("newCountryName");
    if (countryName) {
      countryName.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter") {
          ev.preventDefault();
          addCountry();
        }
      });
    }
    var countryCode = el("newCountryCode");
    if (countryCode) {
      countryCode.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter") {
          ev.preventDefault();
          addCountry();
        }
      });
    }

    document.addEventListener("click", function (ev) {
      var t = ev.target;
      if (!t || !t.closest) return;
      var btn = t.closest(".btn-remove-item");
      if (!btn) return;
      var kind = btn.getAttribute("data-kind");
      var name = btn.getAttribute("data-name");
      if (!kind || name == null) return;
      removeItem(kind, name);
    });

    loadLists();
  });
})();
