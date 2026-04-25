/**
 * Lazy-load overview stage photos: sets background-image from data-overview-bg when the
 * element nears the viewport (IntersectionObserver). Uses decode via Image() first.
 */
(function () {
  function cssUrl(value) {
    return "url(" + JSON.stringify(String(value)) + ")";
  }

  function safeThemeSegment(raw) {
    var s = String(raw == null ? "" : raw).trim();
    if (!s) return "";
    // keep simple: allow a-z0-9, dash, underscore; fold spaces to dashes
    s = s.toLowerCase().replace(/\s+/g, "-");
    s = s.replace(/[^a-z0-9_-]/g, "");
    return s;
  }

  function getClubTheme() {
    try {
      var t = sessionStorage.getItem("sportCoach.clubTheme");
      if (t && String(t).trim() !== "") return String(t).trim();
    } catch {
      /* ignore */
    }
    var dock = document.getElementById("clubDock");
    var dt = dock && dock.dataset ? dock.dataset.clubTheme : "";
    return dt && String(dt).trim() !== "" ? String(dt).trim() : "";
  }

  function resolveUrl(rel) {
    var s = String(rel || "").trim();
    if (!s) {
      return "";
    }
    try {
      return new URL(s, window.location.href).href;
    } catch {
      return s;
    }
  }

  function themedRelForDefault(rel, theme) {
    var r = String(rel || "").trim();
    if (!r) return "";
    var seg = safeThemeSegment(theme);
    if (!seg) return "";
    // Only rewrite the standard default folder.
    var marker = "source/image/";
    var idx = r.toLowerCase().indexOf(marker);
    if (idx === -1) return "";
    var file = r.slice(idx + marker.length);
    if (!file) return "";
    // New canonical themed location: `source/image/<Theme>/...` (Theme uses original casing).
    // Keep an old fallback path for earlier experiments: `source/<theme-seg>/image/...`.
    var themedByFolder = "source/image/" + String(theme).trim() + "/" + file;
    var themedBySegment = "source/" + seg + "/image/" + file;
    return themedByFolder + "||" + themedBySegment;
  }

  function loadEl(el) {
    if (!el || el.getAttribute("data-overview-loaded") === "1") {
      return;
    }
    var rel = el.getAttribute("data-overview-bg");
    if (!rel) {
      return;
    }
    var url = resolveUrl(rel);
    if (!url) {
      return;
    }
    var theme = getClubTheme();
    var themedRel = theme ? themedRelForDefault(rel, theme) : "";
    var themedCandidates = themedRel ? String(themedRel).split("||").filter(Boolean) : [];
    var themedUrls = themedCandidates.map(resolveUrl).filter(Boolean);

    function doneOk(finalUrl) {
      el.style.backgroundImage = cssUrl(finalUrl);
      el.setAttribute("data-overview-loaded", "1");
      el.classList.add("cm-overview-bg-loaded");
    }

    function doneErr() {
      el.classList.add("cm-overview-bg-error");
      el.setAttribute("data-overview-loaded", "1");
    }

    function loadUrl(candidateUrl, onOk, onErr) {
      var img = new Image();
      img.onload = function () {
        onOk(candidateUrl);
      };
      img.onerror = onErr;
      img.src = candidateUrl;
    }

    function loadFallback(i) {
      if (i >= themedUrls.length) {
        loadUrl(url, doneOk, doneErr);
        return;
      }
      var candidate = themedUrls[i];
      if (!candidate || candidate === url) {
        loadFallback(i + 1);
        return;
      }
      loadUrl(candidate, doneOk, function () {
        loadFallback(i + 1);
      });
    }

    if (themedUrls.length) {
      loadFallback(0);
    } else {
      loadUrl(url, doneOk, doneErr);
    }
  }

  function boot() {
    var els = document.querySelectorAll("[data-overview-bg]");
    if (!els.length) {
      return;
    }
    if (!("IntersectionObserver" in window)) {
      els.forEach(loadEl);
      return;
    }
    var io = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (ent) {
          if (ent.isIntersecting) {
            loadEl(ent.target);
            io.unobserve(ent.target);
          }
        });
      },
      { root: null, rootMargin: "240px 0px 320px 0px", threshold: 0.01 },
    );
    els.forEach(function (el) {
      io.observe(el);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
