/**
 * Lazy-load overview stage photos: sets background-image from data-overview-bg when the
 * element nears the viewport (IntersectionObserver). Uses decode via Image() first.
 */
(function () {
  function cssUrl(value) {
    return "url(" + JSON.stringify(String(value)) + ")";
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
    var img = new Image();
    img.onload = function () {
      el.style.backgroundImage = cssUrl(url);
      el.setAttribute("data-overview-loaded", "1");
      el.classList.add("cm-overview-bg-loaded");
    };
    img.onerror = function () {
      el.classList.add("cm-overview-bg-error");
      el.setAttribute("data-overview-loaded", "1");
    };
    img.src = url;
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
