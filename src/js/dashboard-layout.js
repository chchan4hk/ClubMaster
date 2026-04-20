/**
 * Top nav: Overview | My Profile | Password Setting (student) or Settings → panel (overview div or iframe).
 */
window.initDashboardPanels = function initDashboardPanels() {
  var overview = document.getElementById("panelOverview");
  var frame = document.getElementById("panelFrame");
  var navOverview = document.getElementById("navOverview");
  var navProfile = document.getElementById("navProfile");
  var navSettings = document.getElementById("navSettings");

  var activeAttr =
    document.body && document.body.getAttribute("data-dashboard-nav-active");
  var activeClasses =
    activeAttr && String(activeAttr).trim() !== ""
      ? String(activeAttr).trim().split(/\s+/)
      : ["ring-2", "ring-amber-500/60", "bg-white/10"];

  function clearNavActive() {
    [navOverview, navProfile, navSettings].forEach(function (el) {
      if (!el) {
        return;
      }
      activeClasses.forEach(function (c) {
        el.classList.remove(c);
      });
    });
  }

  function setNavActive(el) {
    clearNavActive();
    if (el) {
      activeClasses.forEach(function (c) {
        el.classList.add(c);
      });
    }
  }

  function resolvePanelUrl(src) {
    if (!src) {
      return src;
    }
    var s = String(src).trim();
    if (/^https?:\/\//i.test(s)) {
      return s;
    }
    if (s.startsWith("/")) {
      return window.location.origin + s;
    }
    try {
      return new URL(s, window.location.href).href;
    } catch (e) {
      return s;
    }
  }

  function showOverview() {
    if (overview) {
      overview.classList.remove("hidden");
    }
    if (frame) {
      frame.classList.add("hidden");
      frame.removeAttribute("src");
    }
    setNavActive(navOverview);
  }

  function postClubContextToPanel(frameEl) {
    if (!frameEl || !frameEl.contentWindow) {
      return;
    }
    var ctx =
      window.api && window.api.getClubFolderContext && window.api.getClubFolderContext();
    var dock = document.getElementById("clubDock");
    var cid =
      (ctx && ctx.clubId) ||
      (dock && dock.dataset && dock.dataset.clubId) ||
      "";
    var cname =
      (ctx && ctx.clubName) ||
      (dock && dock.dataset && dock.dataset.clubName) ||
      "";
    if (!cid) {
      return;
    }
    var payload = {
      type: "sportCoach.clubContext",
      clubId: cid,
      clubName: cname || "",
    };
    function send() {
      try {
        frameEl.contentWindow.postMessage(payload, window.location.origin);
      } catch (e) {
        /* ignore */
      }
    }
    send();
    setTimeout(send, 50);
    setTimeout(send, 200);
  }

  function showIframe(src) {
    if (overview) {
      overview.classList.add("hidden");
    }
    if (frame) {
      frame.classList.remove("hidden");
      var resolved = resolvePanelUrl(src);
      frame.onload = function () {
        frame.onload = null;
        postClubContextToPanel(frame);
      };
      frame.src = resolved;
    }
  }

  /**
   * Open a URL in the right-hand panel iframe (same as Profile/Settings).
   * @param {string} src - path relative to site root, e.g. "useraccount.html"
   * @param {HTMLElement | null} [navButton] - optional nav button to mark active; default Overview
   */
  window.openDashboardPanelIframe = function openDashboardPanelIframe(src, navButton) {
    showIframe(src);
    var fallbackNav = navOverview || null;
    setNavActive(navButton != null ? navButton : fallbackNav);
  };

  navOverview?.addEventListener("click", showOverview);
  navProfile?.addEventListener("click", function () {
    window.openDashboardPanelIframe("useraccount.html", navProfile);
  });
  navSettings?.addEventListener("click", function () {
    window.openDashboardPanelIframe("userpassword.html", navSettings);
  });

  showOverview();
};
