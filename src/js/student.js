if (window.dashboardInit("Student", "Student")) {
  window.initDashboardPanels();

  function moduleSrcNeedsClubFolder(src) {
    if (!src) {
      return false;
    }
    return (
      src.indexOf("coach_manager_modules") !== -1 ||
      src.indexOf("lesson_modules") !== -1 ||
      src.indexOf("payment_modules") !== -1
    );
  }

  function applyClubQueryToSrc(baseSrc, cid, cname) {
    if (!cid) {
      return baseSrc;
    }
    var sep = baseSrc.indexOf("?") >= 0 ? "&" : "?";
    var qs =
      "club_id=" +
      encodeURIComponent(cid) +
      "&clubId=" +
      encodeURIComponent(cid) +
      "&uid=" +
      encodeURIComponent(cid) +
      (cname ? "&clubName=" + encodeURIComponent(cname) : "");
    return baseSrc + sep + qs;
  }

  function resolveClubFolderFromDom() {
    var ctx =
      window.api.getClubFolderContext && window.api.getClubFolderContext();
    return {
      cid: (ctx && ctx.clubId) || "",
      cname: (ctx && ctx.clubName) || "",
    };
  }

  function syncClubFolderFromMe() {
    var p =
      typeof window.dashboardWaitMe === "function"
        ? window.dashboardWaitMe()
        : window.api.api("/me");
    return p.then(function (data) {
      var u = data.user || {};
      var cfu =
        u.club_folder_uid != null && String(u.club_folder_uid).trim() !== ""
          ? String(u.club_folder_uid).trim()
          : "";
      var cn =
        u.club_name != null &&
        String(u.club_name).trim() !== "" &&
        u.club_name !== "—"
          ? String(u.club_name).trim()
          : "";
      if (cfu && typeof window.api.setClubFolderContext === "function") {
        window.api.setClubFolderContext(cfu, cn || null);
      }
      return { cid: cfu, cname: cn };
    });
  }

  function appendClubToIframeSrcIfNeeded(src) {
    var s = String(src || "").trim();
    if (!s || !moduleSrcNeedsClubFolder(s)) {
      return s;
    }
    var r = resolveClubFolderFromDom();
    if (r.cid) {
      return applyClubQueryToSrc(s, r.cid, r.cname);
    }
    return s;
  }

  document.getElementById("panelOverview")?.addEventListener("click", function (ev) {
    var link = ev.target.closest("a.cm-hotspot[data-dashboard-iframe]");
    if (!link) {
      return;
    }
    ev.preventDefault();
    var src = link.getAttribute("data-dashboard-iframe");
    if (!src || typeof window.openDashboardPanelIframe !== "function") {
      return;
    }

    function openResolved(finalSrc) {
      window.openDashboardPanelIframe(finalSrc, document.getElementById("navOverview"));
    }

    var withClub = appendClubToIframeSrcIfNeeded(src);
    if (withClub !== src || resolveClubFolderFromDom().cid) {
      openResolved(withClub);
      return;
    }

    syncClubFolderFromMe()
      .then(function (next) {
        openResolved(
          moduleSrcNeedsClubFolder(src)
            ? applyClubQueryToSrc(src, next.cid, next.cname)
            : src
        );
      })
      .catch(function () {
        openResolved(src);
      });
  });

}
