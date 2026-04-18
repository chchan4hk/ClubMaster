if (window.dashboardInit("Coach", "Coach")) {
  window.initDashboardPanels();

  function moduleSrcNeedsClubFolder(src) {
    if (!src) {
      return false;
    }
    return (
      src.indexOf("coach_information") !== -1 ||
      src.indexOf("student_information") !== -1 ||
      src.indexOf("prize_modules") !== -1 ||
      src.indexOf("club_master") !== -1 ||
      src.indexOf("lesson_modules") !== -1 ||
      src.indexOf("lesson_reservation") !== -1 ||
      src.indexOf("coach_salary_payment") !== -1
    );
  }

  function resolveClubFolderFromDom() {
    var ctx =
      window.api.getClubFolderContext &&
      window.api.getClubFolderContext();
    var dock = document.getElementById("clubDock");
    var cid =
      (ctx && ctx.clubId) ||
      (dock && dock.dataset && dock.dataset.clubId) ||
      "";
    var cname =
      (ctx && ctx.clubName) ||
      (dock && dock.dataset && dock.dataset.clubName) ||
      "";
    return { cid: cid, cname: cname };
  }

  function applyClubQueryToSrc(baseSrc, cid, cname, duplicateUid) {
    if (!cid) {
      return baseSrc;
    }
    var sep = baseSrc.indexOf("?") >= 0 ? "&" : "?";
    var qs =
      "clubId=" +
      encodeURIComponent(cid) +
      (duplicateUid ? "&uid=" + encodeURIComponent(cid) : "") +
      (cname ? "&clubName=" + encodeURIComponent(cname) : "");
    return baseSrc + sep + qs;
  }

  function syncClubFolderFromMe() {
    return window.api.api("/me").then(function (data) {
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
      return resolveClubFolderFromDom();
    });
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

    var needCtx = moduleSrcNeedsClubFolder(src);
    var duplicateUid =
      src.indexOf("prize_modules") !== -1 ||
      src.indexOf("student_modules") !== -1 ||
      src.indexOf("lesson_modules") !== -1;

    function openResolved(finalSrc) {
      window.openDashboardPanelIframe(finalSrc, document.getElementById("navOverview"));
    }

    if (!needCtx) {
      openResolved(src);
      return;
    }

    var r = resolveClubFolderFromDom();
    if (r.cid) {
      openResolved(applyClubQueryToSrc(src, r.cid, r.cname, duplicateUid));
      return;
    }

    syncClubFolderFromMe()
      .then(function (next) {
        openResolved(
          applyClubQueryToSrc(src, next.cid, next.cname, duplicateUid)
        );
      })
      .catch(function () {
        openResolved(applyClubQueryToSrc(src, r.cid, r.cname, duplicateUid));
      });
  });
}
