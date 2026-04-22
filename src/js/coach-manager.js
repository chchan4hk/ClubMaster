if (window.dashboardInit("CoachManager", "Coach manager")) {
  window.initDashboardPanels();

  document.getElementById("panelOverview")?.addEventListener("click", function (ev) {
    var link = ev.target.closest("a.cm-hotspot[data-dashboard-iframe]");
    if (!link) {
      return;
    }
    ev.preventDefault();
    var src = link.getAttribute("data-dashboard-iframe");
    if (src && typeof window.openDashboardPanelIframe === "function") {
      if (
        src.indexOf("coach_information") !== -1 ||
        src.indexOf("student_information") !== -1 ||
        src.indexOf("prize_modules") !== -1 ||
        src.indexOf("club_master") !== -1 ||
        src.indexOf("lesson_modules") !== -1 ||
        src.indexOf("payment_modules") !== -1 ||
        src.indexOf("coach_salary") !== -1
      ) {
        var ctx =
          window.api.getClubFolderContext &&
          window.api.getClubFolderContext();
        var cid = (ctx && ctx.clubId) || "";
        var cname = (ctx && ctx.clubName) || "";
        if (cid) {
          var sep = src.indexOf("?") >= 0 ? "&" : "?";
          src =
            src +
            sep +
            "club_id=" +
            encodeURIComponent(cid) +
            "&clubId=" +
            encodeURIComponent(cid) +
            (cname ? "&clubName=" + encodeURIComponent(cname) : "");
        }
      }
      window.openDashboardPanelIframe(src, document.getElementById("navOverview"));
    }
  });
}
