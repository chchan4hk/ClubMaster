if (window.dashboardInit("Student", "Student")) {
  window.initDashboardPanels();
  document.querySelectorAll("a.cm-hotspot[data-student-panel]").forEach(function (a) {
    a.addEventListener("click", function (ev) {
      ev.preventDefault();
      var src = a.getAttribute("data-student-panel");
      if (src && typeof window.openDashboardPanelIframe === "function") {
        window.openDashboardPanelIframe(src, null);
      }
    });
  });
}
