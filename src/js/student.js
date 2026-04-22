if (window.dashboardInit("Student", "Student")) {
  window.initDashboardPanels();

  function moduleSrcNeedsClubFolder(src) {
    if (!src) {
      return false;
    }
    return (
      src.indexOf("coach_manager_modules") !== -1 ||
      src.indexOf("lesson_modules") !== -1
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
      return { cid: cfu, cname: cn };
    });
  }

  function refreshStudentPaymentLinkWithClub() {
    var pay = document.getElementById("studentPaymentLink");
    if (!pay) {
      return;
    }
    function apply(cid) {
      if (!cid) {
        return;
      }
      pay.href =
        "/student/payment?club_id=" +
        encodeURIComponent(cid) +
        "&clubId=" +
        encodeURIComponent(cid);
    }
    var r = resolveClubFolderFromDom();
    if (r.cid) {
      apply(r.cid);
      return;
    }
    syncClubFolderFromMe()
      .then(function (next) {
        apply(next.cid);
      })
      .catch(function () {
        /* keep default href */
      });
  }

  document.querySelectorAll("a.cm-hotspot[data-student-panel]").forEach(function (a) {
    a.addEventListener("click", function (ev) {
      ev.preventDefault();
      var src = a.getAttribute("data-student-panel");
      if (!src || typeof window.openDashboardPanelIframe !== "function") {
        return;
      }

      function openResolved(finalSrc) {
        window.openDashboardPanelIframe(finalSrc, null);
      }

      if (!moduleSrcNeedsClubFolder(src)) {
        openResolved(src);
        return;
      }

      var r = resolveClubFolderFromDom();
      if (r.cid) {
        openResolved(applyClubQueryToSrc(src, r.cid, r.cname));
        return;
      }

      syncClubFolderFromMe()
        .then(function (next) {
          openResolved(applyClubQueryToSrc(src, next.cid, next.cname));
        })
        .catch(function () {
          openResolved(applyClubQueryToSrc(src, r.cid, r.cname));
        });
    });
  });

  refreshStudentPaymentLinkWithClub();
}
