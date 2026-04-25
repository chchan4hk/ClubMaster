/**
 * Shared dashboard boot: guard role, logout, optional /me (never logs out on /me failure).
 */
window.dashboardInit = function dashboardInit(expectedRole, shortRoleLabel) {
  if (!window.auth.guardPage(expectedRole)) {
    return false;
  }

  document.getElementById("logoutBtn")?.addEventListener("click", function () {
    window.auth.logout();
  });

  var welcome = document.getElementById("welcome");
  function setFallback() {
    if (welcome) {
      welcome.textContent =
        shortRoleLabel +
        " · signed in — profile refresh skipped (sign out and back in if something misbehaves)";
    }
    var roleHeading = document.getElementById("cmRoleLabel");
    if (roleHeading && shortRoleLabel) {
      roleHeading.textContent = shortRoleLabel;
    }
  }

  window.api
    .api("/me")
    .then(function (data) {
      if (data && data.user) {
        var theme =
          data.user.club_theme != null && String(data.user.club_theme).trim() !== ""
            ? String(data.user.club_theme).trim()
            : "";
        if (theme) {
          try {
            sessionStorage.setItem("sportCoach.clubTheme", theme);
          } catch {
            /* ignore storage errors */
          }
        } else {
          try {
            sessionStorage.removeItem("sportCoach.clubTheme");
          } catch {
            /* ignore storage errors */
          }
        }
        var dockAny = document.getElementById("clubDock");
        if (dockAny && dockAny.dataset) {
          if (theme) {
            dockAny.dataset.clubTheme = theme;
          } else {
            delete dockAny.dataset.clubTheme;
          }
        }
      }
      if (welcome && data.user) {
        var typeLabel =
          data.user.usertype != null && String(data.user.usertype).trim() !== ""
            ? String(data.user.usertype).trim()
            : data.user.role;
        var un =
          data.user.username != null && String(data.user.username).trim() !== ""
            ? String(data.user.username).trim()
            : "";
        welcome.textContent = un ? "User Account · " + un : "User Account";
        var roleHeading = document.getElementById("cmRoleLabel");
        if (roleHeading) {
          roleHeading.textContent = typeLabel || String(data.user.role || "").trim();
        }
      }
      if (
        data.user &&
        data.user.role === "CoachManager" &&
        data.user.uid != null &&
        String(data.user.uid).trim() !== "" &&
        typeof window.api.setClubFolderContext === "function"
      ) {
        var cn =
          data.user.club_name != null &&
          String(data.user.club_name).trim() !== "" &&
          data.user.club_name !== "—"
            ? String(data.user.club_name).trim()
            : null;
        var uidStr = String(data.user.uid).trim();
        window.api.setClubFolderContext(uidStr, cn);
        var dock = document.getElementById("clubDock");
        if (dock && dock.dataset) {
          dock.dataset.clubId = uidStr;
          if (cn) {
            dock.dataset.clubName = cn;
          } else {
            delete dock.dataset.clubName;
          }
        }
      }
      if (
        data.user &&
        data.user.role === "Coach" &&
        typeof window.api.setClubFolderContext === "function"
      ) {
        var cfu =
          data.user.club_folder_uid != null &&
          String(data.user.club_folder_uid).trim() !== ""
            ? String(data.user.club_folder_uid).trim()
            : "";
        if (cfu) {
          var cnCoach =
            data.user.club_name != null &&
            String(data.user.club_name).trim() !== "" &&
            data.user.club_name !== "—"
              ? String(data.user.club_name).trim()
              : null;
          window.api.setClubFolderContext(cfu, cnCoach);
          var dockCoach = document.getElementById("clubDock");
          if (dockCoach && dockCoach.dataset) {
            dockCoach.dataset.clubId = cfu;
            if (cnCoach) {
              dockCoach.dataset.clubName = cnCoach;
            } else {
              delete dockCoach.dataset.clubName;
            }
          }
        }
      }
      if (
        data.user &&
        data.user.role === "Student" &&
        typeof window.api.setClubFolderContext === "function"
      ) {
        var cfuSt =
          data.user.club_folder_uid != null &&
          String(data.user.club_folder_uid).trim() !== ""
            ? String(data.user.club_folder_uid).trim()
            : "";
        if (cfuSt) {
          var cnSt =
            data.user.club_name != null &&
            String(data.user.club_name).trim() !== "" &&
            data.user.club_name !== "—"
              ? String(data.user.club_name).trim()
              : null;
          window.api.setClubFolderContext(cfuSt, cnSt);
        }
      }
    })
    .catch(function () {
      setFallback();
    });

  return true;
};
