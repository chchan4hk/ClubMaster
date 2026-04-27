/**
 * Shared dashboard boot: guard role, logout, /me hydration (never logs out on /me failure).
 *
 * Workarounds for slow first paint:
 * - Apply last /me snapshot from sessionStorage when role matches (short TTL).
 * - Defer the network /me until after first paint (rAF) so CSS/layout are not competing.
 * - Expose dashboardWaitMe() so coach.js / student.js reuse the same in-flight /me promise.
 */
(function () {
  var ME_CACHE_KEY = "sportCoach.dashboardMeCache";
  var ME_CACHE_TTL_MS = 15 * 60 * 1000;

  function readMeCache(expectedRole) {
    try {
      var raw = sessionStorage.getItem(ME_CACHE_KEY);
      if (!raw) {
        return null;
      }
      var o = JSON.parse(raw);
      if (!o || o.v !== 1 || o.role !== expectedRole) {
        return null;
      }
      if (typeof o.at !== "number" || Date.now() - o.at > ME_CACHE_TTL_MS) {
        return null;
      }
      return o.payload;
    } catch {
      return null;
    }
  }

  function writeMeCache(expectedRole, payload) {
    try {
      sessionStorage.setItem(
        ME_CACHE_KEY,
        JSON.stringify({
          v: 1,
          role: expectedRole,
          at: Date.now(),
          payload: payload,
        }),
      );
    } catch {
      /* quota / private mode */
    }
  }

  function applyMePayload(data) {
    var welcome = document.getElementById("welcome");
    if (data && data.user) {
      var theme =
        data.user.club_theme != null && String(data.user.club_theme).trim() !== ""
          ? String(data.user.club_theme).trim()
          : "";
      if (theme) {
        try {
          sessionStorage.setItem("sportCoach.clubTheme", theme);
        } catch {
          /* ignore */
        }
      } else {
        try {
          sessionStorage.removeItem("sportCoach.clubTheme");
        } catch {
          /* ignore */
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
    if (welcome && data && data.user) {
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
      data &&
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
      data &&
      data.user &&
      data.user.role === "Coach" &&
      typeof window.api.setClubFolderContext === "function"
    ) {
      var cfu =
        data.user.club_folder_uid != null && String(data.user.club_folder_uid).trim() !== ""
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
      data &&
      data.user &&
      data.user.role === "Student" &&
      typeof window.api.setClubFolderContext === "function"
    ) {
      var cfuSt =
        data.user.club_folder_uid != null && String(data.user.club_folder_uid).trim() !== ""
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
  }

  function scheduleAfterFirstPaint(fn) {
    if (typeof requestAnimationFrame === "function") {
      requestAnimationFrame(function () {
        requestAnimationFrame(fn);
      });
    } else {
      setTimeout(fn, 0);
    }
  }

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

    var mePromise = null;

    window.dashboardWaitMe = function dashboardWaitMe() {
      if (!mePromise) {
        mePromise = window.api
          .api("/me")
          .then(function (data) {
            writeMeCache(expectedRole, data);
            applyMePayload(data);
            return data;
          })
          .catch(function () {
            mePromise = null;
            setFallback();
            return Promise.reject(new Error("/me failed"));
          });
      }
      return mePromise;
    };

    var cached = readMeCache(expectedRole);
    if (cached) {
      applyMePayload(cached);
    }

    scheduleAfterFirstPaint(function () {
      void window.dashboardWaitMe().catch(function () {
        /* setFallback already ran */
      });
    });

    return true;
  };
})();
