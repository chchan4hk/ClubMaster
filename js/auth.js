const TOKEN_KEY = "token";
const ROLE_KEY = "userRole";
const USER_TYPE_KEY = "userType";

const ROLE_PAGE = {
  Admin: "/admin.html",
  CoachManager: "/coach-manager.html",
  Coach: "/coach.html",
  Student: "/student.html",
};

/** main.html `<select>` values → dashboard (used after login-with-context). */
const FORM_ROLE_TO_PAGE = {
  Admin: "/admin.html",
  "Coach Manager": "/coach-manager.html",
  Coach: "/coach.html",
  Student: "/student.html",
};

/** Normalized CSV/API usertype → dashboard (matches backend mapUserTypeToRole inputs). */
const USERTYPE_PAGE = {
  administrator: "/admin.html",
  "coach manager": "/coach-manager.html",
  coach: "/coach.html",
  student: "/student.html",
};

function normalizeUserType(s) {
  return String(s || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

function setSession(token, role, userType) {
  localStorage.setItem(TOKEN_KEY, token);
  if (role) {
    localStorage.setItem(ROLE_KEY, role);
  }
  if (userType != null && String(userType).trim() !== "") {
    localStorage.setItem(USER_TYPE_KEY, String(userType).trim());
  }
}

function clearSession() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(ROLE_KEY);
  localStorage.removeItem(USER_TYPE_KEY);
}

/**
 * Pick dashboard URL from login/API user object (prefers userType / usertype from CSV).
 */
function resolveDashboardPage(user) {
  if (!user) {
    return null;
  }
  const raw = user.usertype != null ? user.usertype : user.userType;
  const key = normalizeUserType(raw);
  if (key && USERTYPE_PAGE[key]) {
    return USERTYPE_PAGE[key];
  }
  if (user.role && ROLE_PAGE[user.role]) {
    return ROLE_PAGE[user.role];
  }
  return null;
}

function resolveDashboardPageFromStorage() {
  const ut = localStorage.getItem(USER_TYPE_KEY);
  const key = normalizeUserType(ut);
  if (key && USERTYPE_PAGE[key]) {
    return USERTYPE_PAGE[key];
  }
  const role = localStorage.getItem(ROLE_KEY);
  if (role && ROLE_PAGE[role]) {
    return ROLE_PAGE[role];
  }
  return null;
}

function redirectForRole(role) {
  const page = role && ROLE_PAGE[role];
  if (page) {
    window.location.href = page;
  } else {
    window.location.href = "/main.html";
  }
}

function guardPage(expectedRole) {
  const token = getToken();
  if (!token) {
    window.location.href = "/main.html";
    return false;
  }
  const role = localStorage.getItem(ROLE_KEY);
  if (expectedRole && role !== expectedRole) {
    redirectForRole(role);
    return false;
  }
  return true;
}

async function login(username, password) {
  const { api } = window.api;
  const data = await api("/auth/login", {
    method: "POST",
    body: JSON.stringify({ username, password }),
  });
  if (data.accountExpired && data.user) {
    return data;
  }
  if (data.token && data.user) {
    const page = resolveDashboardPage(data.user);
    if (!page) {
      throw new Error("Unknown user type — cannot open a dashboard.");
    }
    if (
      window.api &&
      typeof window.api.setClubFolderContext === "function" &&
      data.user.club_folder_uid != null &&
      String(data.user.club_folder_uid).trim() !== ""
    ) {
      const cfu = String(data.user.club_folder_uid).trim();
      const cnRaw = data.user.club_name;
      const cn =
        cnRaw != null &&
        String(cnRaw).trim() !== "" &&
        String(cnRaw).trim() !== "—"
          ? String(cnRaw).trim()
          : null;
      window.api.setClubFolderContext(cfu, cn);
    }
    setSession(data.token, data.user.role, data.user.usertype || data.user.userType);
    window.location.href = page;
  }
  return data;
}

/**
 * Sign-in with role + optional club name (main.html). Uses POST /auth/login-with-context.
 * @param {string} username
 * @param {string} password
 * @param {string} role - "Coach Manager" | "Coach" | "Student" | "Admin"
 * @param {string} [clubName]
 */
async function loginWithContext(username, password, role, clubName) {
  const { api } = window.api;
  const data = await api("/auth/login-with-context", {
    method: "POST",
    body: JSON.stringify({
      username,
      password,
      role,
      clubName: clubName != null ? String(clubName).trim() : "",
    }),
  });
  if (data.accountExpired && data.user) {
    return data;
  }
  if (data.token && data.user) {
    const page =
      (role && FORM_ROLE_TO_PAGE[role]) || resolveDashboardPage(data.user);
    if (!page) {
      throw new Error("Unknown user type — cannot open a dashboard.");
    }
    if (
      window.api &&
      typeof window.api.setClubFolderContext === "function" &&
      data.user.club_folder_uid != null &&
      String(data.user.club_folder_uid).trim() !== ""
    ) {
      const cfu = String(data.user.club_folder_uid).trim();
      const cnRaw = data.user.club_name;
      const cn =
        cnRaw != null &&
        String(cnRaw).trim() !== "" &&
        String(cnRaw).trim() !== "—"
          ? String(cnRaw).trim()
          : null;
      window.api.setClubFolderContext(cfu, cn);
    }
    setSession(data.token, data.user.role, data.user.usertype || data.user.userType);
    window.location.href = page;
  }
  return data;
}

function logout() {
  clearSession();
  if (window.api && typeof window.api.clearClubFolderContext === "function") {
    window.api.clearClubFolderContext();
  }
  window.location.href = "/main.html";
}

window.auth = {
  getToken,
  setSession,
  clearSession,
  guardPage,
  login,
  loginWithContext,
  logout,
  redirectForRole,
  resolveDashboardPage,
  resolveDashboardPageFromStorage,
  ROLE_PAGE,
  FORM_ROLE_TO_PAGE,
  USERTYPE_PAGE,
};
