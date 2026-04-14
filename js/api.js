/**
 * API base URL.
 * - Same host when you open the site via the Node server (recommended).
 * - If you open HTML as file://, defaults to http://127.0.0.1:3000/api
 * - Override: localStorage.setItem("API_ORIGIN", "http://127.0.0.1:3000")
 */
function getApiBase() {
  const custom = localStorage.getItem("API_ORIGIN");
  if (custom) {
    let t = custom.replace(/\/$/, "");
    if (/\/api$/i.test(t)) {
      return t;
    }
    return `${t}/api`;
  }
  const { protocol, hostname } = window.location;
  if (protocol === "file:" || hostname === "" || hostname === "null") {
    return "http://127.0.0.1:3000/api";
  }
  return `${window.location.origin}/api`;
}

/**
 * Origin where the Node app serves HTML and /backend/data_club/… (no /api suffix).
 * Use for image and iframe URLs when the page origin may differ (file://, Live Server, API_ORIGIN).
 */
function getAppOrigin() {
  const base = getApiBase();
  const stripped = base.replace(/\/api\/?$/i, "");
  if (stripped) {
    return stripped;
  }
  const { protocol, hostname } = window.location;
  if (protocol === "file:" || hostname === "" || hostname === "null") {
    return "http://127.0.0.1:3000";
  }
  return window.location.origin;
}

/** Initial snapshot; prefer getApiBase() per request (localStorage may change). */
const API_BASE = getApiBase();

/** sessionStorage: ClubID (= manager UID, e.g. C0001) → folder under backend/data_club. */
const CLUB_FOLDER_ID_KEY = "sportCoach.clubFolderId";
const CLUB_FOLDER_NAME_KEY = "sportCoach.clubFolderName";

/**
 * Cache ClubID (= coach manager UID / JWT sub, same value, e.g. C0001).
 * Right-panel iframes read this for paths under backend/data_club/{ClubID}/.
 * APIs still use JWT — this is for client-side paths and display only.
 */
function setClubFolderContext(clubId, clubName) {
  try {
    if (clubId != null && String(clubId).trim() !== "") {
      sessionStorage.setItem(CLUB_FOLDER_ID_KEY, String(clubId).trim());
    } else {
      sessionStorage.removeItem(CLUB_FOLDER_ID_KEY);
    }
    if (
      clubName != null &&
      String(clubName).trim() !== "" &&
      String(clubName).trim() !== "—"
    ) {
      sessionStorage.setItem(CLUB_FOLDER_NAME_KEY, String(clubName).trim());
    } else {
      sessionStorage.removeItem(CLUB_FOLDER_NAME_KEY);
    }
  } catch {
    /* private mode / quota */
  }
}

function getClubFolderContext() {
  try {
    const id = sessionStorage.getItem(CLUB_FOLDER_ID_KEY);
    const name = sessionStorage.getItem(CLUB_FOLDER_NAME_KEY);
    return {
      clubId: id && id.trim() !== "" ? id.trim() : null,
      clubName: name && name.trim() !== "" ? name.trim() : null,
    };
  } catch {
    return { clubId: null, clubName: null };
  }
}

function clearClubFolderContext() {
  try {
    sessionStorage.removeItem(CLUB_FOLDER_ID_KEY);
    sessionStorage.removeItem(CLUB_FOLDER_NAME_KEY);
  } catch {
    /* ignore */
  }
}

/**
 * Same-origin URL for a file inside backend/data_club/{ClubID}/ (ClubID = UID).
 * @param {string} relPath - e.g. "UserList_Coach.json" or "image/photo.jpg"
 * @returns {string | null}
 */
function clubDataFileUrl(relPath) {
  const { clubId } = getClubFolderContext();
  if (!clubId) {
    return null;
  }
  const rel = String(relPath || "")
    .replace(/^[\\/]+/, "")
    .split(/[/\\]/)
    .filter(Boolean)
    .map(encodeURIComponent)
    .join("/");
  if (!rel) {
    return null;
  }
  return `${getAppOrigin()}/backend/data_club/${encodeURIComponent(clubId)}/${rel}`;
}

/**
 * @param {string} path - e.g. "/auth/login" (leading slash optional)
 * @param {RequestInit} [options]
 */
async function api(path, options = {}) {
  let p = path.startsWith("/") ? path : `/${path}`;
  if (p.startsWith("/api/") || p === "/api") {
    p = p.replace(/^\/api/, "") || "/";
  }
  const url = `${getApiBase()}${p}`;
  const token = localStorage.getItem("token");
  const headers = {
    "Content-Type": "application/json",
    ...(options.headers || {}),
  };
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  let res;
  try {
    res = await fetch(url, { ...options, headers, credentials: "include" });
  } catch (e) {
    const hint =
      "Cannot reach API. Start the backend (cd backend → npm run dev) and use " +
      "http://127.0.0.1:3000/main.html — do not open the HTML file directly from disk.";
    const err = new Error(
      e instanceof TypeError ? `${e.message}. ${hint}` : String(e)
    );
    err.cause = e;
    throw err;
  }
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(data.error || res.statusText || "Request failed");
    err.status = res.status;
    err.data = data;
    throw err;
  }
  return data;
}

/** @type {{ countries: string[]; sportTypes: string[] } | null} */
let basicInfoCache = null;

/**
 * Same key rules as backend basicInfoCsv.ts (SportType / Country columns).
 * @param {string} text
 * @returns {{ countries: string[]; sportTypes: string[] }}
 */
function parseBasicInfoCsvText(text) {
  const countries = [];
  const sportTypes = [];
  const seenC = new Set();
  const seenS = new Set();
  const body = String(text || "").replace(/^\uFEFF/, "");
  for (const line of body.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const comma = trimmed.indexOf(",");
    if (comma < 0) {
      continue;
    }
    const rawKey = trimmed.slice(0, comma).trim();
    const val = trimmed.slice(comma + 1).trim();
    if (!rawKey || !val) {
      continue;
    }
    const compact = rawKey
      .toLowerCase()
      .replace(/[\s_]+/g, "");
    if (compact === "sporttype" && !seenS.has(val)) {
      seenS.add(val);
      sportTypes.push(val);
    } else if (compact === "country" && !seenC.has(val)) {
      seenC.add(val);
      countries.push(val);
    }
  }
  return { countries, sportTypes };
}

/**
 * Reference lists from backend/data/BasicInfo.csv.
 * Uses GET /api/basic-info, or falls back to fetching the CSV from the static server if the API fails.
 */
async function fetchBasicInfo() {
  if (basicInfoCache) {
    return basicInfoCache;
  }
  try {
    const d = await api("/basic-info");
    basicInfoCache = {
      countries: Array.isArray(d.countries) ? d.countries : [],
      sportTypes: Array.isArray(d.sportTypes) ? d.sportTypes : [],
    };
    return basicInfoCache;
  } catch {
    try {
      const origin = getAppOrigin();
      const res = await fetch(`${origin}/backend/data/BasicInfo.csv`, {
        credentials: "include",
      });
      if (!res.ok) {
        throw new Error(String(res.status));
      }
      const text = await res.text();
      basicInfoCache = parseBasicInfoCsvText(text);
      return basicInfoCache;
    } catch {
      basicInfoCache = { countries: [], sportTypes: [] };
      return basicInfoCache;
    }
  }
}

function clearBasicInfoCache() {
  basicInfoCache = null;
}

/**
 * @param {HTMLSelectElement | null} sel
 * @param {string[]} values
 * @param {{ leadEmpty?: boolean; leadEmptyLabel?: string; selectedValue?: string | null }} [opts]
 */
function fillSelectFromBasicList(sel, values, opts) {
  if (!sel) {
    return;
  }
  opts = opts || {};
  sel.innerHTML = "";
  if (opts.leadEmpty) {
    const o0 = document.createElement("option");
    o0.value = "";
    o0.textContent =
      opts.leadEmptyLabel != null ? String(opts.leadEmptyLabel) : "—";
    sel.appendChild(o0);
  }
  (values || []).forEach(function (v) {
    const t = String(v || "").trim();
    if (!t) {
      return;
    }
    const o = document.createElement("option");
    o.value = t;
    o.textContent = t;
    sel.appendChild(o);
  });
  const want = opts.selectedValue != null ? String(opts.selectedValue).trim() : "";
  if (want) {
    sel.value = want;
  } else if (!opts.leadEmpty && sel.options.length) {
    sel.selectedIndex = 0;
  }
}

window.api = {
  api,
  getApiBase,
  getAppOrigin,
  API_BASE,
  setClubFolderContext,
  getClubFolderContext,
  clearClubFolderContext,
  clubDataFileUrl,
  fetchBasicInfo,
  clearBasicInfoCache,
  fillSelectFromBasicList,
};
