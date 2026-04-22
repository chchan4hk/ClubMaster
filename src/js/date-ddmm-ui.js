/**
 * DD/MM/YYYY text fields: auto-insert "/" after day and month digits as the user types.
 * Targets `input.date-field-ddmm` (not readonly/disabled). Uses `/js/api.js` when present for blur normalisation.
 */
(function () {
  function digitsOnly(s) {
    return String(s || "").replace(/\D/g, "");
  }

  function formatDdMmYyyyFromDigits(digits) {
    var d = digits.slice(0, 8);
    if (d.length <= 2) {
      return d;
    }
    if (d.length <= 4) {
      return d.slice(0, 2) + "/" + d.slice(2);
    }
    return d.slice(0, 2) + "/" + d.slice(2, 4) + "/" + d.slice(4);
  }

  function digitCountBeforeCaret(value, caret) {
    return digitsOnly(String(value || "").slice(0, Math.max(0, caret))).length;
  }

  function caretAfterDigitIndex(formatted, digitIndex) {
    var di = 0;
    var i = 0;
    for (; i < formatted.length; i++) {
      if (/\d/.test(formatted[i])) {
        di++;
        if (di >= digitIndex) {
          return i + 1;
        }
      }
    }
    return formatted.length;
  }

  function parseDdMmToYmd(raw) {
    if (window.api && typeof window.api.parseDdMmYyyyToYmd === "function") {
      return window.api.parseDdMmYyyyToYmd(raw) || "";
    }
    var t = String(raw || "").trim();
    var m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(t);
    if (!m) {
      return "";
    }
    var day = Number(m[1]);
    var month = Number(m[2]);
    var year = Number(m[3]);
    if (!Number.isFinite(day) || !Number.isFinite(month) || !Number.isFinite(year)) {
      return "";
    }
    var dt = new Date(year, month - 1, day);
    if (
      dt.getFullYear() !== year ||
      dt.getMonth() !== month - 1 ||
      dt.getDate() !== day
    ) {
      return "";
    }
    var mm = String(month).padStart(2, "0");
    var dd = String(day).padStart(2, "0");
    return year + "-" + mm + "-" + dd;
  }

  function formatYmdToDdMm(ymd) {
    if (window.api && typeof window.api.formatDateDisplayDdMmYyyy === "function") {
      return window.api.formatDateDisplayDdMmYyyy(ymd);
    }
    var t = String(ymd || "").trim();
    var iso = /^(\d{4})-(\d{2})-(\d{2})(?:$|[T\s])/.exec(t);
    if (!iso) {
      return "";
    }
    return iso[3] + "/" + iso[2] + "/" + iso[1];
  }

  function wireAutoSlash(textEl) {
    textEl.addEventListener("input", function () {
      var val = textEl.value;
      var caret = textEl.selectionStart != null ? textEl.selectionStart : val.length;
      var digitGoal = digitCountBeforeCaret(val, caret);
      var newDigits = digitsOnly(val).slice(0, 8);
      var formatted = formatDdMmYyyyFromDigits(newDigits);
      if (formatted === val) {
        return;
      }
      textEl.value = formatted;
      var newCaret = caretAfterDigitIndex(formatted, Math.min(digitGoal, newDigits.length));
      try {
        textEl.setSelectionRange(newCaret, newCaret);
      } catch (e) {
        /* ignore */
      }
    });
  }

  function wireBlurNormalize(textEl) {
    textEl.addEventListener("blur", function () {
      var ymd = parseDdMmToYmd(textEl.value.trim());
      if (ymd) {
        textEl.value = formatYmdToDdMm(ymd);
      }
    });
  }

  function enhanceOne(textEl) {
    if (!textEl || textEl.tagName !== "INPUT") {
      return;
    }
    if (textEl.dataset.ddmmUi === "1") {
      return;
    }
    if (textEl.readOnly || textEl.disabled) {
      return;
    }
    textEl.dataset.ddmmUi = "1";
    textEl.setAttribute("maxlength", "10");
    textEl.setAttribute("inputmode", "numeric");
    textEl.setAttribute("placeholder", "DD/MM/YYYY");

    wireAutoSlash(textEl);
    wireBlurNormalize(textEl);
  }

  function initDateFieldDdMmYyyy(root) {
    var scope = root && root.querySelectorAll ? root : document;
    var nodes = scope.querySelectorAll
      ? scope.querySelectorAll("input.date-field-ddmm")
      : [];
    for (var i = 0; i < nodes.length; i++) {
      enhanceOne(nodes[i]);
    }
  }

  window.initDateFieldDdMmYyyy = initDateFieldDdMmYyyy;

  function boot() {
    initDateFieldDdMmYyyy(document);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
