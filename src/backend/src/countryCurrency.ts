/**
 * Display currency for a club from ClubInfo `country` (BasicInfo country names).
 * Unknown / empty country defaults to HKD (legacy behaviour).
 */
export type ClubCurrency = {
  currencyCode: string;
  currencySymbol: string;
};

function normCountry(s: string): string {
  return s
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[.,]+$/g, "");
}

type Rule = { match: (n: string) => boolean; code: string; symbol: string };

const RULES: Rule[] = [
  {
    match: (n) =>
      n.includes("hong kong") || n === "hk" || n.includes("hongkong"),
    code: "HKD",
    symbol: "$",
  },
  {
    match: (n) =>
      n.includes("united kingdom") ||
      n.includes("great britain") ||
      n.includes("england") ||
      n.includes("scotland") ||
      n.includes("wales") ||
      n.includes("northern ireland") ||
      n === "uk" ||
      n === "gb",
    code: "GBP",
    symbol: "£",
  },
  {
    match: (n) =>
      n.includes("united states") ||
      n.includes("u.s.") ||
      n.includes("usa") ||
      n === "us",
    code: "USD",
    symbol: "$",
  },
  {
    match: (n) =>
      n.includes("china") ||
      n.includes("prc") ||
      n.includes("people's republic of china"),
    code: "CNY",
    symbol: "¥",
  },
  {
    match: (n) => n.includes("japan"),
    code: "JPY",
    symbol: "¥",
  },
  {
    match: (n) => n.includes("singapore"),
    code: "SGD",
    symbol: "$",
  },
  {
    match: (n) => n.includes("australia"),
    code: "AUD",
    symbol: "$",
  },
  {
    match: (n) => n.includes("canada"),
    code: "CAD",
    symbol: "$",
  },
  {
    match: (n) =>
      n.includes("new zealand") || n === "nz" || n.includes("newzealand"),
    code: "NZD",
    symbol: "$",
  },
  {
    match: (n) =>
      n.includes("euro") ||
      n.includes("germany") ||
      n.includes("france") ||
      n.includes("italy") ||
      n.includes("spain") ||
      n.includes("netherlands") ||
      n.includes("belgium") ||
      n.includes("austria") ||
      n.includes("ireland") ||
      n.includes("portugal") ||
      n.includes("finland") ||
      n.includes("greece"),
    code: "EUR",
    symbol: "€",
  },
];

const DEFAULT: ClubCurrency = { currencyCode: "HKD", currencySymbol: "$" };

export function clubCurrencyFromCountry(countryRaw: string): ClubCurrency {
  const n = normCountry(countryRaw);
  if (!n) {
    return { ...DEFAULT };
  }
  for (const r of RULES) {
    if (r.match(n)) {
      return { currencyCode: r.code, currencySymbol: r.symbol };
    }
  }
  return { ...DEFAULT };
}
