(function attachUrlUtilities(global) {
  const TRACKING_PARAMS = new Set([
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "utm_id",
    "utm_name", "utm_creative_format", "utm_marketing_tactic", "gclid", "dclid", "gbraid",
    "wbraid", "fbclid", "msclkid", "mc_cid", "mc_eid", "igshid", "yclid", "_hsenc", "_hsmi", "vero_id"
  ]);

  const SEARCH_REDIRECT_HOSTS = new Set([
    "www.google.com", "google.com", "www.google.co.jp", "google.co.jp", "www.bing.com", "bing.com"
  ]);
  const LOCAL_HOSTNAMES = new Set(["localhost", "localhost.localdomain", "0.0.0.0"]);
  const LOCAL_DOMAIN_SUFFIXES = [".localhost", ".local", ".test", ".invalid", ".internal", ".lan", ".home"];

  function isTrackingParam(name) {
    const normalized = String(name || "").trim().toLowerCase();
    return normalized.startsWith("utm_") || TRACKING_PARAMS.has(normalized);
  }

  function decodeBingRedirectTarget(value) {
    if (!value) return null;
    if (/^https?:\/\//i.test(value)) return value;
    const encoded = value.startsWith("a1") ? value.slice(2) : value;
    try {
      const base64 = encoded.replace(/-/g, "+").replace(/_/g, "/").padEnd(Math.ceil(encoded.length / 4) * 4, "=");
      const decoded = typeof global.atob === "function"
        ? global.atob(base64)
        : typeof global.Buffer !== "undefined"
          ? global.Buffer.from(base64, "base64").toString("utf8")
          : "";
      return /^https?:\/\//i.test(decoded) ? decoded : null;
    } catch (_error) {
      return null;
    }
  }

  function unwrapSearchRedirect(rawUrl, baseUrl) {
    try {
      const url = new URL(rawUrl, baseUrl || global.location?.href);
      const host = url.hostname.toLowerCase();
      if (SEARCH_REDIRECT_HOSTS.has(host) && url.pathname === "/url") {
        return url.searchParams.get("q") || url.searchParams.get("url") || url.href;
      }
      if (SEARCH_REDIRECT_HOSTS.has(host) && url.pathname === "/ck/a") {
        return decodeBingRedirectTarget(url.searchParams.get("u")) || url.href;
      }
      return url.href;
    } catch (_error) {
      return rawUrl;
    }
  }

  function normalizePath(pathname) {
    return pathname ? pathname.replace(/\/{2,}/g, "/") : "/";
  }

  function isPrivateIpv4(hostname) {
    const parts = hostname.split(".");
    if (parts.length !== 4 || parts.some((part) => !/^\d+$/.test(part))) return false;
    const octets = parts.map(Number);
    if (octets.some((octet) => octet < 0 || octet > 255)) return false;
    return (
      octets[0] === 10 || octets[0] === 127 ||
      (octets[0] === 172 && octets[1] >= 16 && octets[1] <= 31) ||
      (octets[0] === 192 && octets[1] === 168) ||
      (octets[0] === 169 && octets[1] === 254) ||
      (octets[0] === 100 && octets[1] >= 64 && octets[1] <= 127) ||
      octets[0] === 0
    );
  }

  function isLocalOrPrivateHostname(hostname) {
    const normalized = String(hostname || "").replace(/^\[|\]$/g, "").toLowerCase().replace(/\.$/, "");
    if (!normalized) return true;
    if (LOCAL_HOSTNAMES.has(normalized) || LOCAL_DOMAIN_SUFFIXES.some((suffix) => normalized.endsWith(suffix))) return true;
    if (normalized === "::1" || normalized.startsWith("fc") || normalized.startsWith("fd") || normalized.startsWith("fe80")) return true;
    return isPrivateIpv4(normalized);
  }

  function cleanUrl(rawUrl, baseUrl) {
    if (!rawUrl || typeof rawUrl !== "string") return null;
    let parsed;
    try {
      parsed = new URL(unwrapSearchRedirect(rawUrl.trim(), baseUrl), baseUrl || global.location?.href);
    } catch (_error) {
      return null;
    }
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") return null;

    parsed.hash = "";
    parsed.protocol = parsed.protocol.toLowerCase();
    parsed.hostname = parsed.hostname.toLowerCase();
    if (isLocalOrPrivateHostname(parsed.hostname)) return null;
    parsed.username = "";
    parsed.password = "";
    if ((parsed.protocol === "http:" && parsed.port === "80") || (parsed.protocol === "https:" && parsed.port === "443")) parsed.port = "";
    parsed.pathname = normalizePath(parsed.pathname);

    const keptParams = [];
    for (const [key, value] of parsed.searchParams.entries()) {
      if (!isTrackingParam(key)) keptParams.push([key, value]);
    }
    keptParams.sort(([lk, lv], [rk, rv]) => lk.localeCompare(rk) || lv.localeCompare(rv));
    parsed.search = "";
    for (const [key, value] of keptParams) parsed.searchParams.append(key, value);

    return { domain: parsed.hostname, url: parsed.href };
  }

  global.DDCShared = Object.assign(global.DDCShared || {}, {
    cleanUrl,
    isLocalOrPrivateHostname,
    isTrackingParam,
    TRACKING_PARAMS
  });
})(globalThis);
