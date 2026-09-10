(function collectForDdc() {
  const MAX_PAGE_TEXT_LENGTH = 120000;
  const MAX_SNIPPET_LENGTH = 1200;
  const SEARCH_OBSERVER_LIFETIME_MS = 30000;
  const EXCLUDED_TEXT_TAGS = new Set([
    "SCRIPT",
    "STYLE",
    "NOSCRIPT",
    "TEMPLATE",
    "SVG",
    "CANVAS",
    "INPUT",
    "TEXTAREA",
    "SELECT",
    "OPTION"
  ]);
  const PRIVATE_PAGE_HOSTS = [
    "app.slack.com",
    "bard.google.com",
    "calendar.google.com",
    "chat.openai.com",
    "chat.qwen.ai",
    "chatgpt.com",
    "claude.ai",
    "copilot.microsoft.com",
    "discord.com",
    "docs.google.com",
    "drive.google.com",
    "dropbox.com",
    "figma.com",
    "github.com",
    "gitlab.com",
    "gemini.google.com",
    "huggingface.co",
    "icloud.com",
    "linear.app",
    "mail.google.com",
    "miro.com",
    "notion.so",
    "perplexity.ai",
    "poe.com",
    "qwen.ai",
    "trello.com",
    "www.notion.so"
  ];

  let searchObserver = null;
  let searchDebounceTimer = 0;
  let lastSearchSignature = "";
  let pageCollectedForUrl = "";

  function isGoogleSearchPage() {
    return /(^|\.)google\./i.test(location.hostname) && location.pathname === "/search";
  }

  function isBingSearchPage() {
    return /(^|\.)bing\.com$/i.test(location.hostname) && location.pathname === "/search";
  }

  function isDuckDuckGoSearchPage() {
    return /(^|\.)duckduckgo\.com$/i.test(location.hostname) && (location.pathname === "/" || location.pathname === "/html/");
  }

  function isSearchPage() {
    return isGoogleSearchPage() || isBingSearchPage() || isDuckDuckGoSearchPage();
  }

  function requestIdle(callback, timeout) {
    if (typeof window.requestIdleCallback === "function") {
      window.requestIdleCallback(callback, { timeout });
      return;
    }
    window.setTimeout(callback, Math.min(timeout || 1000, 1000));
  }

  function sendItems(items) {
    if (!items.length || typeof chrome === "undefined" || !chrome.runtime?.id) return;
    chrome.runtime.sendMessage({ type: "DDC_COLLECTED_ITEMS", items });
  }

  function compactText(value, maxLength) {
    return String(value || "").replace(/\s+/g, " ").trim().slice(0, maxLength);
  }

  function closestTextBlock(element) {
    return element?.closest("article, li, div, section") || element?.parentElement || document.body;
  }

  function itemFromAnchor(anchor, block) {
    const cleaned = DDCShared.cleanUrl(anchor.href, location.href);
    if (!cleaned || cleaned.domain === location.hostname.toLowerCase()) return null;

    const title = compactText(anchor.innerText || anchor.textContent || "", 300);
    const blockText = compactText(block?.innerText || block?.textContent || "", MAX_SNIPPET_LENGTH);
    const snippet = compactText(blockText.replace(title, ""), MAX_SNIPPET_LENGTH);
    if (!title && !snippet) return null;

    return { source: "search_result", url: cleaned.url, title, snippet };
  }

  function dedupeItems(items) {
    const byUrl = new Map();
    for (const item of items) {
      if (!item?.url || byUrl.has(item.url)) continue;
      byUrl.set(item.url, item);
    }
    return Array.from(byUrl.values());
  }

  function collectGoogleResults() {
    const items = [];
    const headings = document.querySelectorAll("a[href] h3");
    for (const heading of headings) {
      const anchor = heading.closest("a[href]");
      const block = anchor?.closest("div.g, div.MjjYud, div[data-sokoban-container]") || closestTextBlock(anchor);
      const item = anchor ? itemFromAnchor(anchor, block) : null;
      if (item) items.push(item);
    }
    return dedupeItems(items);
  }

  function collectBingResults() {
    const items = [];
    const blocks = document.querySelectorAll("li.b_algo, .b_algo");
    for (const block of blocks) {
      const anchor = block.querySelector("h2 a[href], a[href]");
      const item = anchor ? itemFromAnchor(anchor, block) : null;
      if (item) items.push(item);
    }
    return dedupeItems(items);
  }

  function collectDuckDuckGoResults() {
    const items = [];
    const anchors = document.querySelectorAll('a[data-testid="result-title-a"], a.result__a, article a[href]');
    for (const anchor of anchors) {
      const block = anchor.closest("article, .result") || closestTextBlock(anchor);
      const item = itemFromAnchor(anchor, block);
      if (item) items.push(item);
    }
    return dedupeItems(items);
  }

  function collectSearchResults() {
    if (isGoogleSearchPage()) return collectGoogleResults();
    if (isBingSearchPage()) return collectBingResults();
    if (isDuckDuckGoSearchPage()) return collectDuckDuckGoResults();
    return [];
  }

  function hasSensitivePasswordField() {
    return Boolean(document.querySelector('input[type="password"]'));
  }

  function isPrivatePageHost(hostname) {
    const normalized = String(hostname || "").toLowerCase().replace(/\.$/, "");
    return PRIVATE_PAGE_HOSTS.some((host) => normalized === host || normalized.endsWith(`.${host}`));
  }

  function hasNoindexDirective() {
    return Array.from(document.querySelectorAll('meta[name="robots" i], meta[name="googlebot" i]')).some((meta) =>
      /\bnoindex\b/i.test(meta.getAttribute("content") || "")
    );
  }

  function hasAuthenticatedAppSignals() {
    if (document.querySelector('a[href*="logout" i], a[href*="signout" i], form[action*="logout" i], form[action*="signout" i]')) {
      return true;
    }
    const cookieNames = document.cookie
      .split(";")
      .map((cookie) => cookie.split("=", 1)[0].trim().toLowerCase())
      .filter(Boolean);
    return cookieNames.some((name) => /(session|auth|token|jwt|login|user|sid)/.test(name));
  }

  function shouldCollectPageText(cleaned) {
    if (!cleaned || hasSensitivePasswordField() || hasNoindexDirective()) return false;
    if (DDCShared.isLocalOrPrivateHostname(cleaned.domain) || isPrivatePageHost(cleaned.domain)) return false;
    return !hasAuthenticatedAppSignals();
  }

  function isElementAllowed(element) {
    if (!element || EXCLUDED_TEXT_TAGS.has(element.tagName)) return false;
    if (element.closest("script, style, noscript, template, svg, canvas, input, textarea, select, option")) return false;
    const style = window.getComputedStyle(element);
    if (style.display === "none" || style.visibility === "hidden" || style.visibility === "collapse" || style.opacity === "0") return false;
    return element.getClientRects().length > 0;
  }

  function extractVisibleBodyText() {
    if (!document.body) return "";
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        const value = node.nodeValue;
        if (!value || !value.trim()) return NodeFilter.FILTER_REJECT;
        return isElementAllowed(node.parentElement) ? NodeFilter.FILTER_ACCEPT : NodeFilter.FILTER_REJECT;
      }
    });

    const parts = [];
    let totalLength = 0;
    let current = walker.nextNode();
    while (current && totalLength < MAX_PAGE_TEXT_LENGTH) {
      const text = compactText(current.nodeValue, 1000);
      if (text) {
        parts.push(text);
        totalLength += text.length + 1;
      }
      current = walker.nextNode();
    }
    return parts.join(" ").slice(0, MAX_PAGE_TEXT_LENGTH);
  }

  function collectPageItem() {
    const cleaned = DDCShared.cleanUrl(location.href);
    if (!shouldCollectPageText(cleaned)) return null;
    const text = extractVisibleBodyText();
    if (!text) return null;
    return {
      source: "page",
      url: cleaned.url,
      title: compactText(document.title, 300),
      text
    };
  }

  function scheduleSearchCollection() {
    window.clearTimeout(searchDebounceTimer);
    searchDebounceTimer = window.setTimeout(() => {
      requestIdle(() => {
        const items = collectSearchResults();
        const signature = items.map((item) => item.url).join("|");
        if (signature && signature !== lastSearchSignature) {
          lastSearchSignature = signature;
          sendItems(items);
        }
      }, 1500);
    }, 500);
  }

  function schedulePageCollection() {
    if (pageCollectedForUrl === location.href || document.visibilityState === "hidden" || isSearchPage()) return;
    pageCollectedForUrl = location.href;
    requestIdle(() => {
      const item = collectPageItem();
      if (item) sendItems([item]);
    }, 3000);
  }

  function startSearchObserver() {
    if (!document.body || searchObserver) return;
    searchObserver = new MutationObserver(scheduleSearchCollection);
    searchObserver.observe(document.body, { childList: true, subtree: true });
    window.setTimeout(() => {
      searchObserver?.disconnect();
      searchObserver = null;
    }, SEARCH_OBSERVER_LIFETIME_MS);
  }

  function start() {
    if (isSearchPage()) {
      scheduleSearchCollection();
      startSearchObserver();
      return;
    }
    schedulePageCollection();
  }

  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") start();
  }, { passive: true });

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start, { once: true });
  } else {
    start();
  }
})();
