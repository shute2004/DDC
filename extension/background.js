importScripts("shared/url.js", "shared/keywords.js");

const QUEUE_KEY = "ddc_queue";
const SETTINGS_KEY = "ddc_settings";
const FLUSH_ALARM_NAME = "ddc_flush_queue";
const DEFAULT_ENDPOINT = "http://127.0.0.1:8787/ingest";
const FLUSH_INTERVAL_MINUTES = 3;
const FLUSH_THRESHOLD = 10;
const MAX_BATCH_SIZE = 50;
const MAX_QUEUE_SIZE = 500;

let flushInProgress = false;

function storageGet(keys) {
  return new Promise((resolve) => chrome.storage.local.get(keys, resolve));
}

function storageSet(values) {
  return new Promise((resolve) => chrome.storage.local.set(values, resolve));
}

function getUuid() {
  if (globalThis.crypto && typeof globalThis.crypto.randomUUID === "function") {
    return globalThis.crypto.randomUUID();
  }

  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

async function getSettings() {
  const stored = await storageGet(SETTINGS_KEY);
  return Object.assign(
    {
      enabled: true,
      endpoint: DEFAULT_ENDPOINT
    },
    stored[SETTINGS_KEY] || {}
  );
}

function setupFlushAlarm() {
  chrome.alarms.create(FLUSH_ALARM_NAME, {
    periodInMinutes: FLUSH_INTERVAL_MINUTES
  });
}

function trimText(value, maxLength) {
  return String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, maxLength);
}

function normalizeKeywords(keywords) {
  const result = [];
  const seen = new Set();

  for (const keyword of Array.isArray(keywords) ? keywords : []) {
    const value = trimText(keyword, 128).toLowerCase();
    if (!value || seen.has(value)) {
      continue;
    }

    seen.add(value);
    result.push(value);

    if (result.length >= 128) {
      break;
    }
  }

  return result;
}

function itemToRecord(item) {
  const cleaned = DDCShared.cleanUrl(item?.url);
  if (!cleaned) {
    return null;
  }

  const textForKeywords = trimText(item?.text || item?.snippet || item?.title || "", item?.source === "page" ? 120000 : 4000);
  const maxKeywords = item?.source === "page" ? 128 : 48;
  const extractedKeywords = DDCShared.extractKeywords(textForKeywords, { maxKeywords });

  return {
    domain: cleaned.domain,
    url: cleaned.url,
    title: trimText(item?.title, 300),
    discovered_at: new Date().toISOString(),
    keywords: normalizeKeywords(extractedKeywords),
    source: item?.source === "page" ? "page" : "search_result"
  };
}

function shouldAcceptItem(item, sender) {
  if (!item || typeof item.url !== "string") {
    return false;
  }

  if (item.source === "page" && sender?.tab?.active === false) {
    return false;
  }

  return item.source === "page" || item.source === "search_result";
}

async function enqueueItems(items, sender) {
  const settings = await getSettings();
  if (!settings.enabled) {
    return { accepted: 0, queued: 0, disabled: true };
  }

  const records = (Array.isArray(items) ? items : [])
    .filter((item) => shouldAcceptItem(item, sender))
    .map(itemToRecord)
    .filter(Boolean);

  const stored = await storageGet(QUEUE_KEY);
  if (records.length === 0) {
    return { accepted: 0, queued: stored[QUEUE_KEY]?.length || 0 };
  }

  const currentQueue = Array.isArray(stored[QUEUE_KEY]) ? stored[QUEUE_KEY] : [];
  const byUrl = new Map(currentQueue.map((entry) => [entry.record.url, entry]));

  for (const record of records) {
    const existing = byUrl.get(record.url);
    byUrl.set(record.url, {
      id: existing?.id || getUuid(),
      queued_at: existing?.queued_at || new Date().toISOString(),
      record
    });
  }

  const queue = Array.from(byUrl.values()).slice(-MAX_QUEUE_SIZE);
  await storageSet({ [QUEUE_KEY]: queue });

  if (queue.length >= FLUSH_THRESHOLD) {
    flushQueue().catch((error) => console.warn("DDC queue flush failed", error));
  }

  return { accepted: records.length, queued: queue.length };
}

async function flushQueue() {
  if (flushInProgress) {
    return { sent: 0, skipped: "in_progress" };
  }

  flushInProgress = true;
  try {
    const settings = await getSettings();
    if (!settings.enabled || !settings.endpoint) {
      return { sent: 0, skipped: "disabled" };
    }

    const stored = await storageGet(QUEUE_KEY);
    const queue = Array.isArray(stored[QUEUE_KEY]) ? stored[QUEUE_KEY] : [];
    if (queue.length === 0) {
      return { sent: 0 };
    }

    const batch = queue.slice(0, MAX_BATCH_SIZE);
    const response = await fetch(settings.endpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ records: batch.map((entry) => entry.record) }),
      credentials: "omit"
    });

    if (!response.ok) {
      throw new Error(`DDC relay returned ${response.status}`);
    }

    const sentIds = new Set(batch.map((entry) => entry.id));
    const latest = await storageGet(QUEUE_KEY);
    const latestQueue = Array.isArray(latest[QUEUE_KEY]) ? latest[QUEUE_KEY] : [];
    const remaining = latestQueue.filter((entry) => !sentIds.has(entry.id));
    await storageSet({ [QUEUE_KEY]: remaining });

    return { sent: batch.length, queued: remaining.length };
  } finally {
    flushInProgress = false;
  }
}

chrome.runtime.onInstalled.addListener(setupFlushAlarm);
chrome.runtime.onStartup.addListener(setupFlushAlarm);
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === FLUSH_ALARM_NAME) {
    flushQueue().catch((error) => console.warn("DDC queue flush failed", error));
  }
});

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message?.type === "DDC_COLLECTED_ITEMS") {
    enqueueItems(message.items, sender).then(sendResponse).catch((error) => sendResponse({ error: error.message }));
    return true;
  }
  if (message?.type === "DDC_FLUSH_NOW") {
    flushQueue().then(sendResponse).catch((error) => sendResponse({ error: error.message }));
    return true;
  }
  if (message?.type === "DDC_GET_STATUS") {
    Promise.all([storageGet(QUEUE_KEY), getSettings()])
      .then(([stored, settings]) => sendResponse({
        queued: Array.isArray(stored[QUEUE_KEY]) ? stored[QUEUE_KEY].length : 0,
        settings
      }))
      .catch((error) => sendResponse({ error: error.message }));
    return true;
  }
  return false;
});

setupFlushAlarm();
