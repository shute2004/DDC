const SETTINGS_KEY = "ddc_settings";
const DEFAULT_SETTINGS = {
  enabled: true,
  endpoint: "http://127.0.0.1:8787/ingest"
};

const enabledInput = document.querySelector("#enabled");
const endpointInput = document.querySelector("#endpoint");
const statusElement = document.querySelector("#status");
const messageElement = document.querySelector("#message");
const saveButton = document.querySelector("#save");
const flushButton = document.querySelector("#flush");

function storageGet(keys) {
  return new Promise((resolve) => chrome.storage.local.get(keys, resolve));
}

function storageSet(values) {
  return new Promise((resolve) => chrome.storage.local.set(values, resolve));
}

function runtimeMessage(message) {
  return new Promise((resolve) => chrome.runtime.sendMessage(message, resolve));
}

function setMessage(message) {
  messageElement.textContent = message;
}

async function load() {
  const stored = await storageGet(SETTINGS_KEY);
  const settings = Object.assign({}, DEFAULT_SETTINGS, stored[SETTINGS_KEY] || {});
  enabledInput.checked = Boolean(settings.enabled);
  endpointInput.value = settings.endpoint;

  const status = await runtimeMessage({ type: "DDC_GET_STATUS" });
  statusElement.textContent = `キュー: ${status?.queued || 0} 件`;
}

async function save() {
  const endpoint = endpointInput.value.trim();
  if (!endpoint) {
    setMessage("エンドポイントを入力してください。");
    return;
  }

  await storageSet({
    [SETTINGS_KEY]: {
      enabled: enabledInput.checked,
      endpoint
    }
  });

  setMessage("保存しました。");
  await load();
}

async function flush() {
  setMessage("送信中...");
  const result = await runtimeMessage({ type: "DDC_FLUSH_NOW" });
  if (result?.error) {
    setMessage(`送信失敗: ${result.error}`);
    return;
  }
  setMessage(`${result?.sent || 0} 件を送信しました。`);
  await load();
}

saveButton.addEventListener("click", save);
flushButton.addEventListener("click", flush);
load().catch((error) => setMessage(error.message));
