import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import path from "node:path";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(__dirname, "../extension/shared/keywords.js"), "utf8");
const context = { Intl };
context.globalThis = context;
vm.createContext(context);
vm.runInContext(source, context);

test("extractKeywords deduplicates words and removes common stopwords", () => {
  const keywords = Array.from(context.DDCShared.extractKeywords("The DDC project collects DDC datasets for open search.", {
    maxKeywords: 10
  }));
  assert.deepEqual(keywords, ["ddc", "project", "collects", "datasets", "open", "search"]);
});

test("extractKeywords handles Japanese text with local segmentation", () => {
  const keywords = Array.from(context.DDCShared.extractKeywords("分散型データセットと検索エンジンのためのプロジェクト", {
    maxKeywords: 10
  }));
  assert.ok(keywords.includes("分散"));
  assert.ok(keywords.includes("データセット"));
  assert.ok(keywords.includes("検索"));
});
