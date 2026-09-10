import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import path from "node:path";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const source = fs.readFileSync(path.join(__dirname, "../extension/shared/url.js"), "utf8");
const context = {
  Buffer,
  URL,
  location: { href: "https://www.google.com/search?q=ddc" }
};
context.globalThis = context;
vm.createContext(context);
vm.runInContext(source, context);

test("cleanUrl removes tracking parameters and fragments while preserving content parameters", () => {
  const result = context.DDCShared.cleanUrl("https://www.youtube.com/watch?v=abc123&utm_source=x&fbclid=y#comments");
  assert.equal(result.domain, "www.youtube.com");
  assert.equal(result.url, "https://www.youtube.com/watch?v=abc123");
});

test("cleanUrl unwraps Google result redirects without fetching the target", () => {
  const result = context.DDCShared.cleanUrl("/url?q=https%3A%2F%2Fexample.com%2Fpage%3Futm_medium%3Dad%26id%3D10&sa=U");
  assert.equal(result.url, "https://example.com/page?id=10");
});

test("cleanUrl unwraps Bing encoded redirects without fetching the target", () => {
  const encoded = `a1${Buffer.from("https://example.org/resource?utm_campaign=x&v=keep").toString("base64url")}`;
  const result = context.DDCShared.cleanUrl(`https://www.bing.com/ck/a?u=${encoded}`);
  assert.equal(result.url, "https://example.org/resource?v=keep");
});

test("cleanUrl rejects non-http protocols", () => {
  assert.equal(context.DDCShared.cleanUrl("javascript:alert(1)"), null);
});

test("cleanUrl rejects local and private network URLs", () => {
  assert.equal(context.DDCShared.cleanUrl("http://127.0.0.1:8787/ingest"), null);
  assert.equal(context.DDCShared.cleanUrl("http://localhost:3000"), null);
  assert.equal(context.DDCShared.cleanUrl("http://192.168.1.10/page"), null);
});
