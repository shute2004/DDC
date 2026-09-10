(function attachKeywordUtilities(global) {
  const DEFAULT_MAX_KEYWORDS = 128;
  const STOPWORDS = new Set([
    "a", "about", "after", "all", "also", "an", "and", "are", "as", "at", "be", "been", "but", "by", "can",
    "for", "from", "has", "have", "how", "in", "into", "is", "it", "its", "more", "new", "no", "not", "of", "on",
    "or", "our", "that", "the", "this", "to", "was", "we", "were", "what", "when", "where", "which", "who", "will",
    "with", "you", "your", "これ", "それ", "あれ", "ここ", "そこ", "ため", "こと", "もの", "よう", "する", "した", "ます", "です",
    "いる", "ある", "れる", "られる", "について", "として", "から", "まで", "より"
  ]);

  function normalizeToken(token) {
    return String(token || "")
      .normalize("NFKC")
      .replace(/^[\p{P}\p{S}\s]+|[\p{P}\p{S}\s]+$/gu, "")
      .trim()
      .toLowerCase();
  }

  function isUsefulToken(token) {
    if (!token || STOPWORDS.has(token) || token.length < 2 || /^\d+$/u.test(token)) return false;
    if (/^[\p{Script=Hiragana}ー]+$/u.test(token) && token.length < 4) return false;
    return /[\p{Letter}\p{Number}]/u.test(token);
  }

  function segmentWithIntl(text) {
    if (!global.Intl || typeof global.Intl.Segmenter !== "function") return null;
    const segmenter = new global.Intl.Segmenter(["ja", "en"], { granularity: "word" });
    return Array.from(segmenter.segment(text)).filter((entry) => entry.isWordLike).map((entry) => entry.segment);
  }

  function segmentWithRegex(text) {
    return text.match(/[\p{Script=Han}\p{Script=Hiragana}\p{Script=Katakana}ー]{2,}|[a-zA-Z][a-zA-Z0-9_-]{2,}/gu) || [];
  }

  function extractKeywords(text, options) {
    const maxKeywords = Math.max(1, Number(options?.maxKeywords || DEFAULT_MAX_KEYWORDS));
    const normalizedText = String(text || "").normalize("NFKC");
    const rawTokens = segmentWithIntl(normalizedText) || segmentWithRegex(normalizedText);
    const keywords = [];
    const seen = new Set();

    for (const rawToken of rawTokens) {
      const token = normalizeToken(rawToken);
      if (!isUsefulToken(token) || seen.has(token)) continue;
      seen.add(token);
      keywords.push(token);
      if (keywords.length >= maxKeywords) break;
    }
    return keywords;
  }

  global.DDCShared = Object.assign(global.DDCShared || {}, { extractKeywords });
})(globalThis);
