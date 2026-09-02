export const MAX_ARTICLE_IMPORT_URLS = 500;

function normalizeUrl(value) {
    try {
        const url = new URL(String(value || '').trim());
        if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) return null;
        url.hash = '';
        return url.toString();
    } catch {
        return null;
    }
}

export function parseArticleImportText(value, { maxUrls = MAX_ARTICLE_IMPORT_URLS } = {}) {
    const lines = String(value || '').split(/\r?\n/u).map(line => line.trim()).filter(Boolean);
    const urls = [];
    const seen = new Set();
    let invalid = 0;
    let duplicates = 0;
    let overflow = 0;

    for (const line of lines) {
        const url = normalizeUrl(line);
        if (!url) { invalid += 1; continue; }
        if (seen.has(url)) { duplicates += 1; continue; }
        seen.add(url);
        if (urls.length >= maxUrls) { overflow += 1; continue; }
        urls.push(url);
    }

    return { urls, lines: lines.length, invalid, duplicates, overflow };
}
