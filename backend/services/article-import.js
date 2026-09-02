import { get, run } from '../database/datenbank.js';
import { canonicalizeArticleUrl } from '../routes/digest.js';
import { ingestArticle } from './article-ingest.js';

export const MANUAL_IMPORT_FEED_PREFIX = 'nbs-import:';

export function isManualImportFeedUrl(value) {
    return String(value || '').startsWith(MANUAL_IMPORT_FEED_PREFIX);
}

export function normalizeImportUrl(value) {
    const text = String(value || '').trim();
    if (!text || text.length > 2_048) return null;
    try {
        const url = new URL(text);
        if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password) return null;
        url.hash = '';
        return url.toString();
    } catch {
        return null;
    }
}

export function deriveImportedArticleTitle(value) {
    const url = new URL(value);
    const segment = url.pathname.split('/').filter(Boolean).at(-1) || '';
    let title = segment;
    try { title = decodeURIComponent(segment); } catch { /* Keep the encoded path segment. */ }
    title = title
        .replace(/\.(?:s?html?|php|aspx?)$/iu, '')
        .replace(/[-_]+/gu, ' ')
        .replace(/\s+/gu, ' ')
        .trim();
    return (title || url.hostname.replace(/^www\./iu, '')).slice(0, 1_000);
}

async function ensureImportFeed(articleUrl) {
    const url = new URL(articleUrl);
    const websiteUrl = new URL('/', url.origin).toString();
    const fallbackName = url.hostname.replace(/^www\./iu, '') || 'Imported';
    await run(
        `INSERT INTO sources (name, websiteUrl, canonicalWebsiteUrl, createdAt, updatedAt)
         VALUES (?, ?, ?, datetime('now'), datetime('now'))
         ON CONFLICT(canonicalWebsiteUrl) DO UPDATE SET updatedAt = datetime('now')`,
        [fallbackName, websiteUrl, websiteUrl],
    );
    const source = await get('SELECT id, name FROM sources WHERE canonicalWebsiteUrl = ?', [websiteUrl]);
    const feedUrl = `${MANUAL_IMPORT_FEED_PREFIX}${url.origin}`;
    const feedName = `${source?.name || fallbackName} (Imported)`;
    await run(
        `INSERT INTO feeds (sourceId, name, feedUrl, createdAt, updatedAt)
         VALUES (?, ?, ?, datetime('now'), datetime('now'))
         ON CONFLICT(feedUrl) DO UPDATE SET sourceId = excluded.sourceId, name = excluded.name, updatedAt = datetime('now')`,
        [source.id, feedName, feedUrl],
    );
    return get('SELECT id FROM feeds WHERE feedUrl = ?', [feedUrl]);
}

export async function importArticlesFromUrls(values) {
    const inputs = Array.isArray(values) ? values : [];
    const result = { received: inputs.length, imported: 0, duplicates: 0, invalid: 0, failed: 0, articleIds: [], issues: [] };
    const seen = new Set();

    for (let index = 0; index < inputs.length; index += 1) {
        const url = normalizeImportUrl(inputs[index]);
        if (!url) {
            result.invalid += 1;
            result.issues.push({ index, reason: 'Invalid HTTP(S) URL' });
            continue;
        }
        const canonicalUrl = canonicalizeArticleUrl(url) || url;
        if (seen.has(canonicalUrl)) {
            result.duplicates += 1;
            continue;
        }
        seen.add(canonicalUrl);

        const existing = await get('SELECT id FROM articles WHERE canonicalUrl = ? ORDER BY id ASC LIMIT 1', [canonicalUrl]);
        if (existing) {
            result.duplicates += 1;
            result.articleIds.push(Number(existing.id));
            continue;
        }

        try {
            const feed = await ensureImportFeed(url);
            const imported = await ingestArticle({
                feedId: Number(feed.id),
                externalId: canonicalUrl,
                title: deriveImportedArticleTitle(url),
                url,
                publishedAt: new Date().toISOString(),
            });
            if (imported.inserted) result.imported += 1;
            else result.duplicates += 1;
            result.articleIds.push(Number(imported.id));
        } catch {
            result.failed += 1;
            result.issues.push({ index, reason: 'Could not store article' });
        }
    }

    result.articleIds = [...new Set(result.articleIds)];
    return result;
}
