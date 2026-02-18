import crypto from 'node:crypto';
import express from 'express';
import { get, run } from '../database/datenbank.js';
import { publish } from '../services/events.js';

const router = express.Router();

function toIsoDate(value) {
    if (value === null || value === undefined || value === '') {
        return null;
    }

    if (typeof value === 'number' && Number.isFinite(value)) {
        const millis = value > 9999999999 ? value : value * 1000;
        const date = new Date(millis);
        return Number.isNaN(date.getTime()) ? null : date.toISOString();
    }

    if (typeof value === 'string') {
        const trimmed = value.trim();
        if (!trimmed) {
            return null;
        }

        if (/^\d+$/.test(trimmed)) {
            const numeric = Number(trimmed);
            if (Number.isFinite(numeric)) {
                const millis = numeric > 9999999999 ? numeric : numeric * 1000;
                const date = new Date(millis);
                return Number.isNaN(date.getTime()) ? null : date.toISOString();
            }
        }
    }

    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function normalizeTeaser(value, maxLen = 220) {
    if (!value) {
        return null;
    }

    const cleaned = String(value)
        .replace(/<[^>]*>/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();

    if (!cleaned) {
        return null;
    }

    if (cleaned.length <= maxLen) {
        return cleaned;
    }

    return `${cleaned.slice(0, maxLen - 1)}…`;
}

function createGuidOrHash(item, feedId, fallbackPublishedAt) {
    const candidate = item.guidOrHash || item.guid || item.id || item.url;
    if (candidate) {
        return String(candidate);
    }

    const seed = `${feedId}:${item.title || ''}:${fallbackPublishedAt || ''}`;
    return crypto.createHash('sha256').update(seed).digest('hex');
}

function normalizeItems(body) {
    if (Array.isArray(body?.items)) {
        return body.items;
    }

    if (body && typeof body === 'object' && (body.title || body.url || body.guid || body.guidOrHash)) {
        return [body];
    }

    return [];
}

router.post('/articles', async ({ body }, res) => {
    const items = normalizeItems(body);
    const defaultFeedId = body?.feedId !== undefined && body?.feedId !== null ? Number(body.feedId) : null;

    if (items.length === 0) {
        return res.status(400).json({
            error: 'Request body must include at least one article via items[] or single article payload.',
        });
    }

    const feedExistsCache = new Map();
    let inserted = 0;
    let ignored = 0;
    let invalid = 0;

    for (const item of items) {
        const feedIdRaw = item?.feedId !== undefined && item?.feedId !== null ? item.feedId : defaultFeedId;
        const feedId = Number(feedIdRaw);

        if (!Number.isInteger(feedId) || feedId <= 0) {
            invalid += 1;
            continue;
        }

        if (!feedExistsCache.has(feedId)) {
            const feedRow = await get('SELECT id FROM feeds WHERE id = ?', [feedId]);
            feedExistsCache.set(feedId, Boolean(feedRow));
        }

        if (!feedExistsCache.get(feedId)) {
            invalid += 1;
            continue;
        }

        const publishedAt = toIsoDate(item?.publishedAt || item?.isoDate || item?.pubDate || item?.published);
        const teaser = normalizeTeaser(item?.teaser || item?.summary || item?.description || item?.contentSnippet || item?.content);
        const guidOrHash = createGuidOrHash(item || {}, feedId, publishedAt);

        try {
            const result = await run(
                `INSERT OR IGNORE INTO articles
                 (feedId, title, teaser, url, publishedAt, guidOrHash)
                 VALUES (?, ?, ?, ?, ?, ?)`,
                [feedId, item?.title || null, teaser, item?.url || null, publishedAt, guidOrHash],
            );

            if (result.changes > 0) {
                inserted += 1;
            } else {
                ignored += 1;
            }
        } catch {
            invalid += 1;
        }
    }

    publish('articles.updated', { source: 'webhook', inserted, ignored, invalid, received: items.length });
    publish('webhook.articles.received', { inserted, ignored, invalid, received: items.length });

    return res.status(201).json({
        ok: true,
        received: items.length,
        inserted,
        ignored,
        invalid,
    });
});

export default router;
