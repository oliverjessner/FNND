import express from 'express';
import { all, get, run, transaction } from '../database/datenbank.js';
import { auth } from '../middleware/auth.js';
import { isValidUrl } from '../utils/validation.js';
import Parser from 'rss-parser';
import { fetchSiteLogo } from '../services/logo.js';
import { publish } from '../services/events.js';

const router = express.Router();
const parser = new Parser({
    timeout: 8000,
    headers: {
        'User-Agent': 'rss-parser',
        Accept: 'application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8',
    },
});

async function isFeedReachable(url) {
    try {
        await parser.parseURL(url);
        return true;
    } catch {
        return false;
    }
}

const feedSelect = `SELECT feeds.id, feeds.sourceId, feeds.feedUrl, feeds.createdAt, feeds.updatedAt,
    COALESCE(NULLIF(feeds.name, ''), sources.name) AS name,
    sources.websiteUrl, sources.logo, sources.logoMime
    FROM feeds JOIN sources ON sources.id = feeds.sourceId`;

function mapFeed(feed) {
    const logoDataUrl = feed.logo && feed.logoMime ? `/api/feeds/${encodeURIComponent(feed.id)}/logo` : null;
    const { logo, logoMime, ...rest } = feed;
    return { ...rest, logoDataUrl };
}

async function upsertSource({ name, websiteUrl, logoBuffer, logoMime }) {
    const canonicalWebsiteUrl = new URL(websiteUrl).toString();
    await run(
        `INSERT INTO sources (name, websiteUrl, canonicalWebsiteUrl, logo, logoMime, createdAt, updatedAt)
         VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
         ON CONFLICT(canonicalWebsiteUrl) DO UPDATE SET
           websiteUrl = excluded.websiteUrl, logo = COALESCE(excluded.logo, sources.logo),
           logoMime = COALESCE(excluded.logoMime, sources.logoMime), updatedAt = datetime('now')`,
        [name, websiteUrl, canonicalWebsiteUrl, logoBuffer, logoMime],
    );
    return get('SELECT id FROM sources WHERE canonicalWebsiteUrl = ?', [canonicalWebsiteUrl]);
}

export async function createFeedRecord({ name, websiteUrl, feedUrl, logoBuffer = null, logoMime = null }) {
    return transaction(async () => {
        const source = await upsertSource({ name, websiteUrl, logoBuffer, logoMime });
        const result = await run(
            `INSERT INTO feeds (sourceId, name, feedUrl, createdAt, updatedAt)
             VALUES (?, ?, ?, datetime('now'), datetime('now'))`,
            [source.id, name, feedUrl],
        );
        return Number(result.lastID);
    });
}

export async function updateFeedRecord({ id, name, websiteUrl, feedUrl, logoBuffer = null, logoMime = null }) {
    return transaction(async () => {
        const source = await upsertSource({ name, websiteUrl, logoBuffer, logoMime });
        return run(
            `UPDATE feeds SET sourceId = ?, name = ?, feedUrl = ?, updatedAt = datetime('now') WHERE id = ?`,
            [source.id, name, feedUrl, id],
        );
    });
}

router.get('/', auth, async (_, res) => {
    const feeds = await all(`${feedSelect} ORDER BY feeds.id DESC`);
    return res.json(feeds.map(mapFeed));
});

router.get('/:id/logo', auth, async (req, res) => {
    const row = await get(
        `SELECT sources.logo, sources.logoMime FROM feeds
         JOIN sources ON sources.id = feeds.sourceId WHERE feeds.id = ?`,
        [req.params.id],
    );
    if (!row?.logo || !row?.logoMime) return res.status(404).end();
    res.setHeader('Content-Type', row.logoMime);
    res.setHeader('Cache-Control', 'public, max-age=86400, immutable');
    return res.send(row.logo);
});

router.post('/', auth, async (req, res) => {
    const { name, websiteUrl, feedUrl } = req.body || {};
    let logoBuffer = null;
    let logoMime = null;

    if (!name || !websiteUrl || !feedUrl) {
        return res.status(400).json({ error: 'name, websiteUrl, and feedUrl are required' });
    }
    if (!isValidUrl(websiteUrl) || !isValidUrl(feedUrl)) {
        return res.status(400).json({ error: 'Invalid URL format' });
    }
    if (await get('SELECT id FROM feeds WHERE feedUrl = ?', [feedUrl])) {
        return res.status(409).json({ error: 'Feed URL already exists' });
    }
    const reachable = await isFeedReachable(feedUrl);
    if (!reachable) {
        return res.status(400).json({ error: 'Feed URL not reachable' });
    }

    const logo = await fetchSiteLogo(websiteUrl);
    if (logo) {
        logoBuffer = logo.buffer;
        logoMime = logo.mime;
    }

    const feedId = await createFeedRecord({ name, websiteUrl, feedUrl, logoBuffer, logoMime });
    const feed = await get(`${feedSelect} WHERE feeds.id = ?`, [feedId]);

    publish('feeds.updated', { id: feed.id });
    return res.status(201).json(mapFeed(feed));
});

router.put('/:id', auth, async (req, res) => {
    const { id } = req.params;
    const { name, websiteUrl, feedUrl } = req.body || {};
    const existing = await get(`${feedSelect} WHERE feeds.id = ? AND ? = ?`, [id, req.auth.ownerId, 'local-owner']);

    if (!name || !websiteUrl || !feedUrl) {
        return res.status(400).json({ error: 'name, websiteUrl, and feedUrl are required' });
    }
    if (!isValidUrl(websiteUrl) || !isValidUrl(feedUrl)) {
        return res.status(400).json({ error: 'Invalid URL format' });
    }
    const duplicate = await get('SELECT id FROM feeds WHERE feedUrl = ? AND id <> ?', [feedUrl, id]);
    if (duplicate) return res.status(409).json({ error: 'Feed URL already exists' });
    const reachable = await isFeedReachable(feedUrl);
    if (!reachable) {
        return res.status(400).json({ error: 'Feed URL not reachable' });
    }
    if (!existing) {
        return res.status(404).json({ error: 'Feed not found' });
    }

    let logoBuffer = existing.logo;
    let logoMime = existing.logoMime;

    if (!existing.logo || existing.websiteUrl !== websiteUrl) {
        const logo = await fetchSiteLogo(websiteUrl);
        if (logo) {
            logoBuffer = logo.buffer;
            logoMime = logo.mime;
        }
    }

    const result = await updateFeedRecord({ id, name, websiteUrl, feedUrl, logoBuffer, logoMime });

    if (result.changes === 0) {
        return res.status(404).json({ error: 'Feed not found' });
    }

    const feed = await get(`${feedSelect} WHERE feeds.id = ? AND ? = ?`, [id, req.auth.ownerId, 'local-owner']);

    publish('feeds.updated', { id: feed.id });
    res.json(mapFeed(feed));
});

router.delete('/:id', auth, async (req, res) => {
    const { id } = req.params;
    const result = await run('DELETE FROM feeds WHERE id = ? AND ? = ?', [id, req.auth.ownerId, 'local-owner']);

    if (result.changes === 0) {
        return res.status(404).json({ error: 'Feed not found' });
    }

    publish('feeds.updated', { id });
    return res.status(204).end();
});

router.get('/test/url', auth, async ({ query: { url } }, res) => {
    if (!isValidUrl(url)) {
        return res.status(400).json({ error: 'Invalid URL format' });
    }

    try {
        const feed = await parser.parseURL(url);
        const titles = (feed.items || [])
            .slice(0, 3)
            .map(item => item.title)
            .filter(Boolean);

        return res.json({ title: feed.title || null, itemCount: (feed.items || []).length, sampleTitles: titles });
    } catch (err) {
        return res.status(400).json({ error: 'Feed not reachable or invalid RSS' });
    }
});

export default router;
