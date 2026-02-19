import express from 'express';
import { all, get, run } from '../database/datenbank.js';
import { NORMALIZED_PUBLISHED_AT_SQL, clusterDigestArticles, getTodayRangeIso, mapArticleRow } from './dailyDigest.js';
import { publish } from '../services/events.js';
import { logInfo } from '../utils/logger.js';

const router = express.Router();

function normalizeArticleIds(value) {
    if (!Array.isArray(value)) {
        return [];
    }

    const set = new Set();
    value.forEach(id => {
        const normalized = Number(id);
        if (Number.isInteger(normalized) && normalized > 0) {
            set.add(normalized);
        }
    });
    return Array.from(set);
}

function chunkArray(items, size = 400) {
    if (!Array.isArray(items) || items.length === 0) {
        return [];
    }

    const chunks = [];
    for (let index = 0; index < items.length; index += size) {
        chunks.push(items.slice(index, index + size));
    }
    return chunks;
}

async function markArticlesAsDailyDigestedInTransaction(articleIds) {
    const ids = normalizeArticleIds(articleIds);
    if (ids.length === 0) {
        return { updated: 0, total: 0 };
    }

    await run('BEGIN IMMEDIATE');
    let updated = 0;
    try {
        const chunks = chunkArray(ids, 400);
        for (const chunk of chunks) {
            const placeholders = chunk.map(() => '?').join(', ');
            const result = await run(`UPDATE articles SET dailyDigested = 1 WHERE id IN (${placeholders})`, chunk);
            updated += Number(result?.changes || 0);
        }
        await run('COMMIT');
    } catch (err) {
        try {
            await run('ROLLBACK');
        } catch {
            // ignore rollback error to preserve original failure
        }
        throw err;
    }

    return { updated, total: ids.length };
}

router.get('/stats', async (_req, res) => {
    const row = await get('SELECT COUNT(*) AS total FROM articles');
    return res.json({ total: Number(row?.total || 0) });
});

router.get('/daily-digest', async (_req, res) => {
    const { startIso, endIso } = getTodayRangeIso();
    const rows = await all(
        `
        WITH normalized_articles AS (
            SELECT
                articles.*,
                feeds.name as sourceName,
                feeds.logo as sourceLogo,
                feeds.logoMime as sourceLogoMime,
                ${NORMALIZED_PUBLISHED_AT_SQL} AS publishedAtParsed
            FROM articles
            JOIN feeds ON feeds.id = articles.feedId
        )
        SELECT *
        FROM normalized_articles
        WHERE publishedAtParsed IS NOT NULL
          AND COALESCE(dailyDigested, 0) = 0
          AND publishedAtParsed >= datetime(?)
          AND publishedAtParsed < datetime(?)
        ORDER BY publishedAtParsed DESC, id DESC
        `,
        [startIso, endIso],
    );
    const mapped = rows.map(mapArticleRow);
    const clusters = clusterDigestArticles(mapped);

    return res.json({
        startIso,
        endIso,
        totalArticles: mapped.length,
        totalClusters: clusters.length,
        clusters,
    });
});

router.patch('/:id/daily-digested', async ({ params: { id } }, res) => {
    const articleId = Number(id);
    if (!Number.isInteger(articleId) || articleId <= 0) {
        return res.status(400).json({ error: 'Invalid article id' });
    }

    const existing = await get('SELECT id, dailyDigested FROM articles WHERE id = ?', [articleId]);
    if (!existing) {
        return res.status(404).json({ error: 'Article not found' });
    }

    await run('UPDATE articles SET dailyDigested = 1 WHERE id = ?', [articleId]);
    publish('articles.updated', { source: 'daily-digest', articleId, dailyDigested: true });

    return res.json({
        ok: true,
        id: articleId,
        dailyDigested: true,
        alreadyDigested: Boolean(existing.dailyDigested),
    });
});

router.post('/daily-digest/mark-all-digested', async ({ body }, res) => {
    const articleIds = normalizeArticleIds(body?.articleIds);
    const result = await markArticlesAsDailyDigestedInTransaction(articleIds);
    publish('articles.updated', {
        source: 'daily-digest',
        batch: true,
        updated: result.updated,
        total: result.total,
    });

    return res.json({ ok: true, ...result });
});

router.get('/', async ({ query: { feedId, source, listId, query, limit = 100 } }, res) => {
    const params = [];
    const whereParts = [];
    const like = `%${query}%`;

    if (feedId) {
        whereParts.push('feeds.id = ?');
        params.push(feedId);
    } else if (source) {
        whereParts.push('feeds.name = ?');
        params.push(source);
    }
    if (listId) {
        whereParts.push('list_items.listId = ?');
        params.push(listId);
    }
    if (query) {
        logInfo('Search query', { query });
        whereParts.push('(articles.title LIKE ? OR articles.teaser LIKE ? OR feeds.name LIKE ?)');
        params.push(like, like, like);
    }

    const where = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const sql = `
    SELECT articles.*, feeds.name as sourceName, feeds.logo as sourceLogo, feeds.logoMime as sourceLogoMime
    FROM articles
    JOIN feeds ON feeds.id = articles.feedId
    LEFT JOIN list_items ON list_items.articleId = articles.id
    ${where}
    ORDER BY datetime(articles.publishedAt) DESC, articles.id DESC
    LIMIT ?
  `;
    params.push(Number(limit) || 100);

    const rows = await all(sql, params);
    const mapped = rows.map(mapArticleRow);

    return res.json(mapped);
});

router.get('/:id/lists', async ({ params: { id } }, res) => {
    const rows = await all(
        `SELECT lists.id, lists.name, lists.color
     FROM list_items
     JOIN lists ON lists.id = list_items.listId
     WHERE list_items.articleId = ?`,
        [id],
    );

    return res.json(rows);
});

export default router;
