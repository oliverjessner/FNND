import express from 'express';
import { all, get, run } from '../database/datenbank.js';
import { auth } from '../middleware/auth.js';
import { queryArticles } from '../services/article-queries.js';
import { buildDigestPayload } from '../services/digest-payload.js';
import { normalizeDigestVariant } from './digest.js';
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

async function buildArticleListsBulkPayload(articleIds) {
    const normalizedArticleIds = normalizeArticleIds(articleIds);
    const listsByArticleId = new Map();
    const listById = new Map();

    if (normalizedArticleIds.length === 0) {
        return {
            articleIds: [],
            listsByArticleId: {},
            commonLists: [],
        };
    }

    const articleIdChunks = chunkArray(normalizedArticleIds, 400);
    for (const chunk of articleIdChunks) {
        const rows = await all(
            `
            SELECT
                list_items.articleId AS articleId,
                lists.id AS id,
                lists.name AS name,
                lists.color AS color
            FROM list_items
            JOIN lists ON lists.id = list_items.listId
            WHERE list_items.articleId IN (SELECT value FROM json_each(?))
            ORDER BY lists.name COLLATE NOCASE ASC, lists.id ASC
            `,
            [JSON.stringify(chunk)],
        );

        rows.forEach(row => {
            const articleId = Number(row.articleId);
            const listId = Number(row.id);

            if (!Number.isInteger(articleId) || articleId <= 0) {
                return;
            }
            if (!Number.isInteger(listId) || listId <= 0) {
                return;
            }

            const normalizedList = {
                id: listId,
                name: row.name,
                color: row.color,
            };
            const currentLists = listsByArticleId.get(articleId) || [];

            if (!currentLists.some(list => Number(list.id) === listId)) {
                currentLists.push(normalizedList);
                listsByArticleId.set(articleId, currentLists);
            }
            if (!listById.has(listId)) {
                listById.set(listId, normalizedList);
            }
        });
    }

    const listOccurrenceCount = new Map();
    normalizedArticleIds.forEach(articleId => {
        const articleLists = listsByArticleId.get(articleId) || [];
        const uniqueListIds = new Set(articleLists.map(list => String(list.id)));

        uniqueListIds.forEach(listId => {
            listOccurrenceCount.set(listId, (listOccurrenceCount.get(listId) || 0) + 1);
        });
    });

    const commonListIds = Array.from(listOccurrenceCount.entries())
        .filter(([, count]) => count === normalizedArticleIds.length)
        .map(([listId]) => Number(listId))
        .filter(listId => Number.isInteger(listId) && listId > 0);
    const commonLists = commonListIds
        .map(listId => listById.get(listId))
        .filter(Boolean)
        .sort((left, right) => String(left?.name || '').localeCompare(String(right?.name || ''), 'en', { sensitivity: 'base' }));

    const listsByArticleIdObject = {};
    normalizedArticleIds.forEach(articleId => {
        const articleLists = listsByArticleId.get(articleId) || [];
        listsByArticleIdObject[String(articleId)] = articleLists;
    });

    return {
        articleIds: normalizedArticleIds,
        listsByArticleId: listsByArticleIdObject,
        commonLists,
    };
}

export async function setArticlesDigestedStateInTransaction(articleIds, digested) {
    const ids = normalizeArticleIds(articleIds);
    if (ids.length === 0) {
        return { updated: 0, total: 0 };
    }

    await run('BEGIN IMMEDIATE');
    let updated = 0;
    try {
        for (const chunk of chunkArray(ids, 400)) {
            const result = await run(
                'UPDATE articles SET dailyDigested = ? WHERE id IN (SELECT value FROM json_each(?))',
                [digested ? 1 : 0, JSON.stringify(chunk)],
            );
            updated += Number(result?.changes || 0);
        }
        await run('COMMIT');
    } catch (err) {
        try {
            await run('ROLLBACK');
        } catch {
            // Preserve the original transaction error.
        }
        throw err;
    }

    return { updated, total: ids.length };
}

export async function setArticleDismissedState(articleId, dismissed) {
    const existing = await get('SELECT id FROM articles WHERE id = ?', [articleId]);
    if (!existing) {
        return false;
    }

    await run("UPDATE articles SET dismissedAt = CASE WHEN ? = 1 THEN datetime('now') ELSE NULL END WHERE id = ?", [
        dismissed ? 1 : 0,
        articleId,
    ]);
    return true;
}

async function handleDigestRequest(req, res) {
    const variant = normalizeDigestVariant(req.query?.variant || req.query?.range || 'day');
    const payload = await buildDigestPayload(variant, { all });
    return res.json(payload);
}

async function handleMarkArticleDigested(req, res) {
    const articleId = Number(req.params?.id);
    if (!Number.isInteger(articleId) || articleId <= 0) {
        return res.status(400).json({ error: 'Invalid article id' });
    }

    const existing = await get('SELECT id, dailyDigested FROM articles WHERE id = ? AND ? = ?', [
        articleId,
        req.auth.ownerId,
        'local-owner',
    ]);
    if (!existing) {
        return res.status(404).json({ error: 'Article not found' });
    }

    await run('UPDATE articles SET dailyDigested = 1 WHERE id = ? AND ? = ?', [articleId, req.auth.ownerId, 'local-owner']);
    publish('articles.updated', { source: 'digest', articleId, dailyDigested: true });

    return res.json({
        ok: true,
        id: articleId,
        digested: true,
        dailyDigested: true,
        alreadyDigested: Boolean(existing.dailyDigested),
    });
}

async function handleMarkAllDigested(req, res) {
    const articleIds = normalizeArticleIds(req.body?.articleIds);
    const result = await setArticlesDigestedStateInTransaction(articleIds, true);
    publish('articles.updated', {
        source: 'digest',
        batch: true,
        updated: result.updated,
        total: result.total,
    });

    return res.json({ ok: true, ...result });
}

async function handleRestoreDigested(req, res) {
    const articleIds = normalizeArticleIds(req.body?.articleIds);
    const result = await setArticlesDigestedStateInTransaction(articleIds, false);
    publish('articles.updated', {
        source: 'digest',
        batch: true,
        restored: true,
        updated: result.updated,
        total: result.total,
    });

    return res.json({ ok: true, ...result });
}

async function handleSetDismissed(req, res) {
    const articleId = Number(req.params?.id);
    if (!Number.isInteger(articleId) || articleId <= 0) {
        return res.status(400).json({ error: 'Invalid article id' });
    }

    const dismissed = req.body?.dismissed !== false;
    const updated = await setArticleDismissedState(articleId, dismissed);
    if (!updated) {
        return res.status(404).json({ error: 'Article not found' });
    }
    publish('articles.updated', { source: 'feed', articleId, dismissed });

    return res.json({ ok: true, id: articleId, dismissed });
}

router.get('/stats', auth, async (_req, res) => {
    const row = await get(
        `SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN dismissedAt IS NULL THEN 1 ELSE 0 END) AS unread,
            SUM(CASE WHEN dismissedAt IS NOT NULL THEN 1 ELSE 0 END) AS dismissed
         FROM articles`,
    );
    return res.json({
        total: Number(row?.total || 0),
        unread: Number(row?.unread || 0),
        dismissed: Number(row?.dismissed || 0),
    });
});

router.get('/digest', auth, handleDigestRequest);
router.get('/daily-digest', auth, async (_req, res) => {
    const payload = await buildDigestPayload('day', { all });
    return res.json(payload);
});
router.patch('/:id/digested', auth, handleMarkArticleDigested);
router.patch('/:id/daily-digested', auth, handleMarkArticleDigested);
router.post('/digest/mark-all-digested', auth, handleMarkAllDigested);
router.post('/daily-digest/mark-all-digested', auth, handleMarkAllDigested);
router.post('/digest/restore', auth, handleRestoreDigested);
router.patch('/:id/dismissed', auth, handleSetDismissed);

router.get('/', auth, async ({ query: { feedId, source, listId, topic, query, limit = 100, offset = 0 } }, res) => {
    if (query) {
        logInfo('Search query', { query });
    }
    const articles = await queryArticles(
        { feedId, source, listId, topic, query, limit, offset, activeOnly: true, includeTopics: true, maxLimit: 250 },
        { all },
    );
    return res.json(articles);
});

router.post('/lists/bulk', auth, async ({ body }, res) => {
    const articleIds = normalizeArticleIds(body?.articleIds);
    const payload = await buildArticleListsBulkPayload(articleIds);

    return res.json(payload);
});

router.get('/:id/lists', auth, async ({ params: { id } }, res) => {
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
