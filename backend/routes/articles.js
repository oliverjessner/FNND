import express from 'express';
import { all, get, run } from '../database/datenbank.js';
import { auth } from '../middleware/auth.js';
import { queryArticles } from '../services/article-queries.js';
import { getStoredDigestPayload, setDigestClustersCompleted } from '../services/digest-store.js';
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

export async function setArticlesDigestedStateInTransaction(articleIds, digested, variant = 'day') {
    const ids = normalizeArticleIds(articleIds);
    return setDigestClustersCompleted(ids, digested, normalizeDigestVariant(variant));
}

export async function setArticleDismissedState(articleId, dismissed) {
    const existing = await get('SELECT id FROM articles WHERE id = ?', [articleId]);
    if (!existing) {
        return false;
    }

    await run(
        `INSERT INTO article_state (articleId, dismissedAt, createdAt, updatedAt)
         VALUES (?, CASE WHEN ? = 1 THEN datetime('now') ELSE NULL END, datetime('now'), datetime('now'))
         ON CONFLICT(articleId) DO UPDATE SET dismissedAt = excluded.dismissedAt, updatedAt = datetime('now')`,
        [articleId, dismissed ? 1 : 0],
    );
    return true;
}

async function handleDigestRequest(req, res) {
    const variant = normalizeDigestVariant(req.query?.variant || req.query?.range || 'day');
    const payload = await getStoredDigestPayload(variant);
    return res.json(payload);
}

async function handleSetDigestState(req, res) {
    const articleIds = normalizeArticleIds(req.body?.articleIds);
    const variant = normalizeDigestVariant(req.body?.variant || 'day');
    const completed = req.body?.completed !== false;
    const result = await setArticlesDigestedStateInTransaction(articleIds, completed, variant);
    publish('articles.updated', {
        source: 'digest',
        batch: true,
        completed,
        variant,
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
            SUM(CASE WHEN article_state.dismissedAt IS NULL THEN 1 ELSE 0 END) AS unread,
            SUM(CASE WHEN article_state.dismissedAt IS NOT NULL THEN 1 ELSE 0 END) AS dismissed
         FROM articles LEFT JOIN article_state ON article_state.articleId = articles.id`,
    );
    return res.json({
        total: Number(row?.total || 0),
        unread: Number(row?.unread || 0),
        dismissed: Number(row?.dismissed || 0),
    });
});

router.get('/digest', auth, handleDigestRequest);
router.post('/digest/state', auth, handleSetDigestState);
router.patch('/:id/dismissed', auth, handleSetDismissed);

router.get('/', auth, async ({ query: { feedId, source, listId, topic, query, limit = 100, offset = 0, cursorPublishedAt, cursorId } }, res) => {
    if (query) {
        logInfo('Search query', { query });
    }
    const articles = await queryArticles(
        { feedId, source, listId, topic, query, limit, offset, cursorPublishedAt, cursorId, activeOnly: true, includeTopics: true, maxLimit: 250 },
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
