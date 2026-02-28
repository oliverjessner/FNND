import express from 'express';
import { all, get, run } from '../database/datenbank.js';
import {
    clusterDigestArticles,
    getDigestRangeIso,
    mapArticleRow,
    normalizeDigestVariant,
} from './digest.js';
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

async function loadTopicsByArticleIds(articleIds) {
    const normalizedArticleIds = normalizeArticleIds(articleIds);
    const topicsByArticleId = new Map();

    if (normalizedArticleIds.length === 0) {
        return topicsByArticleId;
    }

    const articleIdChunks = chunkArray(normalizedArticleIds, 400);
    for (const chunk of articleIdChunks) {
        const placeholders = chunk.map(() => '?').join(', ');
        const topicRows = await all(
            `
            SELECT
                article_topics.articleId AS articleId,
                article_topics.topicSlug AS topicSlug,
                topics.label AS topicLabel,
                article_topics.score AS score
            FROM article_topics
            JOIN topics ON topics.slug = article_topics.topicSlug
            WHERE article_topics.articleId IN (${placeholders})
            ORDER BY article_topics.score DESC, article_topics.topicSlug ASC
            `,
            chunk,
        );

        topicRows.forEach(row => {
            const articleId = Number(row.articleId);
            if (!Number.isInteger(articleId) || articleId <= 0) {
                return;
            }
            const existing = topicsByArticleId.get(articleId) || [];
            existing.push({
                slug: row.topicSlug,
                label: row.topicLabel || row.topicSlug,
                score: Number(row.score || 0),
            });
            topicsByArticleId.set(articleId, existing);
        });
    }

    return topicsByArticleId;
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
        const placeholders = chunk.map(() => '?').join(', ');
        const rows = await all(
            `
            SELECT
                list_items.articleId AS articleId,
                lists.id AS id,
                lists.name AS name,
                lists.color AS color
            FROM list_items
            JOIN lists ON lists.id = list_items.listId
            WHERE list_items.articleId IN (${placeholders})
            ORDER BY lists.name COLLATE NOCASE ASC, lists.id ASC
            `,
            chunk,
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

async function markArticlesAsDigestedInTransaction(articleIds) {
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

async function buildDigestPayload(variant = 'day') {
    const normalizedVariant = normalizeDigestVariant(variant);
    const { startIso, endIso } = getDigestRangeIso(normalizedVariant);
    const rows = await all(
        `
        SELECT
            articles.*,
            feeds.name as sourceName,
            feeds.logo as sourceLogo,
            feeds.logoMime as sourceLogoMime
        FROM articles
        JOIN feeds ON feeds.id = articles.feedId
        WHERE articles.publishedAt IS NOT NULL
          AND articles.dailyDigested = 0
          AND articles.publishedAt >= ?
          AND articles.publishedAt < ?
          AND NOT EXISTS (
              SELECT 1
              FROM digest_excluded_feeds
              WHERE digest_excluded_feeds.feedId = articles.feedId
          )
          AND NOT EXISTS (
              SELECT 1
              FROM digest_blocked_words
              WHERE length(trim(digest_blocked_words.word)) > 0
                AND instr(
                    lower(coalesce(articles.title, '') || ' ' || coalesce(articles.teaser, '')),
                    lower(trim(digest_blocked_words.word))
                ) > 0
          )
        ORDER BY articles.publishedAt DESC, articles.id DESC
        `,
        [startIso, endIso],
    );

    const mapped = rows.map(mapArticleRow);
    const articleIds = mapped
        .map(article => Number(article.id))
        .filter(articleId => Number.isInteger(articleId) && articleId > 0);
    const topicsByArticleId = await loadTopicsByArticleIds(articleIds);
    const withTopics = mapped.map(article => ({
        ...article,
        topics: topicsByArticleId.get(Number(article.id)) || [],
    }));
    const clusters = clusterDigestArticles(withTopics);
    const visibleClusters =
        normalizedVariant === 'month'
            ? clusters.filter(cluster => {
                  const itemCount = Array.isArray(cluster?.items) ? cluster.items.length : 0;
                  const clusterCount = Number(cluster?.clusterCount || 0);
                  return Math.max(itemCount, clusterCount) > 1;
              })
            : clusters;
    const totalArticles = visibleClusters.reduce((sum, cluster) => {
        const items = Array.isArray(cluster?.items) ? cluster.items : [];
        return sum + items.length;
    }, 0);

    return {
        variant: normalizedVariant,
        startIso,
        endIso,
        totalArticles,
        totalClusters: visibleClusters.length,
        clusters: visibleClusters,
    };
}

async function handleDigestRequest(req, res) {
    const variant = normalizeDigestVariant(req.query?.variant || req.query?.range || 'day');
    const payload = await buildDigestPayload(variant);
    return res.json(payload);
}

async function handleMarkArticleDigested(req, res) {
    const articleId = Number(req.params?.id);
    if (!Number.isInteger(articleId) || articleId <= 0) {
        return res.status(400).json({ error: 'Invalid article id' });
    }

    const existing = await get('SELECT id, dailyDigested FROM articles WHERE id = ?', [articleId]);
    if (!existing) {
        return res.status(404).json({ error: 'Article not found' });
    }

    await run('UPDATE articles SET dailyDigested = 1 WHERE id = ?', [articleId]);
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
    const result = await markArticlesAsDigestedInTransaction(articleIds);
    publish('articles.updated', {
        source: 'digest',
        batch: true,
        updated: result.updated,
        total: result.total,
    });

    return res.json({ ok: true, ...result });
}

router.get('/stats', async (_req, res) => {
    const row = await get('SELECT COUNT(*) AS total FROM articles');
    return res.json({ total: Number(row?.total || 0) });
});

router.get('/digest', handleDigestRequest);
router.get('/daily-digest', async (_req, res) => {
    const payload = await buildDigestPayload('day');
    return res.json(payload);
});
router.patch('/:id/digested', handleMarkArticleDigested);
router.patch('/:id/daily-digested', handleMarkArticleDigested);
router.post('/digest/mark-all-digested', handleMarkAllDigested);
router.post('/daily-digest/mark-all-digested', handleMarkAllDigested);

router.get('/', async ({ query: { feedId, source, listId, topic, query, limit = 100 } }, res) => {
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
    if (topic) {
        whereParts.push('EXISTS (SELECT 1 FROM article_topics WHERE article_topics.articleId = articles.id AND article_topics.topicSlug = ?)');
        params.push(String(topic).trim().toLowerCase());
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
    ORDER BY articles.publishedAt DESC, articles.id DESC
    LIMIT ?
  `;
    params.push(Number(limit) || 100);

    const rows = await all(sql, params);
    const mapped = rows.map(mapArticleRow);
    const articleIds = mapped
        .map(article => Number(article.id))
        .filter(articleId => Number.isInteger(articleId) && articleId > 0);
    const topicsByArticleId = await loadTopicsByArticleIds(articleIds);

    const withTopics = mapped.map(article => ({
        ...article,
        topics: topicsByArticleId.get(Number(article.id)) || [],
    }));

    return res.json(withTopics);
});

router.post('/lists/bulk', async ({ body }, res) => {
    const articleIds = normalizeArticleIds(body?.articleIds);
    const payload = await buildArticleListsBulkPayload(articleIds);

    return res.json(payload);
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
