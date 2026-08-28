import { mapArticleRow } from '../routes/digest.js';

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

export async function loadTopicsByArticleIds(articleIds, { all }) {
    const normalizedArticleIds = normalizeArticleIds(articleIds);
    const topicsByArticleId = new Map();

    if (normalizedArticleIds.length === 0) {
        return topicsByArticleId;
    }

    const articleIdChunks = chunkArray(normalizedArticleIds, 400);
    for (const chunk of articleIdChunks) {
        const topicRows = await all(
            `
            SELECT
                article_topics.articleId AS articleId,
                article_topics.topicSlug AS topicSlug,
                topics.label AS topicLabel,
                article_topics.score AS score
            FROM article_topics
            JOIN topics ON topics.slug = article_topics.topicSlug
            WHERE article_topics.articleId IN (SELECT value FROM json_each(?))
            ORDER BY article_topics.score DESC, article_topics.topicSlug ASC
            `,
            [JSON.stringify(chunk)],
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

export async function queryArticles(
    {
        feedId,
        source,
        listId,
        topic,
        query,
        limit = 100,
        offset = 0,
        activeOnly = true,
        includeTopics = true,
        maxLimit = 250,
    } = {},
    { all },
) {
    const params = [];
    const whereParts = [];

    if (feedId) {
        whereParts.push('feeds.id = ?');
        params.push(feedId);
    } else if (source) {
        whereParts.push('feeds.name = ?');
        params.push(source);
    }
    if (listId) {
        whereParts.push('EXISTS (SELECT 1 FROM list_items WHERE list_items.articleId = articles.id AND list_items.listId = ?)');
        params.push(listId);
    }
    if (topic) {
        whereParts.push('EXISTS (SELECT 1 FROM article_topics WHERE article_topics.articleId = articles.id AND article_topics.topicSlug = ?)');
        params.push(String(topic).trim().toLowerCase());
    }
    if (query) {
        const like = `%${query}%`;
        whereParts.push('(articles.title LIKE ? OR articles.teaser LIKE ? OR feeds.name LIKE ?)');
        params.push(like, like, like);
    }
    if (activeOnly) {
        whereParts.push('articles.dismissedAt IS NULL');
    }

    const parsedLimit = Number(limit);
    const parsedOffset = Number(offset);
    const hasUpperLimit = maxLimit !== null && maxLimit !== undefined && Number.isFinite(Number(maxLimit));
    const upperLimit = hasUpperLimit ? Math.max(Math.trunc(Number(maxLimit)), 1) : null;
    const positiveLimit = Number.isFinite(parsedLimit) ? Math.max(Math.trunc(parsedLimit), 1) : 100;
    const normalizedLimit = upperLimit === null ? positiveLimit : Math.min(positiveLimit, upperLimit);
    const normalizedOffset = Number.isFinite(parsedOffset) ? Math.max(Math.trunc(parsedOffset), 0) : 0;

    const sql = [
        `SELECT articles.*, feeds.name as sourceName, feeds.logo as sourceLogo, feeds.logoMime as sourceLogoMime,
         EXISTS (SELECT 1 FROM list_items WHERE list_items.articleId = articles.id) AS saved`,
        'FROM articles',
        'JOIN feeds ON feeds.id = articles.feedId',
        whereParts.length ? 'WHERE ' + whereParts.join(' AND ') : '',
        'ORDER BY articles.publishedAt DESC, articles.id DESC',
        'LIMIT ? OFFSET ?',
    ]
        .filter(Boolean)
        .join('\n');
    params.push(normalizedLimit, normalizedOffset);

    const rows = await all(sql, params);
    const mapped = rows.map(mapArticleRow);
    if (!includeTopics) {
        return mapped;
    }

    const articleIds = mapped
        .map(article => Number(article.id))
        .filter(articleId => Number.isInteger(articleId) && articleId > 0);
    const topicsByArticleId = await loadTopicsByArticleIds(articleIds, { all });

    return mapped.map(article => ({
        ...article,
        topics: topicsByArticleId.get(Number(article.id)) || [],
    }));
}
