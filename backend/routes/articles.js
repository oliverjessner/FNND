import express from 'express';
import simhashPkg from '@biu/simhash';
import { all, get, run } from '../database/datenbank.js';
import { publish } from '../services/events.js';
import { logInfo } from '../utils/logger.js';

const router = express.Router();
const { simhash, similarity } = simhashPkg;

function readNumberEnv(name, fallback) {
    const value = Number(process.env[name]);
    return Number.isFinite(value) ? value : fallback;
}

const DIGEST_SIMILARITY_THRESHOLD_STRICT = readNumberEnv('DIGEST_SIMILARITY_STRICT', 0.76);
const DIGEST_SIMILARITY_THRESHOLD_SOFT = readNumberEnv('DIGEST_SIMILARITY_SOFT', 0.64);
const DIGEST_TOKEN_OVERLAP_THRESHOLD = readNumberEnv('DIGEST_TOKEN_OVERLAP', 0.26);
const DIGEST_STRICT_MIN_TOKEN_OVERLAP = readNumberEnv('DIGEST_STRICT_MIN_OVERLAP', 0.25);
const DIGEST_STRICT_ANCHOR_MIN_MATCH = readNumberEnv('DIGEST_STRICT_ANCHOR_MIN_MATCH', 1);
const DIGEST_TITLE_ANCHOR_MIN_MATCH = readNumberEnv('DIGEST_TITLE_ANCHOR_MIN_MATCH', 2);
const DIGEST_SOFT_MATCH_MAX_HOURS = readNumberEnv('DIGEST_SOFT_MAX_HOURS', 18);
const DIGEST_TITLE_ANCHOR_STOPWORDS = new Set([
    'a',
    'an',
    'and',
    'are',
    'as',
    'at',
    'auf',
    'aus',
    'be',
    'bei',
    'das',
    'dem',
    'den',
    'der',
    'des',
    'die',
    'ein',
    'eine',
    'einem',
    'einen',
    'einer',
    'es',
    'for',
    'from',
    'für',
    'have',
    'has',
    'how',
    'ich',
    'im',
    'in',
    'into',
    'is',
    'ist',
    'it',
    'mit',
    'nach',
    'noch',
    'not',
    'of',
    'on',
    'or',
    'sind',
    'that',
    'the',
    'this',
    'today',
    'top',
    'und',
    'von',
    'vs',
    'was',
    'watch',
    'were',
    'wie',
    'with',
    'you',
    'your',
    'zu',
    'zum',
    'anzeige',
    'best',
    'deal',
    'first',
    'free',
    'latest',
    'new',
    'review',
    'save',
    'test',
    'tested',
    'amazon',
    'euro',
    'rabatt',
    'prozent',
    'deal',
    'deals',
    'angebot',
    'angebote',
    'unter',
    'weniger',
    'mehr',
    'jetzt',
    'sichern',
    'kaufen',
    'günstig',
    'news',
    'update',
    'updates',
    'breaking',
    'live',
    'video',
    'videos',
    'report',
    'reports',
    'says',
    'said',
    'will',
    'latest',
    'first',
]);
const NORMALIZED_PUBLISHED_AT_SQL = `
    CASE
        WHEN articles.publishedAt IS NULL THEN NULL
        WHEN typeof(articles.publishedAt) IN ('integer', 'real') THEN datetime(
            CASE
                WHEN CAST(articles.publishedAt AS INTEGER) > 9999999999
                    THEN CAST(articles.publishedAt AS INTEGER) / 1000
                ELSE CAST(articles.publishedAt AS INTEGER)
            END,
            'unixepoch'
        )
        WHEN trim(articles.publishedAt) != '' AND trim(articles.publishedAt) NOT GLOB '*[^0-9]*' THEN datetime(
            CASE
                WHEN length(trim(articles.publishedAt)) > 10
                    THEN CAST(trim(articles.publishedAt) AS INTEGER) / 1000
                ELSE CAST(trim(articles.publishedAt) AS INTEGER)
            END,
            'unixepoch'
        )
        ELSE datetime(articles.publishedAt)
    END
`;

function mapArticleRow(row) {
    const logoDataUrl =
        row.sourceLogo && row.sourceLogoMime ? `data:${row.sourceLogoMime};base64,${row.sourceLogo.toString('base64')}` : null;
    const { sourceLogo, sourceLogoMime, ...rest } = row;
    return { ...rest, sourceLogoDataUrl: logoDataUrl };
}

function getTodayRangeIso() {
    const start = new Date();
    start.setHours(0, 0, 0, 0);
    const end = new Date(start);
    end.setDate(end.getDate() + 1);
    return { startIso: start.toISOString(), endIso: end.toISOString() };
}

function stripHtml(value) {
    return String(value || '').replace(/<[^>]*>/g, ' ');
}

function buildFingerprintText(article) {
    return [
        article.title,
        article.teaser,
        article.summary,
        article.description,
        article.contentSnippet,
        article.content_snippet,
        article.content,
    ]
        .filter(value => typeof value === 'string' && value.trim())
        .join(' ');
}

function normalizeFingerprintText(value) {
    return stripHtml(value)
        .toLowerCase()
        .normalize('NFKC')
        .replace(/[^\p{L}\p{N}\s]/gu, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function tokenizeFingerprintText(value, { keepStopwords = false } = {}) {
    return String(value || '')
        .split(' ')
        .map(token => token.trim())
        .filter(token => token.length > 2)
        .filter(token => !/^\d+$/.test(token))
        .filter(token => keepStopwords || !DIGEST_TITLE_ANCHOR_STOPWORDS.has(token));
}

function buildSimhashTokens(tokens) {
    if (tokens.length === 0) {
        return [];
    }

    const tokenWeightMap = new Map();
    tokens.forEach(token => {
        tokenWeightMap.set(token, (tokenWeightMap.get(token) || 0) + 1);
    });

    return Array.from(tokenWeightMap, ([text, weight]) => ({ text, weight }));
}

function toTimestampMs(value) {
    const date = new Date(value);
    const timestamp = date.getTime();
    return Number.isFinite(timestamp) ? timestamp : null;
}

function canonicalizeArticleUrl(value) {
    if (!value) {
        return null;
    }

    try {
        const url = new URL(String(value));
        url.hash = '';
        url.searchParams.delete('utm_source');
        url.searchParams.delete('utm_medium');
        url.searchParams.delete('utm_campaign');
        url.searchParams.delete('utm_term');
        url.searchParams.delete('utm_content');
        url.searchParams.delete('gclid');
        url.searchParams.delete('fbclid');
        return url.toString();
    } catch {
        return null;
    }
}

function getTokenOverlapScore(leftSet, rightSet) {
    if (!leftSet || !rightSet || leftSet.size === 0 || rightSet.size === 0) {
        return 0;
    }

    let intersection = 0;
    leftSet.forEach(token => {
        if (rightSet.has(token)) {
            intersection += 1;
        }
    });

    return intersection / Math.min(leftSet.size, rightSet.size);
}

function getSetIntersectionCount(leftSet, rightSet) {
    if (!leftSet || !rightSet || leftSet.size === 0 || rightSet.size === 0) {
        return 0;
    }

    let intersection = 0;
    leftSet.forEach(token => {
        if (rightSet.has(token)) {
            intersection += 1;
        }
    });

    return intersection;
}

function isAnchorToken(token) {
    if (typeof token !== 'string') {
        return false;
    }
    if (token.length < 5) {
        return false;
    }
    if (DIGEST_TITLE_ANCHOR_STOPWORDS.has(token)) {
        return false;
    }
    if (/^\d+$/.test(token)) {
        return false;
    }
    return true;
}

function isSoftDigestMatchAllowed(
    currentFingerprint,
    clusterFingerprint,
    similarityScore,
    overlapScore,
    titleAnchorOverlapCount,
) {
    if (similarityScore >= DIGEST_SIMILARITY_THRESHOLD_STRICT) {
        return (
            titleAnchorOverlapCount >= DIGEST_STRICT_ANCHOR_MIN_MATCH &&
            overlapScore >= DIGEST_STRICT_MIN_TOKEN_OVERLAP
        );
    }

    if (overlapScore < DIGEST_TOKEN_OVERLAP_THRESHOLD) {
        return false;
    }
    if (titleAnchorOverlapCount < DIGEST_TITLE_ANCHOR_MIN_MATCH) {
        return false;
    }

    if (!currentFingerprint.publishedAtMs || !clusterFingerprint.publishedAtMs) {
        return true;
    }

    const maxDistanceMs = DIGEST_SOFT_MATCH_MAX_HOURS * 60 * 60 * 1000;
    return Math.abs(currentFingerprint.publishedAtMs - clusterFingerprint.publishedAtMs) <= maxDistanceMs;
}

function createDigestFingerprint(article) {
    const normalizedText = normalizeFingerprintText(buildFingerprintText(article));
    const words = tokenizeFingerprintText(normalizedText);
    const effectiveWords = words.length > 0 ? words : tokenizeFingerprintText(normalizedText, { keepStopwords: true });
    const normalizedTitle = normalizeFingerprintText(article.title || '');
    const titleWords = tokenizeFingerprintText(normalizedTitle);
    const effectiveTitleWords =
        titleWords.length > 0 ? titleWords : tokenizeFingerprintText(normalizedTitle, { keepStopwords: true });
    const tokenSet = new Set(effectiveWords);
    const titleAnchorSet = new Set(titleWords.filter(isAnchorToken));
    const hash = effectiveWords.length > 0 ? simhash(buildSimhashTokens(effectiveWords)) : null;
    return {
        hash,
        tokenSet,
        titleAnchorSet: titleAnchorSet.size > 0 ? titleAnchorSet : new Set(effectiveTitleWords.filter(isAnchorToken)),
        publishedAtMs: toTimestampMs(article.publishedAt),
        canonicalUrl: canonicalizeArticleUrl(article.url),
    };
}

function clusterDigestArticles(articles) {
    const clusters = [];
    const clusterByCanonicalUrl = new Map();

    articles.forEach(article => {
        const fingerprint = createDigestFingerprint(article);
        const canonicalUrl = fingerprint.canonicalUrl;
        let matchedClusterIndex = canonicalUrl ? clusterByCanonicalUrl.get(canonicalUrl) : undefined;
        let matchedCluster = Number.isInteger(matchedClusterIndex) ? clusters[matchedClusterIndex] : null;

        if (!matchedCluster && fingerprint.hash) {
            let bestCluster = null;
            let bestClusterIndex = -1;
            let bestSimilarity = -1;
            let bestOverlap = -1;
            let bestAnchorMatches = -1;

            for (let clusterIndex = 0; clusterIndex < clusters.length; clusterIndex += 1) {
                const cluster = clusters[clusterIndex];

                for (const member of cluster.members) {
                    if (!member.fingerprint.hash) {
                        continue;
                    }

                    const score = similarity(fingerprint.hash, member.fingerprint.hash);
                    if (score < DIGEST_SIMILARITY_THRESHOLD_SOFT) {
                        continue;
                    }

                    const overlapScore = getTokenOverlapScore(fingerprint.tokenSet, member.fingerprint.tokenSet);
                    const titleAnchorOverlapCount = getSetIntersectionCount(
                        fingerprint.titleAnchorSet,
                        member.fingerprint.titleAnchorSet,
                    );

                    if (
                        !isSoftDigestMatchAllowed(
                            fingerprint,
                            member.fingerprint,
                            score,
                            overlapScore,
                            titleAnchorOverlapCount,
                        )
                    ) {
                        continue;
                    }

                    if (
                        score > bestSimilarity ||
                        (score === bestSimilarity && titleAnchorOverlapCount > bestAnchorMatches) ||
                        (score === bestSimilarity &&
                            titleAnchorOverlapCount === bestAnchorMatches &&
                            overlapScore > bestOverlap)
                    ) {
                        bestCluster = cluster;
                        bestClusterIndex = clusterIndex;
                        bestSimilarity = score;
                        bestOverlap = overlapScore;
                        bestAnchorMatches = titleAnchorOverlapCount;
                    }
                }
            }

            matchedCluster = bestCluster;
            matchedClusterIndex = bestClusterIndex;
        }

        if (matchedCluster) {
            matchedCluster.items.push(article);
            matchedCluster.members.push({ article, fingerprint });
            matchedCluster.clusterCount = matchedCluster.items.length;
            if (canonicalUrl && Number.isInteger(matchedClusterIndex)) {
                clusterByCanonicalUrl.set(canonicalUrl, matchedClusterIndex);
            }
            return;
        }

        const newCluster = {
            representative: article,
            clusterTitle: article.title || 'Ohne Titel',
            clusterCount: 1,
            items: [article],
            members: [{ article, fingerprint }],
        };
        clusters.push(newCluster);
        if (canonicalUrl) {
            clusterByCanonicalUrl.set(canonicalUrl, clusters.length - 1);
        }
    });

    return clusters.map(cluster => ({
        representative: cluster.representative,
        clusterTitle: cluster.clusterTitle,
        clusterCount: cluster.clusterCount,
        items: cluster.items,
    }));
}

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
