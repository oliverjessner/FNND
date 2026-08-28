import {
    clusterDigestArticles,
    getDigestRangeIso,
    mapArticleRow,
    normalizeDigestVariant,
} from '../routes/digest.js';
import { loadTopicsByArticleIds } from './article-queries.js';

export async function buildDigestPayload(variant = 'day', { all }) {
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
          AND articles.dismissedAt IS NULL
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
    const topicsByArticleId = await loadTopicsByArticleIds(articleIds, { all });
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
