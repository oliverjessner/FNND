function compactArticle(article) {
    return {
        id: article.id,
        title: article.title,
        url: article.url,
        publishedAt: article.publishedAt,
        sourceName: article.sourceName,
    };
}

export function formatLastArticles(articles, { url = false, title = false } = {}) {
    if (url || title) {
        return articles
            .map(article => {
                if (url && title) {
                    return `${article.url || ''}\t${article.title || ''}`;
                }
                return String(url ? article.url || '' : article.title || '');
            })
            .join('\n');
    }

    return JSON.stringify(articles.map(compactArticle), null, 2);
}

export function formatDigest(payload, count) {
    const clusters = (Array.isArray(payload?.clusters) ? payload.clusters : []).slice(0, count).map(cluster => ({
        title: cluster.clusterTitle || cluster.representative?.title || 'Ohne Titel',
        count: Number(cluster.clusterCount || cluster.items?.length || 0),
        articles: (Array.isArray(cluster.items) ? cluster.items : []).map(article => ({
            title: article.title,
            url: article.url,
            sourceName: article.sourceName,
            publishedAt: article.publishedAt,
        })),
    }));

    return JSON.stringify(clusters, null, 2);
}
