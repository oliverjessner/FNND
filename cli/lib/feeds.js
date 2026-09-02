export async function readStoredFeeds(database) {
    const sourceNameExpression = database.feedNamesAvailable === false
        ? 'sources.name'
        : "COALESCE(NULLIF(feeds.name, ''), sources.name)";

    const selectSql = [
        'SELECT',
        sourceNameExpression,
        `AS name, feeds.feedUrl, sources.websiteUrl
         FROM feeds
         JOIN sources ON sources.id = feeds.sourceId
         WHERE feeds.feedUrl NOT LIKE 'nbs-import:%'
         ORDER BY feeds.id ASC`,
    ].join('\n');

    return database.all(selectSql);
}
