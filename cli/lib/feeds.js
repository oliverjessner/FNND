export async function readStoredFeeds(database) {
    const sourceNameExpression = database.feedNamesAvailable === false
        ? 'sources.name'
        : "COALESCE(NULLIF(feeds.name, ''), sources.name)";

    return database.all(
        `SELECT ${sourceNameExpression} AS name, feeds.feedUrl, sources.websiteUrl
         FROM feeds
         JOIN sources ON sources.id = feeds.sourceId
         ORDER BY feeds.id ASC`,
    );
}
