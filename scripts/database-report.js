import sqlite3 from '@vscode/sqlite3';
import { stat } from 'node:fs/promises';
import path from 'node:path';
import { getDigestPeriodDefinition, getDigestTimezone } from '../backend/services/digest-periods.js';

const databasePath = path.resolve(process.argv[2] || process.env.DB_PATH || 'data-v2.db');

function openReadOnly(filename) {
    return new Promise((resolve, reject) => {
        const database = new sqlite3.Database(filename, sqlite3.OPEN_READONLY, error => (error ? reject(error) : resolve(database)));
    });
}

function get(database, sql, params = []) {
    return new Promise((resolve, reject) => database.get(sql, params, (error, row) => (error ? reject(error) : resolve(row))));
}

function all(database, sql, params = []) {
    return new Promise((resolve, reject) => database.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows))));
}

function close(database) {
    return new Promise((resolve, reject) => database.close(error => (error ? reject(error) : resolve())));
}

async function timed(label, operation) {
    const start = performance.now();
    const value = await operation();
    return { label, durationMs: Math.round((performance.now() - start) * 100) / 100, rows: Array.isArray(value) ? value.length : undefined };
}

const database = await openReadOnly(databasePath);
try {
    const tables = new Set((await all(database, "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')")).map(row => row.name));
    const articleColumns = new Set((await all(database, 'PRAGMA table_info(articles)')).map(row => row.name));
    const isV2 = tables.has('digest_periods') && articleColumns.has('externalId');
    const info = await stat(databasePath);
    const articleSummary = await get(database, 'SELECT COUNT(*) AS count, MIN(publishedAt) AS oldest, MAX(publishedAt) AS newest FROM articles');
    const feedCount = Number((await get(database, 'SELECT COUNT(*) AS count FROM feeds'))?.count || 0);
    const topicAssignments = tables.has('article_topics')
        ? Number((await get(database, 'SELECT COUNT(*) AS count FROM article_topics'))?.count || 0)
        : 0;
    const timings = [];
    timings.push(await timed('latest-feed-100', () => all(database, 'SELECT id, title, url, publishedAt FROM articles ORDER BY publishedAt DESC, id DESC LIMIT 100')));
    if (isV2 && tables.has('articles_fts')) {
        timings.push(await timed('text-search-openai', () => all(database, "SELECT rowid FROM articles_fts WHERE articles_fts MATCH '\"openai\"*' LIMIT 100")));
    } else {
        timings.push(await timed('text-search-openai', () => all(database, "SELECT id FROM articles WHERE title LIKE '%OpenAI%' OR teaser LIKE '%OpenAI%' LIMIT 100")));
    }
    for (const type of ['day', 'week', 'month']) {
        const period = getDigestPeriodDefinition(type, new Date(), getDigestTimezone());
        if (isV2) {
            timings.push(await timed(`${type}-digest`, () => all(database,
                `SELECT digest_clusters.id FROM digest_periods
                 LEFT JOIN digest_clusters ON digest_clusters.digestGenerationId = digest_periods.activeGenerationId
                 WHERE digest_periods.type = ? AND digest_periods.startsAt = ?`, [type, period.startsAt])));
        } else {
            timings.push(await timed(`${type}-article-scan`, () => all(database,
                'SELECT id FROM articles WHERE publishedAt >= ? AND publishedAt < ? ORDER BY publishedAt DESC, id DESC',
                [period.startsAt, period.endsAt])));
        }
    }
    console.log(JSON.stringify({
        databasePath,
        schema: isV2 ? 'v2' : 'legacy',
        bytes: info.size,
        journalMode: (await get(database, 'PRAGMA journal_mode'))?.journal_mode,
        busyTimeout: (await get(database, 'PRAGMA busy_timeout'))?.timeout,
        integrity: (await get(database, 'PRAGMA integrity_check'))?.integrity_check,
        foreignKeyViolations: (await all(database, 'PRAGMA foreign_key_check')).length,
        articles: { count: Number(articleSummary?.count || 0), oldest: articleSummary?.oldest || null, newest: articleSummary?.newest || null },
        feeds: feedCount,
        topicAssignments,
        timings,
    }, null, 2));
} finally {
    await close(database);
}
