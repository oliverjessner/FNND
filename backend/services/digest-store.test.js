import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

const testDirectory = await mkdtemp(path.join(os.tmpdir(), 'nbs-digest-v2-'));
process.env.DB_PATH = path.join(testDirectory, 'digest-v2.db');
process.env.TOPIC_RULES_FILE_PATH = path.resolve('topics.rules.json');

const database = await import('../database/datenbank.js');
const { ingestArticle } = await import('./article-ingest.js');
const { ensureTopicDefinitionsInitialized } = await import('./topics.js');
const { generateDigestPeriod, generateDirtyDigestPeriods, getStoredDigestPayload, setDigestClustersCompleted } = await import('./digest-store.js');
const { queryArticles } = await import('./article-queries.js');

test.before(async () => {
    await database.initSchema();
    await ensureTopicDefinitionsInitialized();
    await database.run(
        `INSERT INTO sources (name, websiteUrl, canonicalWebsiteUrl, createdAt, updatedAt)
         VALUES ('Source', 'https://example.test', 'https://example.test/', datetime('now'), datetime('now'))`,
    );
    await database.run(
        `INSERT INTO feeds (sourceId, feedUrl, createdAt, updatedAt) VALUES
         (1, 'https://example.test/a.xml', datetime('now'), datetime('now')),
         (1, 'https://example.test/b.xml', datetime('now'), datetime('now'))`,
    );
});

test.after(async () => {
    await database.closeDatabase();
    await rm(testDirectory, { recursive: true, force: true });
});

test('materializes idempotent feed-scoped articles and independent digest state', async () => {
    const publishedAt = new Date().toISOString();
    const first = await ingestArticle({
        feedId: 1,
        externalId: 'shared-guid',
        title: 'OpenAI launches a new model',
        teaser: 'The updated model is available now',
        url: 'https://example.test/story?utm_source=first',
        publishedAt,
    });
    const second = await ingestArticle({
        feedId: 2,
        externalId: 'shared-guid',
        title: 'OpenAI launches its new model',
        teaser: 'The model is available now',
        url: 'https://example.test/story?utm_medium=second',
        publishedAt,
    });
    assert.equal(first.inserted, true);
    assert.equal(second.inserted, true);
    assert.equal(Number((await database.get('SELECT COUNT(*) AS count FROM articles')).count), 2);
    assert.equal(Number((await database.get('SELECT COUNT(*) AS count FROM digest_period_articles')).count), 6);

    const update = await ingestArticle({
        feedId: 1,
        externalId: 'shared-guid',
        title: 'OpenAI launches an updated model',
        teaser: 'The updated model is available now',
        url: 'https://example.test/story?utm_source=first',
        publishedAt,
    });
    assert.equal(update.inserted, false);
    assert.equal(update.changed, true);
    const repeat = await ingestArticle({
        feedId: 1,
        externalId: 'shared-guid',
        title: 'OpenAI launches an updated model',
        teaser: 'The updated model is available now',
        url: 'https://example.test/story?utm_source=first',
        publishedAt,
    });
    assert.equal(repeat.changed, false);
    assert.equal(Number((await database.get('SELECT COUNT(*) AS count FROM articles')).count), 2);

    await generateDirtyDigestPeriods();
    const day = await getStoredDigestPayload('day');
    const week = await getStoredDigestPayload('week');
    assert.equal(day.status, 'ready');
    assert.equal(day.totalArticles, 2);
    assert.equal(day.totalClusters, 1);
    assert.equal(week.totalClusters, 1);

    await setDigestClustersCompleted([first.id], true, 'day');
    assert.equal((await getStoredDigestPayload('day')).totalClusters, 0);
    assert.equal((await getStoredDigestPayload('week')).totalClusters, 1);
    await setDigestClustersCompleted([first.id], false, 'day');
    assert.equal((await getStoredDigestPayload('day')).totalClusters, 1);

    const search = await queryArticles({ query: 'updated model', activeOnly: true }, { all: database.all });
    assert.equal(search.length, 1);
    assert.equal(search[0].id, first.id);

    const firstPage = await queryArticles({ limit: 1, activeOnly: true }, { all: database.all });
    const secondPage = await queryArticles(
        { limit: 1, activeOnly: true, cursorPublishedAt: firstPage[0].publishedAt, cursorId: firstPage[0].id },
        { all: database.all },
    );
    assert.equal(firstPage.length, 1);
    assert.equal(secondPage.length, 1);
    assert.notEqual(firstPage[0].id, secondPage[0].id);

    assert.deepEqual(await database.get('PRAGMA integrity_check'), { integrity_check: 'ok' });
    assert.deepEqual(await database.all('PRAGMA foreign_key_check'), []);
});

test('keeps the previous active generation when a rebuild fails', async () => {
    const period = await database.get("SELECT id, activeGenerationId, startsAt FROM digest_periods WHERE type = 'day' ORDER BY startsAt DESC LIMIT 1");
    const activeGenerationId = Number(period.activeGenerationId);
    await database.run("UPDATE digest_periods SET dirtyAt = datetime('now') WHERE id = ?", [period.id]);
    await database.run(
        `CREATE TRIGGER fail_digest_member_insert BEFORE INSERT ON digest_cluster_articles
         BEGIN SELECT RAISE(ABORT, 'simulated generation failure'); END`,
    );
    await assert.rejects(generateDigestPeriod(period.id), /simulated generation failure/u);
    const periodIdentity = ['day', period.startsAt];
    const afterFailure = await database.get(
        'SELECT status, activeGenerationId, dirtyAt FROM digest_periods WHERE type = ? AND startsAt = ?',
        periodIdentity,
    );
    assert.equal(Number(afterFailure.activeGenerationId), activeGenerationId);
    assert.equal(afterFailure.status, 'ready');
    assert.ok(afterFailure.dirtyAt);
    await database.run('DROP TRIGGER fail_digest_member_insert');
    await generateDigestPeriod(period.id);
    const afterRecovery = await database.get(
        'SELECT status, activeGenerationId, dirtyAt FROM digest_periods WHERE type = ? AND startsAt = ?',
        periodIdentity,
    );
    assert.equal(afterRecovery.status, 'ready');
    assert.notEqual(Number(afterRecovery.activeGenerationId), activeGenerationId);
    assert.equal(afterRecovery.dirtyAt, null);
});
