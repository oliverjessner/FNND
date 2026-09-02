import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

const testDirectory = await mkdtemp(path.join(os.tmpdir(), 'nbs-article-import-'));
process.env.DB_PATH = path.join(testDirectory, 'article-import.db');
process.env.TOPIC_RULES_FILE_PATH = path.resolve('topics.rules.json');

const database = await import('../database/datenbank.js');
const {
    deriveImportedArticleTitle,
    importArticlesFromUrls,
    isManualImportFeedUrl,
    normalizeImportUrl,
} = await import('./article-import.js');
const { queryArticles } = await import('./article-queries.js');
const { ensureTopicDefinitionsInitialized } = await import('./topics.js');

test.before(async () => {
    await database.initSchema();
    await ensureTopicDefinitionsInitialized();
});

test.after(async () => {
    await database.closeDatabase();
    await rm(testDirectory, { recursive: true, force: true });
});

test('normalizes only public-style HTTP(S) article URLs', () => {
    assert.equal(normalizeImportUrl(' https://example.com/story#comments '), 'https://example.com/story');
    assert.equal(normalizeImportUrl('ftp://example.com/story'), null);
    assert.equal(normalizeImportUrl('not a url'), null);
    assert.equal(normalizeImportUrl('https://user:secret@example.com/story'), null);
});

test('derives a readable local title from the final URL segment', () => {
    assert.equal(deriveImportedArticleTitle('https://example.com/news/hello-world.html'), 'hello world');
    assert.equal(deriveImportedArticleTitle('https://example.com/'), 'example.com');
});

test('imports links through the normal ingest pipeline and creates hidden manual feeds', async () => {
    const result = await importArticlesFromUrls([
        'https://example.com/news/hello-world?utm_source=test',
        'https://example.com/news/hello-world',
        'invalid',
    ]);

    assert.deepEqual(
        { received: result.received, imported: result.imported, duplicates: result.duplicates, invalid: result.invalid, failed: result.failed },
        { received: 3, imported: 1, duplicates: 1, invalid: 1, failed: 0 },
    );
    const feed = await database.get('SELECT name, feedUrl FROM feeds');
    assert.equal(feed.name, 'example.com (Imported)');
    assert.equal(isManualImportFeedUrl(feed.feedUrl), true);

    const rows = await queryArticles({}, { all: database.all });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].title, 'hello world');
    assert.equal(rows[0].sourceName, 'example.com (Imported)');
});

test('re-importing an existing canonical URL does not create another article', async () => {
    const result = await importArticlesFromUrls(['https://example.com/news/hello-world#again']);
    assert.equal(result.imported, 0);
    assert.equal(result.duplicates, 1);
    assert.equal((await database.get('SELECT COUNT(*) AS count FROM articles')).count, 1);
});
