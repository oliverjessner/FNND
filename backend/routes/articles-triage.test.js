import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

const testDirectory = await mkdtemp(path.join(os.tmpdir(), 'no-bullshit-rss-triage-'));
process.env.DB_PATH = path.join(testDirectory, 'triage.db');

const database = await import('../database/datenbank.js');
const { setArticleDismissedState, setArticlesDigestedStateInTransaction } = await import('./articles.js');
const { ingestArticle } = await import('../services/article-ingest.js');
const { generateDirtyDigestPeriods } = await import('../services/digest-store.js');

await database.initSchema();
await database.run(
    `INSERT INTO sources (name, websiteUrl, canonicalWebsiteUrl, createdAt, updatedAt)
     VALUES ('Test source', 'https://example.com', 'https://example.com/', datetime('now'), datetime('now'))`,
);
await database.run(
    "INSERT INTO feeds (sourceId, feedUrl, createdAt, updatedAt) VALUES (1, 'https://example.com/rss', datetime('now'), datetime('now'))",
);
await ingestArticle({ feedId: 1, externalId: 'test-guid', title: 'Test article', teaser: 'A useful summary', url: 'https://example.com/article' });
await generateDirtyDigestPeriods();
await database.run("INSERT INTO lists (name, description, color, createdAt, updatedAt) VALUES ('Saved', '', '#111111', datetime('now'), datetime('now'))");
await database.run("INSERT INTO list_items (listId, articleId, createdAt) VALUES (1, 1, datetime('now'))");

test('persists saved, dismissed and digested triage states with undo', async () => {
    const initial = await database.get(
        `SELECT article_state.dismissedAt,
         EXISTS (SELECT 1 FROM list_items WHERE list_items.articleId = articles.id) AS saved
         FROM articles JOIN article_state ON article_state.articleId = articles.id WHERE articles.id = 1`,
    );
    assert.equal(initial.saved, 1);
    assert.equal(initial.dismissedAt, null);

    assert.equal(await setArticleDismissedState(1, true), true);
    assert.equal(Number((await database.get('SELECT COUNT(*) AS total FROM article_state WHERE dismissedAt IS NULL')).total), 0);

    assert.equal(await setArticleDismissedState(1, false), true);
    assert.equal(Number((await database.get('SELECT COUNT(*) AS total FROM article_state WHERE dismissedAt IS NULL')).total), 1);

    await setArticlesDigestedStateInTransaction([1], true);
    assert.equal(Number((await database.get('SELECT COUNT(*) AS total FROM digest_cluster_state WHERE completedAt IS NOT NULL')).total), 1);

    await setArticlesDigestedStateInTransaction([1], false);
    assert.equal(Number((await database.get('SELECT COUNT(*) AS total FROM digest_cluster_state WHERE completedAt IS NOT NULL')).total), 0);
});

test.after(async () => {
    await new Promise(resolve => database.default.close(resolve));
    await rm(testDirectory, { recursive: true, force: true });
});
