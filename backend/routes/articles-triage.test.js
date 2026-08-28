import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

const testDirectory = await mkdtemp(path.join(os.tmpdir(), 'no-bullshit-rss-triage-'));
process.env.DB_PATH = path.join(testDirectory, 'triage.db');

const database = await import('../database/datenbank.js');
const { setArticleDismissedState, setArticlesDigestedStateInTransaction } = await import('./articles.js');

await database.initSchema();
await database.run(
    "INSERT INTO feeds (name, websiteUrl, feedUrl) VALUES ('Test source', 'https://example.com', 'https://example.com/rss')",
);
await database.run(
    `INSERT INTO articles (feedId, title, teaser, url, publishedAt, guidOrHash)
     VALUES (1, 'Test article', 'A useful summary', 'https://example.com/article', datetime('now'), 'test-guid')`,
);
await database.run("INSERT INTO lists (name, description, color) VALUES ('Saved', '', '#111111')");
await database.run('INSERT INTO list_items (listId, articleId) VALUES (1, 1)');

test('persists saved, dismissed and digested triage states with undo', async () => {
    const initial = await database.get(
        `SELECT articles.dismissedAt,
         EXISTS (SELECT 1 FROM list_items WHERE list_items.articleId = articles.id) AS saved
         FROM articles WHERE articles.id = 1`,
    );
    assert.equal(initial.saved, 1);
    assert.equal(initial.dismissedAt, null);

    assert.equal(await setArticleDismissedState(1, true), true);
    assert.equal(Number((await database.get('SELECT COUNT(*) AS total FROM articles WHERE dismissedAt IS NULL')).total), 0);

    assert.equal(await setArticleDismissedState(1, false), true);
    assert.equal(Number((await database.get('SELECT COUNT(*) AS total FROM articles WHERE dismissedAt IS NULL')).total), 1);

    await setArticlesDigestedStateInTransaction([1], true);
    assert.equal(Number((await database.get('SELECT dailyDigested FROM articles WHERE id = 1')).dailyDigested), 1);

    await setArticlesDigestedStateInTransaction([1], false);
    assert.equal(Number((await database.get('SELECT dailyDigested FROM articles WHERE id = 1')).dailyDigested), 0);
});

test.after(async () => {
    await new Promise(resolve => database.default.close(resolve));
    await rm(testDirectory, { recursive: true, force: true });
});
