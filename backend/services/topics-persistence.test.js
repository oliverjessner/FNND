import test, { after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';

const testDirectory = await mkdtemp(path.join(os.tmpdir(), 'nbs-topics-'));
process.env.DB_PATH = path.join(testDirectory, 'topics.sqlite');
process.env.TOPIC_RULES_FILE_PATH = path.join(testDirectory, 'topics.rules.json');

const database = await import('../database/datenbank.js');
const topics = await import('./topics.js');
const { ingestArticle } = await import('./article-ingest.js');

after(async () => {
    await new Promise(resolve => database.default.close(resolve));
    await rm(testDirectory, { recursive: true, force: true });
});

test('topic config persists and reprocessing assigns only eligible topics', async () => {
    await database.initSchema();
    await topics.saveTopicsFromDefinitions([
        {
            slug: 'ai',
            label: 'AI',
            type: 'technology',
            minMatches: 2,
            exclude: ['stock market'],
            strong: ['openai', 'chatgpt'],
            medium: [],
            weak: [],
        },
    ]);

    const [loaded] = await topics.getTopicDefinitions({ force: true });
    assert.equal(loaded.type, 'technology');
    assert.equal(loaded.minMatches, 2);
    assert.deepEqual(loaded.exclude, ['stock market']);

    const source = await database.run(
        `INSERT INTO sources (name, websiteUrl, canonicalWebsiteUrl, createdAt, updatedAt)
         VALUES (?, ?, ?, datetime('now'), datetime('now'))`,
        ['Test feed', 'https://example.test', 'https://example.test/'],
    );
    const feed = await database.run(
        "INSERT INTO feeds (sourceId, feedUrl, createdAt, updatedAt) VALUES (?, ?, datetime('now'), datetime('now'))",
        [source.lastID, 'https://example.test/feed'],
    );
    await ingestArticle({ feedId: feed.lastID, externalId: 'topic-test-article', title: 'OpenAI updates ChatGPT', teaser: 'Both products improve', url: 'https://example.test/article' });
    await database.run("UPDATE articles SET classificationStatus = 'pending'");

    const result = await topics.reprocessTopicClassificationForAllArticles();
    const assignments = await database.all('SELECT topics.slug AS topicSlug FROM article_topics JOIN topics ON topics.id = article_topics.topicId ORDER BY topics.slug');

    assert.equal(result.processed, 1);
    assert.equal(result.topicAssignments, 1);
    assert.deepEqual(assignments.map(row => row.topicSlug), ['ai']);
});
