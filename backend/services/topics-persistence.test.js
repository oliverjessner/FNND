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

    const feed = await database.run(
        'INSERT INTO feeds (name, websiteUrl, feedUrl) VALUES (?, ?, ?)',
        ['Test feed', 'https://example.test', 'https://example.test/feed'],
    );
    await database.run(
        'INSERT INTO articles (feedId, title, teaser, content, url, publishedAt, guidOrHash) VALUES (?, ?, ?, ?, ?, ?, ?)',
        [
            feed.lastID,
            'OpenAI updates ChatGPT',
            'Both products improve',
            '',
            'https://example.test/article',
            new Date().toISOString(),
            'topic-test-article',
        ],
    );

    const result = await topics.reprocessTopicClassificationForAllArticles();
    const assignments = await database.all('SELECT topicSlug FROM article_topics ORDER BY topicSlug');

    assert.equal(result.processed, 1);
    assert.equal(result.topicAssignments, 1);
    assert.deepEqual(assignments.map(row => row.topicSlug), ['ai']);
});
