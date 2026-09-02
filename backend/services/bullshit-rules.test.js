import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

const testDirectory = await mkdtemp(path.join(os.tmpdir(), 'nbs-bullshit-rules-'));
process.env.DB_PATH = path.join(testDirectory, 'bullshit-rules.db');
process.env.TOPIC_RULES_FILE_PATH = path.resolve('topics.rules.json');

const database = await import('../database/datenbank.js');
const {
    createBullshitRule,
    deleteBullshitRule,
    evaluateArticleAgainstRules,
    getBullshitRule,
    normalizeBullshitRule,
    reevaluateAllArticles,
    ruleMatchesArticle,
    updateBullshitRule,
} = await import('./bullshit-rules.js');
const { ingestArticle } = await import('./article-ingest.js');
const { queryArticles } = await import('./article-queries.js');
const { ensureTopicDefinitionsInitialized } = await import('./topics.js');

const rule = overrides => ({ id: 1, name: 'Rule', enabled: true, field: 'title', operator: 'contains', value: 'noise', ...overrides });

test.before(async () => {
    await database.initSchema();
    await ensureTopicDefinitionsInitialized();
    await database.run(
        `INSERT INTO sources (name, websiteUrl, canonicalWebsiteUrl, createdAt, updatedAt) VALUES
         ('Source A', 'https://a.example', 'https://a.example/', datetime('now'), datetime('now')),
         ('Source B', 'https://b.example', 'https://b.example/', datetime('now'), datetime('now'))`,
    );
    await database.run(
        `INSERT INTO feeds (sourceId, name, feedUrl, createdAt, updatedAt) VALUES
         (1, 'Source A', 'https://a.example/feed', datetime('now'), datetime('now')),
         (2, 'Source B', 'https://b.example/feed', datetime('now'), datetime('now'))`,
    );
});

test.after(async () => {
    await database.closeDatabase();
    await rm(testDirectory, { recursive: true, force: true });
});

test('contains matches case-insensitively', () => {
    assert.equal(ruleMatchesArticle(rule({ value: 'BLACK FRIDAY' }), { title: 'A Black Friday deal' }), true);
});

test('equals compares the complete field case-insensitively', () => {
    assert.equal(ruleMatchesArticle(rule({ field: 'source', operator: 'equals', value: 'source a' }), { sourceName: 'Source A' }), true);
    assert.equal(ruleMatchesArticle(rule({ operator: 'equals', value: 'noise' }), { title: 'noise report' }), false);
});

test('not_contains matches only when the value is absent', () => {
    assert.equal(ruleMatchesArticle(rule({ operator: 'not_contains', value: 'affiliate' }), { title: 'Editorial report' }), true);
    assert.equal(ruleMatchesArticle(rule({ operator: 'not_contains', value: 'affiliate' }), { title: 'Affiliate report' }), false);
});

test('valid regex rules match case-insensitively', () => {
    const regexRule = normalizeBullshitRule({ name: 'SEO list', field: 'title', operator: 'regex', value: '^Die \\d+ besten', enabled: true });
    assert.equal(ruleMatchesArticle(regexRule, { title: 'DIE 10 BESTEN Laptops' }), true);
});

test('invalid regex rules do not crash evaluation', () => {
    assert.throws(() => normalizeBullshitRule({ name: 'Broken', field: 'title', operator: 'regex', value: '([', enabled: true }), /Invalid regular expression/u);
    assert.doesNotThrow(() => ruleMatchesArticle(rule({ operator: 'regex', value: '([' }), { title: 'Anything' }));
    assert.equal(ruleMatchesArticle(rule({ operator: 'regex', value: '([' }), { title: 'Anything' }), false);
});

test('disabled rules are ignored', () => {
    assert.equal(ruleMatchesArticle(rule({ enabled: false }), { title: 'Noise' }), false);
});

test('one article can match multiple rules', () => {
    const matches = evaluateArticleAgainstRules(
        { title: 'Sponsored Black Friday', url: 'https://example.com/deals/one' },
        [rule({ id: 1, value: 'sponsored' }), rule({ id: 2, value: 'black friday' }), rule({ id: 3, field: 'url', value: '/deals/' })],
    );
    assert.deepEqual(matches.map(item => item.id), [1, 2, 3]);
});

test('new articles are evaluated automatically during ingest', async () => {
    const sponsored = await createBullshitRule({ name: 'Sponsored', field: 'title', operator: 'contains', value: 'Sponsored', enabled: true });
    const ingested = await ingestArticle({
        feedId: 1,
        externalId: 'sponsored-one',
        title: 'SPONSORED: New laptop',
        teaser: 'Promotion',
        url: 'https://a.example/sponsored-one',
        publishedAt: '2026-09-02T10:00:00.000Z',
    });
    const matches = await database.all('SELECT ruleId FROM article_bullshit_matches WHERE articleId = ?', [ingested.id]);
    assert.deepEqual(matches, [{ ruleId: sponsored.id }]);
});

test('central re-evaluation applies rule changes to existing articles', async () => {
    const existing = await ingestArticle({
        feedId: 1,
        externalId: 'affiliate-one',
        title: 'Affiliate recommendation',
        url: 'https://a.example/affiliate-one',
        publishedAt: '2026-09-02T09:00:00.000Z',
    });
    const sponsored = await database.get("SELECT id FROM bullshit_rules WHERE name = 'Sponsored' AND enabled = 1 ORDER BY id DESC LIMIT 1");
    await updateBullshitRule(sponsored.id, { value: 'Affiliate' });
    const result = await reevaluateAllArticles();
    assert.equal(result.matchedArticles >= 1, true);
    assert.equal(Number((await database.get('SELECT COUNT(*) AS count FROM article_bullshit_matches WHERE articleId = ?', [existing.id])).count), 1);
});

test('deleting a rule removes its persisted matches', async () => {
    const sponsored = await database.get("SELECT id FROM bullshit_rules WHERE name = 'Sponsored' AND enabled = 1 ORDER BY id DESC LIMIT 1");
    assert.equal(await deleteBullshitRule(sponsored.id), true);
    assert.equal((await getBullshitRule(sponsored.id)), null);
    assert.equal(Number((await database.get('SELECT COUNT(*) AS count FROM article_bullshit_matches WHERE ruleId = ?', [sponsored.id])).count), 0);
});

test('clean, bullshit, and default article queries use persisted status', async () => {
    const noiseRule = await createBullshitRule({ name: 'Noise', field: 'title', operator: 'contains', value: 'noise', enabled: true });
    const noisy = await ingestArticle({ feedId: 1, externalId: 'nvidia-noise', title: 'Nvidia AI noise', url: 'https://a.example/nvidia-noise', publishedAt: '2026-09-02T08:00:00.000Z' });
    const clean = await ingestArticle({ feedId: 1, externalId: 'nvidia-clean', title: 'Nvidia AI clean', url: 'https://a.example/nvidia-clean', publishedAt: '2026-09-02T07:00:00.000Z' });
    await ingestArticle({ feedId: 2, externalId: 'other-clean', title: 'Other clean', url: 'https://b.example/other-clean', publishedAt: '2026-09-02T06:00:00.000Z' });
    await reevaluateAllArticles();

    const allRows = await queryArticles({ query: 'Nvidia', activeOnly: true }, { all: database.all });
    const cleanRows = await queryArticles({ query: 'Nvidia', bullshit: 'clean', activeOnly: true }, { all: database.all });
    const bullshitRows = await queryArticles({ query: 'Nvidia', bullshit: 'bullshit', activeOnly: true }, { all: database.all });
    assert.deepEqual(new Set(allRows.map(item => item.id)), new Set([noisy.id, clean.id]));
    assert.deepEqual(cleanRows.map(item => item.id), [clean.id]);
    assert.deepEqual(bullshitRows.map(item => item.id), [noisy.id]);
    assert.equal(bullshitRows[0].bullshit, true);
    assert.deepEqual(bullshitRows[0].bullshitRules, [noiseRule.name]);
});

test('search, topic, and bullshit filters combine in one query', async () => {
    const topic = await database.get("SELECT id FROM topics WHERE slug = 'ki'");
    const articles = await database.all("SELECT id FROM articles WHERE externalId IN ('nvidia-noise', 'nvidia-clean') ORDER BY id");
    for (const article of articles) {
        await database.run(
            `INSERT OR REPLACE INTO article_topics (articleId, topicId, score, matchedTermsJson, classificationVersion, createdAt)
             VALUES (?, ?, 10, '{}', 'test', datetime('now'))`,
            [article.id, topic.id],
        );
    }
    const rows = await queryArticles({ query: 'Nvidia', topic: 'ki', bullshit: 'bullshit', activeOnly: true }, { all: database.all });
    assert.equal(rows.length, 1);
    assert.equal(rows[0].title, 'Nvidia AI noise');
});

test('source, list, and bullshit filters combine in one query', async () => {
    const noisy = await database.get("SELECT id FROM articles WHERE externalId = 'nvidia-noise'");
    await database.run("INSERT INTO lists (name, color, createdAt, updatedAt) VALUES ('Noise list', '#111111', datetime('now'), datetime('now'))");
    const list = await database.get("SELECT id FROM lists WHERE name = 'Noise list'");
    await database.run('INSERT INTO list_items (listId, articleId, createdAt) VALUES (?, ?, datetime(\'now\'))', [list.id, noisy.id]);
    const rows = await queryArticles({ feedId: 1, listId: list.id, bullshit: 'bullshit', activeOnly: true }, { all: database.all });
    assert.deepEqual(rows.map(item => item.id), [noisy.id]);
});
