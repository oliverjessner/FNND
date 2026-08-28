import assert from 'node:assert/strict';
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import sqlite3 from '@vscode/sqlite3';
import { parseCliArgs } from './lib/arguments.js';
import { discoverDatabasePath, getElectronDatabasePath } from './lib/database-path.js';
import { formatDigest, formatLastArticles } from './lib/output.js';
import { runCli } from './main.js';

function createCaptureStream() {
    let value = '';
    return {
        write(chunk) {
            value += String(chunk);
        },
        read() {
            return value;
        },
    };
}

async function createFixtureDatabase(databasePath) {
    const database = await new Promise((resolve, reject) => {
        const connection = new sqlite3.Database(databasePath, error => {
            if (error) reject(error);
            else resolve(connection);
        });
    });
    const exec = sql =>
        new Promise((resolve, reject) => {
            database.exec(sql, error => (error ? reject(error) : resolve()));
        });
    const close = () =>
        new Promise((resolve, reject) => {
            database.close(error => (error ? reject(error) : resolve()));
        });

    await exec(`
        PRAGMA foreign_keys = ON;
        CREATE TABLE feeds (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            logo BLOB,
            logoMime TEXT
        );
        CREATE TABLE articles (
            id INTEGER PRIMARY KEY,
            feedId INTEGER NOT NULL,
            title TEXT,
            teaser TEXT,
            content TEXT,
            url TEXT,
            publishedAt TEXT,
            guidOrHash TEXT NOT NULL,
            dailyDigested INTEGER NOT NULL DEFAULT 0,
            dismissedAt TEXT,
            FOREIGN KEY (feedId) REFERENCES feeds(id)
        );
        CREATE TABLE list_items (listId INTEGER NOT NULL, articleId INTEGER NOT NULL);
        CREATE TABLE digest_excluded_feeds (feedId INTEGER PRIMARY KEY);
        CREATE TABLE digest_blocked_words (id INTEGER PRIMARY KEY, word TEXT NOT NULL);
        CREATE TABLE topics (slug TEXT PRIMARY KEY, label TEXT NOT NULL);
        CREATE TABLE article_topics (
            articleId INTEGER NOT NULL,
            topicSlug TEXT NOT NULL,
            score REAL NOT NULL
        );

        INSERT INTO feeds (id, name) VALUES (1, 'Test Source');
    `);

    const now = new Date();
    const newestTimestamp = new Date(now.getTime() - 60_000).toISOString();
    const olderTimestamp = new Date(now.getTime() - 3_600_000).toISOString();
    await exec(`
        INSERT INTO articles
            (id, feedId, title, teaser, url, publishedAt, guidOrHash, dailyDigested, dismissedAt)
        VALUES
            (1, 1, 'Older', 'Older teaser', 'https://example.com/older', '${olderTimestamp}', 'older', 1, NULL),
            (2, 1, 'Same time lower id', 'Digest topic alpha', 'https://example.com/two', '${newestTimestamp}', 'two', 0, NULL),
            (3, 1, 'Same time higher id', 'Digest topic beta', 'https://example.com/three', '${newestTimestamp}', 'three', 0, datetime('now'));
    `);

    return { close };
}

test('parses article projections and digest ranges', () => {
    assert.deepEqual(parseCliArgs(['articles', 'last', '10', '--url']), {
        command: 'articles-last',
        count: 10,
        url: true,
        title: false,
    });
    assert.deepEqual(parseCliArgs(['articles', 'last', '10', '--title']), {
        command: 'articles-last',
        count: 10,
        url: false,
        title: true,
    });
    assert.deepEqual(parseCliArgs(['articles', 'last', '10', '--titles']), {
        command: 'articles-last',
        count: 10,
        url: false,
        title: true,
    });
    assert.deepEqual(parseCliArgs(['articles', 'last', '10', '--url', '--titles']), {
        command: 'articles-last',
        count: 10,
        url: true,
        title: true,
    });
    assert.deepEqual(parseCliArgs(['articles', 'digest', '5']), {
        command: 'articles-digest',
        count: 5,
        variant: 'day',
    });
    assert.equal(parseCliArgs(['articles', 'digest', '5', '--daily']).variant, 'day');
    assert.equal(parseCliArgs(['articles', 'digest', '5', '--weekly']).variant, 'week');
    assert.equal(parseCliArgs(['articles', 'digest', '5', '--monthly']).variant, 'month');
});

test('rejects invalid counts, flags and multiple digest ranges', () => {
    assert.throws(() => parseCliArgs(['articles', 'last', '0']), /greater than 0/u);
    assert.throws(() => parseCliArgs(['articles', 'last', '-1']), /greater than 0/u);
    assert.throws(() => parseCliArgs(['articles', 'last', 'foo']), /greater than 0/u);
    assert.throws(() => parseCliArgs(['articles', 'last', '1.5']), /greater than 0/u);
    assert.throws(() => parseCliArgs(['articles', 'last', '2', '--weekly']), /Unknown option/u);
    assert.throws(() => parseCliArgs(['unknown', 'last', '2']), /Unknown resource/u);
    assert.throws(() => parseCliArgs(['articles', 'unknown', '2']), /Unknown articles command/u);
    assert.throws(
        () => parseCliArgs(['articles', 'digest', '2', '--daily', '--monthly']),
        /Only one of --daily, --weekly or --monthly/u,
    );
});

test('writes validation errors only to stderr and exits non-zero', async () => {
    const stdout = createCaptureStream();
    const stderr = createCaptureStream();
    const exitCode = await runCli(['articles', 'last', '0'], { stdout, stderr });

    assert.equal(exitCode, 1);
    assert.equal(stdout.read(), '');
    assert.match(stderr.read(), /^Error: .+greater than 0/u);
});

test('formats URL, title, combined and compact JSON output', () => {
    const articles = [
        {
            id: 7,
            title: 'A title',
            url: 'https://example.com/a',
            publishedAt: '2026-01-01T00:00:00.000Z',
            sourceName: 'Example',
            teaser: 'not part of compact output',
        },
        {
            id: 6,
            title: 'Second title',
            url: 'https://example.com/b',
            publishedAt: '2025-12-31T00:00:00.000Z',
            sourceName: 'Example',
        },
    ];
    assert.equal(formatLastArticles(articles, { url: true }), 'https://example.com/a\nhttps://example.com/b');
    assert.equal(formatLastArticles(articles, { title: true }), 'A title\nSecond title');
    assert.equal(
        formatLastArticles(articles, { url: true, title: true }),
        'https://example.com/a\tA title\nhttps://example.com/b\tSecond title',
    );
    assert.deepEqual(JSON.parse(formatLastArticles([articles[0]])), [
        {
            id: 7,
            title: 'A title',
            url: 'https://example.com/a',
            publishedAt: '2026-01-01T00:00:00.000Z',
            sourceName: 'Example',
        },
    ]);
    assert.deepEqual(
        JSON.parse(
            formatDigest(
                {
                    clusters: [
                        {
                            clusterTitle: 'Cluster',
                            clusterCount: 1,
                            items: [articles[0]],
                        },
                    ],
                },
                1,
            ),
        ),
        [
            {
                title: 'Cluster',
                count: 1,
                articles: [
                    {
                        title: 'A title',
                        url: 'https://example.com/a',
                        sourceName: 'Example',
                        publishedAt: '2026-01-01T00:00:00.000Z',
                    },
                ],
            },
        ],
    );
});

test('discovers DB_PATH, repository and Electron databases in priority order', async t => {
    const directory = await mkdtemp(path.join(os.tmpdir(), 'no-bullshit-rss-cli-path-'));
    t.after(() => rm(directory, { recursive: true, force: true }));

    const explicitPath = path.join(directory, 'explicit.db');
    await writeFile(explicitPath, 'fixture');
    assert.equal(await discoverDatabasePath({ cwd: directory, env: { DB_PATH: explicitPath } }), explicitPath);

    const repository = path.join(directory, 'repo');
    const nested = path.join(repository, 'nested');
    await mkdir(nested, { recursive: true });
    await writeFile(path.join(repository, 'package.json'), JSON.stringify({ name: 'no-bullshit-rss' }));
    await writeFile(path.join(repository, 'data.db'), 'fixture');
    assert.equal(await discoverDatabasePath({ cwd: nested, env: {} }), path.join(repository, 'data.db'));

    const homeDir = path.join(directory, 'home');
    const electronPath = getElectronDatabasePath({ platform: 'linux', env: {}, homeDir });
    await mkdir(path.dirname(electronPath), { recursive: true });
    await writeFile(electronPath, 'fixture');
    assert.equal(await discoverDatabasePath({ cwd: directory, env: {}, platform: 'linux', homeDir }), electronPath);

    await assert.rejects(
        discoverDatabasePath({ cwd: directory, env: { DB_PATH: path.join(directory, 'missing.db') } }),
        /Database not found at DB_PATH/u,
    );
});

test('runs article and digest commands against only a temporary DB without writes', async t => {
    const directory = await mkdtemp(path.join(os.tmpdir(), 'no-bullshit-rss-cli-db-'));
    const databasePath = path.join(directory, 'fixture.db');
    const fixture = await createFixtureDatabase(databasePath);
    await fixture.close();
    t.after(() => rm(directory, { recursive: true, force: true }));

    const articleStdout = createCaptureStream();
    const articleStderr = createCaptureStream();
    const articleExitCode = await runCli(['articles', 'last', '3'], {
        stdout: articleStdout,
        stderr: articleStderr,
        cwd: directory,
        env: { DB_PATH: databasePath },
    });
    assert.equal(articleExitCode, 0);
    assert.equal(articleStderr.read(), '');
    assert.deepEqual(
        JSON.parse(articleStdout.read()).map(article => article.id),
        [3, 2, 1],
    );

    const digestStdout = createCaptureStream();
    const digestStderr = createCaptureStream();
    const digestExitCode = await runCli(['articles', 'digest', '10', '--daily'], {
        stdout: digestStdout,
        stderr: digestStderr,
        cwd: directory,
        env: { DB_PATH: databasePath },
    });
    assert.equal(digestExitCode, 0);
    assert.equal(digestStderr.read(), '');
    const clusters = JSON.parse(digestStdout.read());
    assert.equal(clusters.length, 1);
    assert.equal(clusters[0].articles[0].title, 'Same time lower id');

    const verification = await createFixtureReader(databasePath);
    const persisted = await verification.all('SELECT id, dailyDigested, dismissedAt FROM articles ORDER BY id');
    assert.deepEqual(
        persisted.map(row => [row.id, row.dailyDigested, Boolean(row.dismissedAt)]),
        [
            [1, 1, false],
            [2, 0, false],
            [3, 0, true],
        ],
    );
    await verification.close();
});

async function createFixtureReader(databasePath) {
    const database = await new Promise((resolve, reject) => {
        const connection = new sqlite3.Database(databasePath, sqlite3.OPEN_READONLY, error => {
            if (error) reject(error);
            else resolve(connection);
        });
    });
    return {
        all(sql, params = []) {
            return new Promise((resolve, reject) => {
                database.all(sql, params, (error, rows) => (error ? reject(error) : resolve(rows)));
            });
        },
        close() {
            return new Promise((resolve, reject) => {
                database.close(error => (error ? reject(error) : resolve()));
            });
        },
    };
}
