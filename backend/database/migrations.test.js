import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

const testDirectory = await mkdtemp(path.join(os.tmpdir(), 'no-bullshit-rss-migrations-'));
process.env.DB_PATH = path.join(testDirectory, 'migrations.sqlite');

const database = await import('./datenbank.js');

test.after(async () => {
    await database.closeDatabase();
    await rm(testDirectory, { recursive: true, force: true });
});

test('applies versioned migrations in order and only once', async () => {
    await database.initSchema();

    assert.deepEqual(
        await database.all('SELECT version FROM schema_migrations ORDER BY version'),
        [
            { version: '0001_schema.sql' },
            { version: '0002_feed_names.sql' },
            { version: '0003_bullshit_rules.sql' },
            { version: '0004_default_bullshit_rules.sql' },
        ],
    );

    const feedColumns = await database.all('PRAGMA table_info(feeds)');
    assert.equal(feedColumns.some(column => column.name === 'name'), true);
    assert.equal((await database.get("SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name = 'bullshit_rules'")).count, 1);
    assert.equal((await database.get("SELECT COUNT(*) AS count FROM sqlite_master WHERE type = 'table' AND name = 'article_bullshit_matches'")).count, 1);
    assert.deepEqual(
        await database.all('SELECT name, enabled, field, operator, value FROM bullshit_rules ORDER BY id'),
        [
            { name: 'Black Friday', enabled: 0, field: 'title', operator: 'contains', value: 'Black Friday' },
            { name: 'Sponsored', enabled: 0, field: 'title', operator: 'contains', value: 'Sponsored' },
            { name: 'Deals', enabled: 0, field: 'url', operator: 'contains', value: '/deals/' },
            { name: 'SEO List', enabled: 0, field: 'title', operator: 'regex', value: '^Die \\d+' },
        ],
    );

    await database.initSchema();

    assert.deepEqual(await database.get('SELECT COUNT(*) AS count FROM schema_migrations'), { count: 4 });
    assert.deepEqual(await database.get('PRAGMA integrity_check'), { integrity_check: 'ok' });
    assert.deepEqual(await database.all('PRAGMA foreign_key_check'), []);
});
