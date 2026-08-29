import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

const testDirectory = await mkdtemp(path.join(os.tmpdir(), 'no-bullshit-rss-feeds-'));
process.env.DB_PATH = path.join(testDirectory, 'feeds.db');

const database = await import('../database/datenbank.js');
const { createFeedRecord, updateFeedRecord } = await import('./feeds.js');

await database.initSchema();

test('keeps feed names independent when feeds share a website source', async () => {
    const websiteUrl = 'https://www.businessinsider.de/';
    const techId = await createFeedRecord({
        name: 'Business Insider Tech',
        websiteUrl,
        feedUrl: 'https://www.businessinsider.de/tech/feed/',
    });
    const startupId = await createFeedRecord({
        name: 'Business Insider Startups',
        websiteUrl,
        feedUrl: 'https://www.businessinsider.de/wirtschaft/startups/feed/',
    });

    assert.equal(Number((await database.get('SELECT COUNT(*) AS count FROM sources')).count), 1);
    assert.deepEqual(
        await database.all('SELECT id, name FROM feeds ORDER BY id'),
        [
            { id: techId, name: 'Business Insider Tech' },
            { id: startupId, name: 'Business Insider Startups' },
        ],
    );

    await updateFeedRecord({
        id: techId,
        name: 'BI Tech',
        websiteUrl,
        feedUrl: 'https://www.businessinsider.de/tech/feed/',
    });

    assert.deepEqual(
        await database.all('SELECT id, name FROM feeds ORDER BY id'),
        [
            { id: techId, name: 'BI Tech' },
            { id: startupId, name: 'Business Insider Startups' },
        ],
    );
});

test.after(async () => {
    await database.closeDatabase();
    await rm(testDirectory, { recursive: true, force: true });
});
