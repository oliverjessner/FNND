import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
    all,
    initSchema,
    run,
} from '../backend/database/datenbank.js';
import { ensureTopicDefinitionsInitialized } from '../backend/services/topics.js';
import { ensureRebuildWindowDigestPeriods, generateDirtyDigestPeriods, recoverInterruptedDigestGenerations } from '../backend/services/digest-store.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

async function seedFeeds() {
    const existing = await all('SELECT id FROM feeds LIMIT 1');
    if (existing.length > 0) return;

    const sourcesPath = path.join(__dirname, 'sources.json');
    const raw = await readFile(sourcesPath, 'utf-8');
    const sources = JSON.parse(raw);

    for (const source of sources) {
        if (!source.hasFeed || !Array.isArray(source.feeds)) {
            continue;
        }

        const canonicalWebsiteUrl = new URL(source.url).toString();
        await run(
            `INSERT INTO sources (name, websiteUrl, canonicalWebsiteUrl, createdAt, updatedAt)
             VALUES (?, ?, ?, datetime('now'), datetime('now'))
             ON CONFLICT(canonicalWebsiteUrl) DO UPDATE SET name = excluded.name, websiteUrl = excluded.websiteUrl,
                 updatedAt = datetime('now')`,
            [source.name, source.url, canonicalWebsiteUrl],
        );
        const [sourceRow] = await all('SELECT id FROM sources WHERE canonicalWebsiteUrl = ?', [canonicalWebsiteUrl]);
        for (const feedUrl of source.feeds) {
            await run(
                `INSERT INTO feeds (sourceId, feedUrl, createdAt, updatedAt)
                 VALUES (?, ?, datetime('now'), datetime('now')) ON CONFLICT(feedUrl) DO NOTHING`,
                [sourceRow.id, feedUrl],
            );
        }
    }
}

export async function initDatabase() {
    await initSchema();
    await recoverInterruptedDigestGenerations();
    await ensureTopicDefinitionsInitialized();
    await seedFeeds();
    await ensureRebuildWindowDigestPeriods();
    await generateDirtyDigestPeriods();
    await run('ANALYZE');
    await run('PRAGMA optimize');
}

if (process.argv[1] && process.argv[1].endsWith('init-db.js')) {
    initDatabase()
        .then(() => {
            console.log('DB initialized');
        })
        .catch(err => {
            console.error('DB init failed:', err);
            process.exit(1);
        });
}
