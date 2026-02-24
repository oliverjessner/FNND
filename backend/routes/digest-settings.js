import express from 'express';
import { all, get, run } from '../database/datenbank.js';
import { publish } from '../services/events.js';

const router = express.Router();

function normalizeFeedIds(value) {
    if (!Array.isArray(value)) {
        return [];
    }

    const ids = new Set();
    value.forEach(id => {
        const normalized = Number(id);
        if (Number.isInteger(normalized) && normalized > 0) {
            ids.add(normalized);
        }
    });

    return Array.from(ids);
}

function normalizeBlockedWord(value) {
    return String(value || '')
        .trim()
        .replace(/\s+/g, ' ')
        .slice(0, 120);
}

async function getDigestSettingsPayload() {
    const excludedFeedRows = await all('SELECT feedId FROM digest_excluded_feeds ORDER BY feedId ASC');
    const blockedWordRows = await all('SELECT id, word, createdAt FROM digest_blocked_words ORDER BY id DESC');

    return {
        excludedFeedIds: excludedFeedRows
            .map(row => Number(row.feedId))
            .filter(feedId => Number.isInteger(feedId) && feedId > 0),
        blockedWords: blockedWordRows.map(row => ({
            id: Number(row.id),
            word: row.word,
            createdAt: row.createdAt,
        })),
    };
}

router.get('/', async (_req, res) => {
    return res.json(await getDigestSettingsPayload());
});

router.put('/excluded-feeds', async ({ body }, res) => {
    const incomingFeedIds = normalizeFeedIds(body?.feedIds);

    let validFeedIds = [];
    if (incomingFeedIds.length > 0) {
        const placeholders = incomingFeedIds.map(() => '?').join(', ');
        const rows = await all(`SELECT id FROM feeds WHERE id IN (${placeholders})`, incomingFeedIds);
        const validIdSet = new Set(
            rows.map(row => Number(row.id)).filter(feedId => Number.isInteger(feedId) && feedId > 0),
        );
        validFeedIds = incomingFeedIds.filter(feedId => validIdSet.has(feedId));
    }

    await run('BEGIN IMMEDIATE');
    try {
        await run('DELETE FROM digest_excluded_feeds');
        for (const feedId of validFeedIds) {
            await run(
                `INSERT OR IGNORE INTO digest_excluded_feeds (feedId, createdAt)
                 VALUES (?, datetime('now'))`,
                [feedId],
            );
        }
        await run('COMMIT');
    } catch (err) {
        try {
            await run('ROLLBACK');
        } catch {
            // ignore rollback error to preserve original failure
        }
        throw err;
    }

    publish('digest.settings.updated', { type: 'excluded-feeds', total: validFeedIds.length });

    return res.json(await getDigestSettingsPayload());
});

router.post('/blocked-words', async ({ body }, res) => {
    const word = normalizeBlockedWord(body?.word);

    if (word.length < 2) {
        return res.status(400).json({ error: 'word must be at least 2 characters long' });
    }

    const result = await run(
        `INSERT OR IGNORE INTO digest_blocked_words (word, createdAt)
         VALUES (?, datetime('now'))`,
        [word],
    );

    const row = await get(
        `SELECT id, word, createdAt
         FROM digest_blocked_words
         WHERE word = ? COLLATE NOCASE
         ORDER BY id DESC
         LIMIT 1`,
        [word],
    );

    publish('digest.settings.updated', {
        type: 'blocked-words',
        action: Number(result?.changes || 0) > 0 ? 'added' : 'exists',
        id: Number(row?.id || 0),
    });

    return res.status(Number(result?.changes || 0) > 0 ? 201 : 200).json({
        id: Number(row?.id || 0),
        word: row?.word || word,
        createdAt: row?.createdAt || null,
    });
});

router.delete('/blocked-words/:id', async ({ params: { id } }, res) => {
    const blockedWordId = Number(id);
    if (!Number.isInteger(blockedWordId) || blockedWordId <= 0) {
        return res.status(400).json({ error: 'Invalid blocked word id' });
    }

    const result = await run('DELETE FROM digest_blocked_words WHERE id = ?', [blockedWordId]);

    if (Number(result?.changes || 0) === 0) {
        return res.status(404).json({ error: 'Blocked word not found' });
    }

    publish('digest.settings.updated', { type: 'blocked-words', action: 'deleted', id: blockedWordId });

    return res.status(204).end();
});

export default router;
