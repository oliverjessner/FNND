import express from 'express';
import { all, get, run } from '../database/datenbank.js';
import { auth } from '../middleware/auth.js';
import { articleIdsSchema, positiveIdParamsSchema, requestSchema as schema } from '../middleware/validate-request.js';
import { publish } from '../services/events.js';

const router = express.Router();
const listBodySchema = {
    type: 'object',
    required: ['name'],
    properties: {
        name: { type: 'string', minLength: 1, maxLength: 200 },
        description: { type: 'string', maxLength: 2_000 },
        color: { type: 'string', minLength: 1, maxLength: 64 },
    },
    additionalProperties: true,
};
const articleIdBodySchema = {
    type: 'object',
    required: ['articleId'],
    properties: {
        articleId: {
            anyOf: [
                { type: 'integer', minimum: 1 },
                { type: 'string', pattern: '^[1-9]\\d*$' },
            ],
        },
    },
    additionalProperties: true,
};
const articleIdsBodySchema = {
    type: 'object',
    required: ['articleIds'],
    properties: { articleIds: articleIdsSchema },
    additionalProperties: true,
};
const listItemParamsSchema = {
    type: 'object',
    required: ['id', 'articleId'],
    properties: {
        id: { type: 'string', pattern: '^[1-9]\\d*$' },
        articleId: { type: 'string', pattern: '^[1-9]\\d*$' },
    },
    additionalProperties: true,
};

function normalizeArticleIds(value) {
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

router.get('/', auth, async (_, res) => {
    const lists = await all('SELECT * FROM lists ORDER BY id DESC');
    return res.json(lists);
});

router.post('/', auth, schema.validate({ body: listBodySchema }), async (req, res) => {
    const { name, description, color = '#1d1d1f' } = req.body || {};
    const desc = description ? description.trim() : null;
    const values = [name.trim(), desc, color];
    const sql = `INSERT INTO lists (name, description, color, createdAt, updatedAt) VALUES (?, ?, ?, datetime('now'), datetime('now'))`;

    if (!name) {
        return res.status(400).json({ error: 'name is required' });
    }

    const result = await run(sql, values);
    const list = await get('SELECT * FROM lists WHERE id = ? AND ? = ?', [
        result.lastID,
        req.auth.ownerId,
        'local-owner',
    ]);

    publish('lists.updated', { id: list.id });
    return res.status(201).json(list);
});

router.put('/:id', auth, schema.validate({ params: positiveIdParamsSchema, body: listBodySchema }), async (req, res) => {
    const { id } = req.params;
    const { name, description, color = '#1d1d1f' } = req.body || {};
    const desc = description ? description.trim() : null;
    const values = [name.trim(), desc, color, id];
    const sql = `UPDATE lists
     SET name = ?, description = ?, color = ?, updatedAt = datetime('now')
     WHERE id = ? AND ? = ?`;

    if (!name) {
        return res.status(400).json({ error: 'name is required' });
    }

    const result = await run(sql, [...values, req.auth.ownerId, 'local-owner']);

    if (result.changes === 0) {
        return res.status(404).json({ error: 'List not found' });
    }

    const list = await get('SELECT * FROM lists WHERE id = ? AND ? = ?', [id, req.auth.ownerId, 'local-owner']);
    publish('lists.updated', { id: list.id });
    res.json(list);
});

router.delete('/:id', auth, schema.validate({ params: positiveIdParamsSchema }), async (req, res) => {
    const { id } = req.params;
    const result = await run("DELETE FROM lists WHERE id = ? AND ? = 'local-owner'", [id, req.auth.ownerId]);

    if (result.changes === 0) {
        return res.status(404).json({ error: 'List not found' });
    }

    publish('lists.updated', { id });
    return res.status(204).end();
});

router.post('/:id/items', auth, schema.validate({ params: positiveIdParamsSchema, body: articleIdBodySchema }), async ({ params: { id }, body: { articleId } }, res) => {
    if (!articleId) {
        return res.status(400).json({ error: 'articleId is required' });
    }

    await run(
        `INSERT OR IGNORE INTO list_items (listId, articleId, createdAt)
     VALUES (?, ?, datetime('now'))`,
        [id, articleId],
    );

    publish('lists.items.updated', { listId: id, articleId });
    return res.status(201).json({ ok: true });
});

router.post('/:id/items/bulk', auth, schema.validate({ params: positiveIdParamsSchema, body: articleIdsBodySchema }), async ({ params: { id }, body: { articleIds } }, res) => {
    const normalizedArticleIds = normalizeArticleIds(articleIds);

    if (normalizedArticleIds.length === 0) {
        return res.status(400).json({ error: 'articleIds must contain at least one valid article id' });
    }

    await run('BEGIN IMMEDIATE');
    let inserted = 0;
    try {
        for (const articleId of normalizedArticleIds) {
            const result = await run(
                `INSERT OR IGNORE INTO list_items (listId, articleId, createdAt)
                 VALUES (?, ?, datetime('now'))`,
                [id, articleId],
            );
            inserted += Number(result?.changes || 0);
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

    publish('lists.items.updated', {
        listId: id,
        articleIds: normalizedArticleIds,
        total: normalizedArticleIds.length,
        inserted,
        batch: true,
    });

    return res.status(201).json({
        ok: true,
        total: normalizedArticleIds.length,
        inserted,
    });
});

router.post('/:id/items/bulk-delete', auth, schema.validate({ params: positiveIdParamsSchema, body: articleIdsBodySchema }), async ({ params: { id }, body: { articleIds } }, res) => {
    const normalizedArticleIds = normalizeArticleIds(articleIds);

    if (normalizedArticleIds.length === 0) {
        return res.status(400).json({ error: 'articleIds must contain at least one valid article id' });
    }

    await run('BEGIN IMMEDIATE');
    let removed = 0;
    try {
        for (const articleId of normalizedArticleIds) {
            const result = await run('DELETE FROM list_items WHERE listId = ? AND articleId = ?', [id, articleId]);
            removed += Number(result?.changes || 0);
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

    publish('lists.items.updated', {
        listId: id,
        articleIds: normalizedArticleIds,
        total: normalizedArticleIds.length,
        removed,
        batch: true,
        action: 'removed',
    });

    return res.json({
        ok: true,
        total: normalizedArticleIds.length,
        removed,
    });
});

router.delete('/:id/items/:articleId', auth, schema.validate({ params: listItemParamsSchema }), async ({ params: { id, articleId } }, res) => {
    const result = await run('DELETE FROM list_items WHERE listId = ? AND articleId = ?', [id, articleId]);

    if (result.changes === 0) {
        return res.status(404).json({ error: 'Item not found' });
    }

    publish('lists.items.updated', { listId: id, articleId });
    return res.status(204).end();
});

export default router;
