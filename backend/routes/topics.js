import express from 'express';
import { publish } from '../services/events.js';
import { auth } from '../middleware/auth.js';
import {
    classifyArticleTopics,
    deleteTopicBySlug,
    getArticleTopics,
    getTopicBySlug,
    getTopicDefinitions,
    getTopicRowsWithMetadata,
    getTopicRulesFilePath,
    getTopicRulesPayload,
    getTopicScoringConfig,
    normalizeTopicSlug,
    reprocessTopicClassificationForAllArticles,
    saveTopicsFromJsonInput,
    upsertTopic,
    validateAndNormalizeTopicDefinitions,
} from '../services/topics.js';

const router = express.Router();

function parseTopicPayload(body = {}) {
    return {
        slug: body.slug,
        label: body.label,
        strong: Array.isArray(body.strong) ? body.strong : String(body.strong || '').split(/[\n,]/),
        medium: Array.isArray(body.medium) ? body.medium : String(body.medium || '').split(/[\n,]/),
        weak: Array.isArray(body.weak) ? body.weak : String(body.weak || '').split(/[\n,]/),
    };
}

function parseRulesBody(body = {}) {
    if (typeof body?.json === 'string') {
        return JSON.parse(body.json);
    }
    if (typeof body?.rules === 'object' && body.rules !== null) {
        return body.rules;
    }
    if (typeof body === 'object' && body !== null && Object.keys(body).length > 0) {
        return body;
    }
    throw new Error('Provide `json` string or `rules` object');
}

router.get('/', auth, async (_req, res) => {
    const topics = await getTopicRowsWithMetadata();
    return res.json({
        topics,
        scoring: getTopicScoringConfig(),
        rulesFilePath: getTopicRulesFilePath(),
    });
});

router.get('/rules', auth, async (_req, res) => {
    return res.json(await getTopicRulesPayload());
});

router.post('/validate', auth, async ({ body }, res) => {
    try {
        const parsedRules = parseRulesBody(body);
        const topics = validateAndNormalizeTopicDefinitions(parsedRules);
        return res.json({
            ok: true,
            topicCount: topics.length,
            topics,
            scoring: getTopicScoringConfig(),
        });
    } catch (err) {
        return res.status(400).json({ error: err.message || 'Invalid rules payload' });
    }
});

router.put('/rules', auth, async ({ body }, res) => {
    try {
        const parsedRules = parseRulesBody(body);
        const result = await saveTopicsFromJsonInput(parsedRules);

        publish('topics.updated', {
            source: 'rules',
            topicCount: result.topics.length,
        });

        return res.json({
            ok: true,
            ...result,
            scoring: getTopicScoringConfig(),
        });
    } catch (err) {
        return res.status(400).json({ error: err.message || 'Invalid rules payload' });
    }
});

router.post('/', auth, async ({ body }, res) => {
    try {
        const topic = await upsertTopic(parseTopicPayload(body));
        publish('topics.updated', { source: 'create', slug: topic.slug });
        return res.status(201).json({ ok: true, topic });
    } catch (err) {
        return res.status(400).json({ error: err.message || 'Could not create topic' });
    }
});

router.put('/:slug', auth, async ({ params, body }, res) => {
    const currentSlug = normalizeTopicSlug(params.slug);
    if (!currentSlug) {
        return res.status(400).json({ error: 'Invalid topic slug' });
    }

    const existing = await getTopicBySlug(currentSlug);
    if (!existing) {
        return res.status(404).json({ error: 'Topic not found' });
    }

    const payload = parseTopicPayload({
        ...existing,
        ...body,
        slug: body?.slug || existing.slug,
        label: body?.label || existing.label,
    });

    try {
        const topic = await upsertTopic(payload, { existingSlug: currentSlug });
        publish('topics.updated', { source: 'update', slug: currentSlug, nextSlug: topic.slug });
        return res.json({ ok: true, topic });
    } catch (err) {
        return res.status(400).json({ error: err.message || 'Could not update topic' });
    }
});

router.delete('/:slug', auth, async ({ params }, res) => {
    const slug = normalizeTopicSlug(params.slug);
    if (!slug) {
        return res.status(400).json({ error: 'Invalid topic slug' });
    }

    try {
        const removed = await deleteTopicBySlug(slug);
        if (!removed) {
            return res.status(404).json({ error: 'Topic not found' });
        }
    } catch (err) {
        return res.status(400).json({ error: err.message || 'Could not delete topic' });
    }

    publish('topics.updated', { source: 'delete', slug });
    return res.status(204).end();
});

router.post('/reprocess', auth, async (_req, res) => {
    try {
        const result = await reprocessTopicClassificationForAllArticles();

        publish('topics.reprocessed', result);
        publish('articles.updated', {
            source: 'topic-reprocess',
            processed: result.processed,
            topicAssignments: result.topicAssignments,
        });

        return res.json({ ok: true, ...result });
    } catch (err) {
        return res.status(500).json({ error: err.message || 'Topic reprocess failed' });
    }
});

router.get('/article/:id', auth, async ({ params }, res) => {
    const articleId = Number(params.id);
    if (!Number.isInteger(articleId) || articleId <= 0) {
        return res.status(400).json({ error: 'Invalid article id' });
    }

    const topics = await getArticleTopics(articleId);
    return res.json({ articleId, topics });
});

router.post('/classify-preview', auth, async ({ body }, res) => {
    const topicDefinitions = await getTopicDefinitions();
    const result = await classifyArticleTopics(
        {
            title: body?.title,
            teaser: body?.teaser,
            content: body?.content,
        },
        { definitions: topicDefinitions },
    );

    return res.json({
        ok: true,
        ...result,
    });
});

export default router;
