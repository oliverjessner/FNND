import test from 'node:test';
import assert from 'node:assert/strict';
import { classifyArticleTopicsFromDefinitions } from './topics.js';

const TOPIC_DEFINITIONS = [
    {
        slug: 'ki',
        label: 'KI',
        strong: ['künstliche intelligenz', 'artificial intelligence', 'chatgpt', 'openai', 'claude', 'llm', 'generative ai'],
        medium: ['copilot', 'prompting', 'gemini', 'anthropic', 'embedding'],
        weak: ['modell', 'inferenz', 'fine tuning'],
    },
    {
        slug: 'startup',
        label: 'Startup',
        strong: ['startup', 'venture capital', 'seed-runde', 'series a', 'cap table'],
        medium: ['founder', 'gründung', 'runway', 'pitch deck', 'business angel'],
        weak: ['investment', 'finanzierung'],
    },
];

test('assigns ki from title: ChatGPT verändert den Support-Alltag', async () => {
    const result = await classifyArticleTopicsFromDefinitions(
        {
            title: 'ChatGPT verändert den Support-Alltag',
            teaser: 'Neue Workflows für Teams',
            content: '',
        },
        TOPIC_DEFINITIONS,
    );

    const slugs = result.assignedTopics.map(topic => topic.slug);
    assert.deepEqual(slugs, ['ki']);
    assert.ok(result.assignedTopics[0].score >= 6);
});

test('assigns startup from title: Startup sammelt Seed-Runde', async () => {
    const result = await classifyArticleTopicsFromDefinitions(
        {
            title: 'Startup sammelt Seed-Runde über 5 Mio. Euro ein',
            teaser: 'VC Markt bleibt aktiv',
            content: '',
        },
        TOPIC_DEFINITIONS,
    );

    const slugs = result.assignedTopics.map(topic => topic.slug);
    assert.deepEqual(slugs, ['startup']);
    assert.ok(result.assignedTopics[0].score >= 6);
});

test('assigns ki from body only with OpenAI + LLM + Prompting', async () => {
    const result = await classifyArticleTopicsFromDefinitions(
        {
            title: 'Tech update',
            teaser: 'Modelle im Fokus',
            content: 'OpenAI verbessert sein LLM deutlich. Prompting Patterns und Embedding Strategien werden erklärt.',
        },
        TOPIC_DEFINITIONS,
    );

    const slugs = result.assignedTopics.map(topic => topic.slug);
    assert.deepEqual(slugs, ['ki']);
    assert.ok(result.assignedTopics[0].score >= 6);
});

test('assigns no topic when there are no keyword matches', async () => {
    const result = await classifyArticleTopicsFromDefinitions(
        {
            title: 'Wetter bleibt stabil am Wochenende',
            teaser: 'Regionale Updates ohne Wirtschaftsbezug',
            content: 'Leichte Wolken, wenig Wind, keine besonderen Ereignisse.',
        },
        TOPIC_DEFINITIONS,
    );

    assert.equal(result.assignedTopics.length, 0);
    assert.equal(result.lowConfidenceTopics.length, 0);
});
