import { Engine } from 'json-rules-engine';
import { access, readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { all, get, run } from '../database/datenbank.js';
import { logInfo, logWarn } from '../utils/logger.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const TOPIC_RULES_FILE_PATH = process.env.TOPIC_RULES_FILE_PATH || path.join(__dirname, '..', '..', 'topics.rules.json');

const TOPIC_KEYWORD_GROUPS = Object.freeze(['strong', 'medium', 'weak']);
const TOPIC_SCORE_WEIGHTS = Object.freeze({ strong: 5, medium: 3, weak: 1 });
const TOPIC_FIELD_MULTIPLIERS = Object.freeze({ title: 2, teaser: 1.5, body: 1 });

const TOPIC_SCORE_ASSIGN_THRESHOLD = 6;
const TOPIC_SCORE_LOW_CONFIDENCE_THRESHOLD = 3;

const TOPIC_CACHE_TTL_MS = 15 * 1000;

const DEFAULT_TOPIC_RULES = {
    ki: {
        label: 'KI',
        strong: ['künstliche intelligenz', 'artificial intelligence', 'chatgpt', 'openai', 'claude', 'llm', 'generative ai'],
        medium: ['copilot', 'prompting', 'gemini', 'anthropic', 'embedding'],
        weak: ['modell', 'inferenz', 'fine tuning'],
    },
    startup: {
        label: 'Startup',
        strong: ['startup', 'venture capital', 'seed-runde', 'series a', 'cap table'],
        medium: ['founder', 'gründung', 'runway', 'pitch deck', 'business angel'],
        weak: ['investment', 'finanzierung'],
    },
};

let topicDefinitionsCache = {
    at: 0,
    definitions: [],
};

let engineCache = {
    signature: '',
    engine: null,
};

function toStableJson(value) {
    return JSON.stringify(value, null, 2);
}

function slugToLabel(slug) {
    return String(slug || '')
        .split(/[-_\s]+/)
        .filter(Boolean)
        .map(part => part.charAt(0).toUpperCase() + part.slice(1))
        .join(' ')
        .trim();
}

function normalizeWhitespace(value) {
    return String(value || '')
        .trim()
        .replace(/\s+/g, ' ');
}

export function normalizeTopicSlug(value) {
    return String(value || '')
        .toLowerCase()
        .trim()
        .replace(/[^a-z0-9_-]+/g, '-')
        .replace(/-+/g, '-')
        .replace(/^-|-$/g, '');
}

function normalizeKeywordToken(value) {
    return String(value || '')
        .trim()
        .toLowerCase()
        .normalize('NFKD')
        .replace(/[\u0300-\u036f]/g, '')
        .replace(/[^\p{L}\p{N}\s-]+/gu, ' ')
        .replace(/[-_]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function normalizeKeywordList(value) {
    if (!Array.isArray(value)) {
        return [];
    }

    const seen = new Set();
    const normalized = [];

    value.forEach(entry => {
        const cleaned = normalizeWhitespace(entry);
        if (!cleaned) {
            return;
        }
        const canonical = normalizeKeywordToken(cleaned);
        if (!canonical || seen.has(canonical)) {
            return;
        }
        seen.add(canonical);
        normalized.push(cleaned);
    });

    return normalized;
}

function normalizeTopicDefinition(slug, rawTopic = {}) {
    const normalizedSlug = normalizeTopicSlug(slug);
    if (!normalizedSlug) {
        throw new Error('Topic slug is required and must contain only a-z, 0-9, -, _');
    }

    const label = normalizeWhitespace(rawTopic.label || slugToLabel(normalizedSlug) || normalizedSlug);
    if (!label) {
        throw new Error(`Topic "${normalizedSlug}" needs a label`);
    }

    const topic = {
        slug: normalizedSlug,
        label,
        strong: normalizeKeywordList(rawTopic.strong),
        medium: normalizeKeywordList(rawTopic.medium),
        weak: normalizeKeywordList(rawTopic.weak),
    };

    const keywordCount = topic.strong.length + topic.medium.length + topic.weak.length;
    if (keywordCount === 0) {
        throw new Error(`Topic "${normalizedSlug}" needs at least one keyword`);
    }

    return topic;
}

function parseObjectTopicMap(input) {
    const entries = Object.entries(input || {});
    if (entries.length === 0) {
        return [];
    }

    return entries.map(([slug, value]) => normalizeTopicDefinition(slug, value));
}

function parseArrayTopicList(input) {
    if (!Array.isArray(input)) {
        return [];
    }

    return input.map(item => normalizeTopicDefinition(item?.slug, item));
}

export function validateAndNormalizeTopicDefinitions(input) {
    const rawTopics = Array.isArray(input) ? parseArrayTopicList(input) : parseObjectTopicMap(input);
    const seen = new Set();

    const normalized = rawTopics.map(topic => {
        if (seen.has(topic.slug)) {
            throw new Error(`Duplicate topic slug: ${topic.slug}`);
        }
        seen.add(topic.slug);
        return topic;
    });

    return normalized.sort((left, right) => left.slug.localeCompare(right.slug));
}

function definitionsToRulesObject(definitions) {
    return definitions.reduce((acc, topic) => {
        acc[topic.slug] = {
            label: topic.label,
            strong: topic.strong,
            medium: topic.medium,
            weak: topic.weak,
        };
        return acc;
    }, {});
}

async function ensureTopicsRulesFileExists() {
    try {
        await access(TOPIC_RULES_FILE_PATH);
    } catch {
        await writeFile(TOPIC_RULES_FILE_PATH, toStableJson(DEFAULT_TOPIC_RULES), 'utf-8');
    }
}

async function readTopicRulesFile() {
    await ensureTopicsRulesFileExists();
    const raw = await readFile(TOPIC_RULES_FILE_PATH, 'utf-8');
    const parsed = JSON.parse(raw);
    return {
        raw,
        definitions: validateAndNormalizeTopicDefinitions(parsed),
    };
}

async function writeTopicRulesFile(definitions) {
    const nextRaw = toStableJson(definitionsToRulesObject(definitions));
    await writeFile(TOPIC_RULES_FILE_PATH, `${nextRaw}\n`, 'utf-8');
    return nextRaw;
}

function parseTopicRow(row) {
    let config = {};
    try {
        config = JSON.parse(row.config_json || '{}');
    } catch {
        config = {};
    }

    return normalizeTopicDefinition(row.slug, {
        label: row.label,
        strong: config.strong,
        medium: config.medium,
        weak: config.weak,
    });
}

async function getTopicDefinitionsFromDatabase() {
    const rows = await all('SELECT id, slug, label, config_json, created_at, updated_at FROM topics ORDER BY slug ASC');
    const definitions = [];
    rows.forEach(row => {
        try {
            definitions.push(parseTopicRow(row));
        } catch (err) {
            logWarn('Skipping invalid topic row', { slug: row.slug, error: err.message });
        }
    });
    return definitions;
}

function invalidateTopicCaches() {
    topicDefinitionsCache = { at: 0, definitions: [] };
    engineCache = { signature: '', engine: null };
}

async function saveTopicDefinitionsToDatabase(definitions) {
    const normalized = validateAndNormalizeTopicDefinitions(definitions);
    const slugs = normalized.map(topic => topic.slug);

    await run('BEGIN IMMEDIATE');
    try {
        for (const topic of normalized) {
            const configJson = JSON.stringify({
                strong: topic.strong,
                medium: topic.medium,
                weak: topic.weak,
            });

            await run(
                `INSERT INTO topics (slug, label, config_json, created_at, updated_at)
                 VALUES (?, ?, ?, datetime('now'), datetime('now'))
                 ON CONFLICT(slug) DO UPDATE SET
                     label = excluded.label,
                     config_json = excluded.config_json,
                     updated_at = datetime('now')`,
                [topic.slug, topic.label, configJson],
            );
        }

        if (slugs.length > 0) {
            const placeholders = slugs.map(() => '?').join(', ');
            await run(`DELETE FROM topics WHERE slug NOT IN (${placeholders})`, slugs);
        } else {
            await run('DELETE FROM topics');
        }

        await run('COMMIT');
    } catch (err) {
        try {
            await run('ROLLBACK');
        } catch {
            // keep original error
        }
        throw err;
    }

    invalidateTopicCaches();
    return normalized;
}

export async function ensureTopicDefinitionsInitialized() {
    const { definitions } = await readTopicRulesFile();
    await saveTopicDefinitionsToDatabase(definitions);
    return definitions;
}

export async function getTopicDefinitions({ force = false } = {}) {
    const now = Date.now();
    if (!force && topicDefinitionsCache.definitions.length > 0 && now - topicDefinitionsCache.at <= TOPIC_CACHE_TTL_MS) {
        return topicDefinitionsCache.definitions;
    }

    let definitions = await getTopicDefinitionsFromDatabase();
    if (definitions.length === 0) {
        definitions = await ensureTopicDefinitionsInitialized();
    }

    topicDefinitionsCache = {
        at: now,
        definitions,
    };

    return definitions;
}

export async function getTopicRowsWithMetadata() {
    const rows = await all('SELECT id, slug, label, config_json, created_at, updated_at FROM topics ORDER BY slug ASC');
    const normalizedRows = [];
    rows.forEach(row => {
        try {
            const parsed = parseTopicRow(row);
            normalizedRows.push({
                id: Number(row.id),
                slug: parsed.slug,
                label: parsed.label,
                strong: parsed.strong,
                medium: parsed.medium,
                weak: parsed.weak,
                createdAt: row.created_at,
                updatedAt: row.updated_at,
            });
        } catch (err) {
            logWarn('Skipping invalid topic row in metadata query', { slug: row.slug, error: err.message });
        }
    });
    return normalizedRows;
}

export async function getTopicRulesPayload() {
    const definitions = await getTopicDefinitions({ force: true });
    const raw = await writeTopicRulesFile(definitions);
    return {
        path: TOPIC_RULES_FILE_PATH,
        raw,
        rules: definitionsToRulesObject(definitions),
        topics: definitions,
    };
}

export async function saveTopicsFromJsonInput(jsonInput) {
    const parsed = typeof jsonInput === 'string' ? JSON.parse(jsonInput) : jsonInput;
    const normalized = validateAndNormalizeTopicDefinitions(parsed);
    await saveTopicDefinitionsToDatabase(normalized);
    const raw = await writeTopicRulesFile(normalized);

    return {
        raw,
        rules: definitionsToRulesObject(normalized),
        topics: normalized,
    };
}

export async function saveTopicsFromDefinitions(definitions) {
    const normalized = validateAndNormalizeTopicDefinitions(definitions);
    await saveTopicDefinitionsToDatabase(normalized);
    await writeTopicRulesFile(normalized);
    return normalized;
}

function stripHtml(value) {
    return String(value || '').replace(/<[^>]*>/g, ' ');
}

function normalizeDocumentScope(value) {
    const normalized = normalizeKeywordToken(stripHtml(value));
    return {
        text: normalized,
        tokenSet: new Set(normalized.split(' ').filter(Boolean)),
    };
}

function escapeRegex(value) {
    return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function keywordMatchesScope(keyword, scope) {
    const normalizedKeyword = normalizeKeywordToken(keyword);
    if (!normalizedKeyword) {
        return false;
    }

    if (normalizedKeyword.includes(' ')) {
        if (!scope.text) {
            return false;
        }
        const pattern = normalizedKeyword
            .split(' ')
            .map(part => escapeRegex(part))
            .join('\\s+');
        const phraseRegex = new RegExp(`(^|\\s)${pattern}(?=\\s|$)`, 'u');
        return phraseRegex.test(scope.text);
    }

    return scope.tokenSet.has(normalizedKeyword);
}

function roundScore(value) {
    return Math.round(Number(value || 0) * 100) / 100;
}

function scoreTopic(topicDefinition, scopes) {
    let score = 0;
    const matchedTerms = [];

    TOPIC_KEYWORD_GROUPS.forEach(group => {
        const terms = Array.isArray(topicDefinition[group]) ? topicDefinition[group] : [];
        const baseWeight = TOPIC_SCORE_WEIGHTS[group] || 0;

        terms.forEach(term => {
            const locations = [];

            Object.entries(TOPIC_FIELD_MULTIPLIERS).forEach(([field, multiplier]) => {
                const scope = scopes[field];
                if (!scope) {
                    return;
                }
                if (!keywordMatchesScope(term, scope)) {
                    return;
                }

                const points = roundScore(baseWeight * multiplier);
                score += points;
                locations.push({ field, points });
            });

            if (locations.length > 0) {
                matchedTerms.push({
                    term,
                    group,
                    baseWeight,
                    locations,
                });
            }
        });
    });

    return {
        score: roundScore(score),
        matchedTerms,
    };
}

function buildEngineCacheSignature(definitions) {
    return JSON.stringify(
        definitions.map(definition => ({
            slug: definition.slug,
            label: definition.label,
        })),
    );
}

function toJsonPathKey(slug) {
    return `$["${String(slug).replace(/"/g, '\\"')}"]`;
}

function buildThresholdEngine(definitions) {
    const engine = new Engine([], { allowUndefinedFacts: true });

    definitions.forEach(definition => {
        const path = toJsonPathKey(definition.slug);

        engine.addRule({
            name: `assign-${definition.slug}`,
            conditions: {
                all: [{ fact: 'topicScores', path, operator: 'greaterThanInclusive', value: TOPIC_SCORE_ASSIGN_THRESHOLD }],
            },
            event: {
                type: 'topic.assign',
                params: { topicSlug: definition.slug },
            },
        });

        engine.addRule({
            name: `low-${definition.slug}`,
            conditions: {
                all: [
                    {
                        fact: 'topicScores',
                        path,
                        operator: 'greaterThanInclusive',
                        value: TOPIC_SCORE_LOW_CONFIDENCE_THRESHOLD,
                    },
                    { fact: 'topicScores', path, operator: 'lessThan', value: TOPIC_SCORE_ASSIGN_THRESHOLD },
                ],
            },
            event: {
                type: 'topic.lowConfidence',
                params: { topicSlug: definition.slug },
            },
        });
    });

    return engine;
}

function getThresholdEngine(definitions) {
    const signature = buildEngineCacheSignature(definitions);
    if (engineCache.signature === signature && engineCache.engine) {
        return engineCache.engine;
    }

    engineCache = {
        signature,
        engine: buildThresholdEngine(definitions),
    };

    return engineCache.engine;
}

async function evaluateTopicThresholds(definitions, scoreMap) {
    const engine = getThresholdEngine(definitions);
    const { events } = await engine.run({ topicScores: scoreMap });

    const assigned = new Set();
    const lowConfidence = new Set();

    events.forEach(event => {
        const topicSlug = event?.params?.topicSlug;
        if (!topicSlug) {
            return;
        }
        if (event.type === 'topic.assign') {
            assigned.add(topicSlug);
            lowConfidence.delete(topicSlug);
            return;
        }
        if (event.type === 'topic.lowConfidence' && !assigned.has(topicSlug)) {
            lowConfidence.add(topicSlug);
        }
    });

    return { assigned, lowConfidence };
}

export async function classifyArticleTopicsFromDefinitions(article, definitionsInput) {
    const definitions = validateAndNormalizeTopicDefinitions(definitionsInput);
    if (definitions.length === 0) {
        return {
            assignedTopics: [],
            lowConfidenceTopics: [],
            scoredTopics: [],
        };
    }

    const scopes = {
        title: normalizeDocumentScope(article?.title || ''),
        teaser: normalizeDocumentScope(article?.teaser || article?.summary || ''),
        body: normalizeDocumentScope(article?.content || article?.body || ''),
    };

    const scoredTopics = definitions.map(definition => {
        const scored = scoreTopic(definition, scopes);
        return {
            slug: definition.slug,
            label: definition.label,
            score: scored.score,
            matchedTerms: scored.matchedTerms,
        };
    });

    const topicScoreMap = scoredTopics.reduce((acc, topic) => {
        acc[topic.slug] = topic.score;
        return acc;
    }, {});

    const thresholdResult = await evaluateTopicThresholds(definitions, topicScoreMap);

    const assignedTopics = scoredTopics
        .filter(topic => thresholdResult.assigned.has(topic.slug))
        .map(topic => ({
            ...topic,
            confidence: 'high',
        }));

    const lowConfidenceTopics = scoredTopics
        .filter(topic => thresholdResult.lowConfidence.has(topic.slug))
        .map(topic => ({
            ...topic,
            confidence: 'low',
        }));

    return {
        assignedTopics,
        lowConfidenceTopics,
        scoredTopics,
    };
}

export async function classifyArticleTopics(article, { definitions = null } = {}) {
    const resolvedDefinitions = definitions || (await getTopicDefinitions());
    return classifyArticleTopicsFromDefinitions(article, resolvedDefinitions);
}

export async function replaceArticleTopics(articleId, classification) {
    const normalizedArticleId = Number(articleId);
    if (!Number.isInteger(normalizedArticleId) || normalizedArticleId <= 0) {
        throw new Error('replaceArticleTopics requires a valid article id');
    }

    await run('DELETE FROM article_topics WHERE article_id = ?', [normalizedArticleId]);

    const assignedTopics = Array.isArray(classification?.assignedTopics) ? classification.assignedTopics : [];
    for (const topic of assignedTopics) {
        const matchedTermsJson = JSON.stringify({
            confidence: topic.confidence,
            score: topic.score,
            matchedTerms: topic.matchedTerms,
        });

        await run(
            `INSERT OR REPLACE INTO article_topics
             (article_id, topic_slug, score, matched_terms_json, created_at)
             VALUES (?, ?, ?, ?, datetime('now'))`,
            [normalizedArticleId, topic.slug, topic.score, matchedTermsJson],
        );
    }
}

export async function classifyAndPersistArticleTopics(article) {
    const articleId = Number(article?.id || article?.articleId);
    if (!Number.isInteger(articleId) || articleId <= 0) {
        throw new Error('classifyAndPersistArticleTopics requires article.id or articleId');
    }

    const classification = await classifyArticleTopics(article);
    await replaceArticleTopics(articleId, classification);

    if (classification.assignedTopics.length > 0 || classification.lowConfidenceTopics.length > 0) {
        logInfo('Topic classification complete', {
            articleId,
            assigned: classification.assignedTopics.map(topic => ({ slug: topic.slug, score: topic.score })),
            lowConfidence: classification.lowConfidenceTopics.map(topic => ({ slug: topic.slug, score: topic.score })),
        });
    }

    return classification;
}

export async function getArticleTopics(articleId) {
    const normalizedArticleId = Number(articleId);
    if (!Number.isInteger(normalizedArticleId) || normalizedArticleId <= 0) {
        return [];
    }

    const rows = await all(
        `SELECT article_topics.article_id,
                article_topics.topic_slug,
                article_topics.score,
                article_topics.matched_terms_json,
                article_topics.created_at,
                topics.label
         FROM article_topics
         JOIN topics ON topics.slug = article_topics.topic_slug
         WHERE article_topics.article_id = ?
         ORDER BY article_topics.score DESC, article_topics.topic_slug ASC`,
        [normalizedArticleId],
    );

    return rows.map(row => {
        let matchedTerms = {};
        try {
            matchedTerms = JSON.parse(row.matched_terms_json || '{}');
        } catch {
            matchedTerms = {};
        }
        return {
            articleId: Number(row.article_id),
            topicSlug: row.topic_slug,
            topicLabel: row.label,
            score: Number(row.score || 0),
            matchedTerms,
            createdAt: row.created_at,
        };
    });
}

export async function reprocessTopicClassificationForAllArticles() {
    const definitions = await getTopicDefinitions({ force: true });
    const allArticles = await all('SELECT id, title, teaser, content FROM articles ORDER BY id ASC');

    const counters = {
        processed: 0,
        assignedArticles: 0,
        topicAssignments: 0,
    };

    await run('BEGIN IMMEDIATE');
    try {
        await run('DELETE FROM article_topics');

        for (const article of allArticles) {
            const classification = await classifyArticleTopics(article, { definitions });
            const assignedTopics = classification.assignedTopics || [];

            if (assignedTopics.length > 0) {
                counters.assignedArticles += 1;
            }

            for (const topic of assignedTopics) {
                counters.topicAssignments += 1;
                await run(
                    `INSERT OR REPLACE INTO article_topics
                     (article_id, topic_slug, score, matched_terms_json, created_at)
                     VALUES (?, ?, ?, ?, datetime('now'))`,
                    [
                        article.id,
                        topic.slug,
                        topic.score,
                        JSON.stringify({
                            confidence: topic.confidence,
                            score: topic.score,
                            matchedTerms: topic.matchedTerms,
                        }),
                    ],
                );
            }

            counters.processed += 1;
            if (counters.processed % 250 === 0) {
                logInfo('Topic reprocess progress', {
                    processed: counters.processed,
                    total: allArticles.length,
                });
            }
        }

        await run('COMMIT');
    } catch (err) {
        try {
            await run('ROLLBACK');
        } catch {
            // keep original error
        }
        throw err;
    }

    logInfo('Topic reprocess complete', {
        processed: counters.processed,
        assignedArticles: counters.assignedArticles,
        topicAssignments: counters.topicAssignments,
    });

    return {
        ...counters,
        totalArticles: allArticles.length,
        topicCount: definitions.length,
    };
}

export async function upsertTopic(topicInput, { existingSlug = null } = {}) {
    const current = await getTopicDefinitions({ force: true });
    const baseSlug = normalizeTopicSlug(existingSlug || topicInput?.slug);

    if (existingSlug && !current.some(topic => topic.slug === baseSlug)) {
        throw new Error(`Topic not found: ${existingSlug}`);
    }

    const nextTopic = normalizeTopicDefinition(topicInput?.slug || baseSlug, topicInput);
    const filtered = current.filter(topic => topic.slug !== baseSlug);

    if (filtered.some(topic => topic.slug === nextTopic.slug)) {
        throw new Error(`Topic slug already exists: ${nextTopic.slug}`);
    }

    const nextDefinitions = [...filtered, nextTopic];
    await saveTopicsFromDefinitions(nextDefinitions);

    return nextTopic;
}

export async function deleteTopicBySlug(slug) {
    const normalizedSlug = normalizeTopicSlug(slug);
    if (!normalizedSlug) {
        throw new Error('Invalid topic slug');
    }

    const current = await getTopicDefinitions({ force: true });
    if (!current.some(topic => topic.slug === normalizedSlug)) {
        return false;
    }

    const nextDefinitions = current.filter(topic => topic.slug !== normalizedSlug);
    await saveTopicsFromDefinitions(nextDefinitions);
    return true;
}

export async function getTopicBySlug(slug) {
    const normalizedSlug = normalizeTopicSlug(slug);
    if (!normalizedSlug) {
        return null;
    }

    const row = await get('SELECT id, slug, label, config_json, created_at, updated_at FROM topics WHERE slug = ?', [
        normalizedSlug,
    ]);

    if (!row) {
        return null;
    }

    const parsed = parseTopicRow(row);
    return {
        id: Number(row.id),
        slug: parsed.slug,
        label: parsed.label,
        strong: parsed.strong,
        medium: parsed.medium,
        weak: parsed.weak,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
    };
}

export function getTopicRulesFilePath() {
    return TOPIC_RULES_FILE_PATH;
}

export function getTopicScoringConfig() {
    return {
        weights: TOPIC_SCORE_WEIGHTS,
        fieldMultipliers: TOPIC_FIELD_MULTIPLIERS,
        thresholds: {
            assign: TOPIC_SCORE_ASSIGN_THRESHOLD,
            lowConfidence: TOPIC_SCORE_LOW_CONFIDENCE_THRESHOLD,
        },
    };
}
