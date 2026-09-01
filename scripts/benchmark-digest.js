import { clusterDigestArticles } from '../backend/routes/digest.js';

const articleCount = Math.max(1, Number(process.argv[2]) || 8_000);
const budgetMs = Math.max(1, Number(process.env.DIGEST_BENCHMARK_BUDGET_MS) || 2_000);
const now = Date.now();
const articles = Array.from({ length: articleCount }, (_, index) => ({
    id: index + 1,
    title: ['Unique report ', index, ' company-', index].join(''),
    teaser: ['Market technology update ', index].join(''),
    url: `https://example.test/${index}`,
    publishedAt: new Date(now - index * 60_000).toISOString(),
}));
const startedAt = performance.now();
const clusters = clusterDigestArticles(articles);
const durationMs = Math.round((performance.now() - startedAt) * 10) / 10;
const result = { articleCount, clusterCount: clusters.length, durationMs, budgetMs, passed: durationMs <= budgetMs };
console.log(JSON.stringify(result, null, 2));
if (!result.passed) process.exitCode = 1;
