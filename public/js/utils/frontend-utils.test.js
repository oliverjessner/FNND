import assert from 'node:assert/strict';
import test from 'node:test';
import { fingerprintArticles, normalizeIds, normalizeSearch } from './data.js';
import { formatTriageTime, normalizeArticleUrl } from './format.js';

test('normalizes ids without duplicates or invalid values', () => {
    assert.deepEqual(normalizeIds([3, '2', 3, 0, -1, 'nope']), [3, 2]);
});

test('normalizes and bounds search input', () => {
    assert.equal(normalizeSearch('  one   two\nthree  ', 9), 'one two t');
});

test('article fingerprints change for rendered content', () => {
    const base = [{ id: 1, title: 'Before', topics: [] }];
    assert.equal(fingerprintArticles(base), fingerprintArticles([{ ...base[0] }]));
    assert.notEqual(fingerprintArticles(base), fingerprintArticles([{ ...base[0], title: 'After' }]));
});

test('accepts only http article URLs', () => {
    assert.equal(normalizeArticleUrl('https://example.com/a'), 'https://example.com/a');
    assert.equal(normalizeArticleUrl('javascript:alert(1)'), null);
    assert.equal(normalizeArticleUrl('not a url'), null);
});

test('formats relative triage dates deterministically', () => {
    const reference = new Date('2026-08-29T12:00:00Z');
    assert.match(formatTriageTime('2026-08-28T10:30:00Z', reference), /^Yesterday · /);
    assert.equal(formatTriageTime('invalid', reference), '—');
});
