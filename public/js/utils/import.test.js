import assert from 'node:assert/strict';
import test from 'node:test';
import { parseArticleImportText } from './import.js';

test('parses a single article URL', () => {
    assert.deepEqual(parseArticleImportText('https://example.com/article#comments'), {
        urls: ['https://example.com/article'], lines: 1, invalid: 0, duplicates: 0, overflow: 0,
    });
});

test('parses a TXT-style link list, removes duplicates, and counts invalid lines', () => {
    const result = parseArticleImportText(`
https://example.com/one
not-a-link
https://example.com/two
https://example.com/one
`);
    assert.deepEqual(result.urls, ['https://example.com/one', 'https://example.com/two']);
    assert.deepEqual({ lines: result.lines, invalid: result.invalid, duplicates: result.duplicates }, { lines: 4, invalid: 1, duplicates: 1 });
});

test('reports URLs beyond the import limit', () => {
    const result = parseArticleImportText('https://example.com/one\nhttps://example.com/two', { maxUrls: 1 });
    assert.deepEqual(result.urls, ['https://example.com/one']);
    assert.equal(result.overflow, 1);
});
