import assert from 'node:assert/strict';
import test from 'node:test';
import { getDigestPeriodDefinition, getDigestPeriodsForArticle, getRebuildWindow } from './digest-periods.js';

test('uses Europe/Vienna midnight across spring DST', () => {
    const before = getDigestPeriodDefinition('day', '2026-03-28T12:00:00Z', 'Europe/Vienna');
    const transition = getDigestPeriodDefinition('day', '2026-03-29T12:00:00Z', 'Europe/Vienna');
    assert.equal(before.startsAt, '2026-03-27T23:00:00.000Z');
    assert.equal(before.endsAt, '2026-03-28T23:00:00.000Z');
    assert.equal(transition.startsAt, '2026-03-28T23:00:00.000Z');
    assert.equal(transition.endsAt, '2026-03-29T22:00:00.000Z');
});

test('uses Europe/Vienna midnight across autumn DST', () => {
    const transition = getDigestPeriodDefinition('day', '2026-10-25T12:00:00Z', 'Europe/Vienna');
    assert.equal(transition.startsAt, '2026-10-24T22:00:00.000Z');
    assert.equal(transition.endsAt, '2026-10-25T23:00:00.000Z');
});

test('uses ISO Monday weeks across year boundaries', () => {
    const period = getDigestPeriodDefinition('week', '2027-01-01T12:00:00Z', 'Europe/Vienna');
    assert.equal(period.periodKey, '2026-W53');
    assert.equal(period.startsAt, '2026-12-27T23:00:00.000Z');
    assert.equal(period.endsAt, '2027-01-03T23:00:00.000Z');
});

test('assigns each article to day, week and month', () => {
    const periods = getDigestPeriodsForArticle('2026-08-29T12:00:00Z', 'Europe/Vienna');
    assert.deepEqual(periods.map(period => period.type), ['day', 'week', 'month']);
    assert.deepEqual(periods.map(period => period.periodKey), ['2026-08-29', '2026-W35', '2026-08']);
});

test('rebuild window includes seven days and current plus previous week/month', () => {
    const window = getRebuildWindow('2026-08-29T12:00:00Z', 'Europe/Vienna');
    assert.equal(window.dayStartsAt, '2026-08-22T22:00:00.000Z');
    assert.equal(window.weekStartsAt, '2026-08-16T22:00:00.000Z');
    assert.equal(window.monthStartsAt, '2026-06-30T22:00:00.000Z');
});
