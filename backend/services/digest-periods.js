const DEFAULT_TIMEZONE = process.env.DIGEST_TIMEZONE || 'Europe/Vienna';
const PERIOD_TYPES = new Set(['day', 'week', 'month']);

function partsForInstant(value, timezone) {
    const formatter = new Intl.DateTimeFormat('en-CA', {
        timeZone: timezone,
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hourCycle: 'h23',
    });
    const parts = Object.fromEntries(
        formatter
            .formatToParts(value)
            .filter(part => part.type !== 'literal')
            .map(part => [part.type, Number(part.value)]),
    );
    return { year: parts.year, month: parts.month, day: parts.day, hour: parts.hour, minute: parts.minute, second: parts.second };
}

function plainDate(year, month, day) {
    const normalized = new Date(Date.UTC(year, month - 1, day));
    return { year: normalized.getUTCFullYear(), month: normalized.getUTCMonth() + 1, day: normalized.getUTCDate() };
}

function addDays(date, amount) {
    return plainDate(date.year, date.month, date.day + amount);
}

function comparePlainDate(left, right) {
    return Date.UTC(left.year, left.month - 1, left.day) - Date.UTC(right.year, right.month - 1, right.day);
}

function localMidnightUtc(date, timezone) {
    const target = Date.UTC(date.year, date.month - 1, date.day, 0, 0, 0);
    let guess = target;
    for (let attempt = 0; attempt < 4; attempt += 1) {
        const actual = partsForInstant(new Date(guess), timezone);
        const represented = Date.UTC(actual.year, actual.month - 1, actual.day, actual.hour, actual.minute, actual.second);
        const difference = represented - target;
        guess -= difference;
        if (difference === 0) break;
    }
    return new Date(guess);
}

function pad(value) {
    return String(value).padStart(2, '0');
}

function dateKey(date) {
    return `${date.year}-${pad(date.month)}-${pad(date.day)}`;
}

function mondayFor(date) {
    const utc = new Date(Date.UTC(date.year, date.month - 1, date.day));
    const weekday = utc.getUTCDay() || 7;
    return addDays(date, 1 - weekday);
}

function isoWeekKey(monday) {
    const thursday = addDays(monday, 3);
    const firstThursdayAnchor = plainDate(thursday.year, 1, 4);
    const firstMonday = mondayFor(firstThursdayAnchor);
    const week = Math.floor(comparePlainDate(thursday, firstMonday) / 86_400_000 / 7) + 1;
    return `${thursday.year}-W${pad(week)}`;
}

export function normalizeDigestPeriodType(value) {
    const normalized = String(value || 'day').trim().toLowerCase();
    return PERIOD_TYPES.has(normalized) ? normalized : 'day';
}

export function getDigestTimezone() {
    partsForInstant(new Date(), DEFAULT_TIMEZONE);
    return DEFAULT_TIMEZONE;
}

export function getDigestPeriodDefinition(type = 'day', referenceDate = new Date(), timezone = getDigestTimezone()) {
    const normalizedType = normalizeDigestPeriodType(type);
    const instant = referenceDate instanceof Date ? referenceDate : new Date(referenceDate);
    if (!Number.isFinite(instant.getTime())) throw new Error('Invalid digest reference date');
    const local = partsForInstant(instant, timezone);
    let startDate = { year: local.year, month: local.month, day: local.day };
    let endDate;
    let periodKey;

    if (normalizedType === 'week') {
        startDate = mondayFor(startDate);
        endDate = addDays(startDate, 7);
        periodKey = isoWeekKey(startDate);
    } else if (normalizedType === 'month') {
        startDate = { year: local.year, month: local.month, day: 1 };
        endDate = plainDate(local.year, local.month + 1, 1);
        periodKey = `${local.year}-${pad(local.month)}`;
    } else {
        endDate = addDays(startDate, 1);
        periodKey = dateKey(startDate);
    }

    return {
        type: normalizedType,
        periodKey,
        timezone,
        startsAt: localMidnightUtc(startDate, timezone).toISOString(),
        endsAt: localMidnightUtc(endDate, timezone).toISOString(),
    };
}

export function getDigestPeriodsForArticle(publishedAt, timezone = getDigestTimezone()) {
    return ['day', 'week', 'month'].map(type => getDigestPeriodDefinition(type, publishedAt, timezone));
}

export function getRebuildWindow(referenceDate = new Date(), timezone = getDigestTimezone()) {
    const currentDay = getDigestPeriodDefinition('day', referenceDate, timezone);
    const currentWeek = getDigestPeriodDefinition('week', referenceDate, timezone);
    const currentMonth = getDigestPeriodDefinition('month', referenceDate, timezone);
    const dayStart = new Date(currentDay.startsAt);
    const weekStart = new Date(currentWeek.startsAt);
    const monthStart = new Date(currentMonth.startsAt);
    return {
        dayStartsAt: getDigestPeriodDefinition('day', new Date(dayStart.getTime() - 6 * 86_400_000), timezone).startsAt,
        weekStartsAt: getDigestPeriodDefinition('week', new Date(weekStart.getTime() - 24 * 60 * 60 * 1000), timezone).startsAt,
        monthStartsAt: getDigestPeriodDefinition('month', new Date(monthStart.getTime() - 24 * 60 * 60 * 1000), timezone).startsAt,
    };
}

export function isPeriodInsideRebuildWindow(period, referenceDate = new Date()) {
    const window = getRebuildWindow(referenceDate, period.timezone || getDigestTimezone());
    const threshold = period.type === 'week' ? window.weekStartsAt : period.type === 'month' ? window.monthStartsAt : window.dayStartsAt;
    return String(period.startsAt) >= threshold;
}
