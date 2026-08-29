const dateTime = new Intl.DateTimeFormat('de-DE', { dateStyle: 'medium', timeStyle: 'short' });
const clock = new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit' });
const shortDate = new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric' });

export function formatDate(value) {
    const timestamp = Date.parse(value);
    return value && Number.isFinite(timestamp) ? dateTime.format(timestamp) : '—';
}

export function formatTriageTime(value, referenceDate = new Date()) {
    const date = new Date(value);
    if (!value || !Number.isFinite(date.getTime())) return '—';
    const time = clock.format(date);
    const today = new Date(referenceDate);
    const articleDay = new Date(date);
    today.setHours(0, 0, 0, 0);
    articleDay.setHours(0, 0, 0, 0);
    const days = Math.round((today.getTime() - articleDay.getTime()) / 86400000);
    if (days === 0) return time;
    if (days === 1) return `Yesterday · ${time}`;
    return `${shortDate.format(date)} · ${time}`;
}

export function normalizeArticleUrl(value) {
    try {
        const url = new URL(String(value || '').trim());
        return url.protocol === 'http:' || url.protocol === 'https:' ? url.toString() : null;
    } catch {
        return null;
    }
}
