export function normalizeIds(values) {
    const ids = new Set();
    for (const value of Array.isArray(values) ? values : []) {
        const id = Number(value);
        if (Number.isInteger(id) && id > 0) ids.add(id);
    }
    return [...ids];
}

export function normalizeSearch(value, maxLength = 300) {
    return String(value || '').trim().replace(/\s+/g, ' ').slice(0, maxLength);
}

export function debounce(callback, delay = 200) {
    let timer = null;
    const wrapped = (...args) => {
        if (timer) clearTimeout(timer);
        timer = setTimeout(() => { timer = null; callback(...args); }, delay);
    };
    wrapped.cancel = () => { if (timer) clearTimeout(timer); timer = null; };
    return wrapped;
}

export function isAbortError(error) {
    return Boolean(error && (error.name === 'AbortError' || error.code === 'ABORT_ERR' || String(error.message || '').toLowerCase().includes('aborted')));
}

export function fingerprintArticles(articles) {
    if (!articles?.length) return 'empty';
    return articles.map(article => [article.id, article.feedId, article.publishedAt, article.title, article.teaser, article.url, Boolean(article.saved), Boolean(article.bullshit), (article.bullshitRules || []).join(','), (article.topics || []).map(topic => `${topic.slug}:${topic.score}`).join(',')].join('|')).join('||');
}
