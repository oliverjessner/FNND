import { dom } from '../ui/dom.js';
import { store } from '../state/store.js';
import { STORAGE_KEYS } from '../config.js';
import { hide, setPressed, show, text } from '../utils/dom.js';
import { normalizeArticleUrl } from '../utils/format.js';

const viewerState = { open: false, url: null, articleId: null, title: 'Article viewer', message: '' };

function articleById(id) {
    const normalized = Number(id);
    return store.feed.articlesById.get(normalized) || store.digest.articlesById.get(normalized) || null;
}

function setActiveCards() {
    for (const card of document.querySelectorAll('[data-article-id].feed-card, [data-article-id].digest-item-card')) {
        const inActiveView = card.closest(`#view-${store.ui.activeView}`);
        card.classList.toggle('is-active', viewerState.open && Boolean(inActiveView) && Number(card.dataset.articleId) === viewerState.articleId);
    }
}

export function renderViewer() {
    for (const [view, context] of Object.entries({ main: dom.viewer.feed, digest: dom.viewer.digest })) {
        const visible = viewerState.open && store.ui.activeView === view;
        context.layout.dataset.viewerWidth = store.ui.viewerWidth;
        context.layout.classList.toggle('is-viewer-open', visible);
        text(context.title, viewerState.title);
        if (!visible) {
            hide(context.panel); hide(context.frame); hide(context.message);
            context.frame.removeAttribute('src'); context.frame.removeAttribute('data-loaded-url');
            continue;
        }
        show(context.panel);
        if (viewerState.url) {
            if (context.frame.dataset.loadedUrl !== viewerState.url) {
                context.frame.src = viewerState.url;
                context.frame.dataset.loadedUrl = viewerState.url;
            }
            hide(context.message); show(context.frame);
        } else {
            context.frame.removeAttribute('src'); context.frame.removeAttribute('data-loaded-url');
            hide(context.frame); text(context.message, viewerState.message || 'No article selected.'); show(context.message);
        }
    }
    setPressed(dom.viewer.widthOptions, option => option.dataset.viewerWidth === store.ui.viewerWidth);
    setActiveCards();
}

export function openArticle(id) {
    const article = articleById(id);
    if (!article) return;
    viewerState.open = true;
    viewerState.articleId = Number(article.id);
    viewerState.url = normalizeArticleUrl(article.url);
    viewerState.title = String(article.title || 'Untitled');
    viewerState.message = viewerState.url ? '' : 'This article has no valid URL for the in-app viewer.';
    renderViewer();
}

export function openExternal(id, fallbackUrl = '') {
    const article = articleById(id);
    const url = normalizeArticleUrl(article?.url || fallbackUrl);
    if (url) { window.open(url, '_blank', 'noopener,noreferrer'); return; }
    viewerState.open = true; viewerState.articleId = Number(article?.id) || null; viewerState.url = null;
    viewerState.title = article?.title || 'Untitled'; viewerState.message = 'This article has no valid URL to open externally.';
    renderViewer();
}

export function closeViewer() {
    Object.assign(viewerState, { open: false, url: null, articleId: null, title: 'Article viewer', message: '' });
    renderViewer();
}

export function isViewerOpen() { return viewerState.open; }

export function initArticleViewer() {
    [dom.viewer.feed.hide, dom.viewer.digest.hide].forEach(button => button?.addEventListener('click', closeViewer));
    dom.viewer.widthOptions.forEach(option => option.addEventListener('click', () => {
        const width = ['35', '50'].includes(option.dataset.viewerWidth) ? option.dataset.viewerWidth : '50';
        store.ui.viewerWidth = width;
        localStorage.setItem(STORAGE_KEYS.viewerWidth, width);
        renderViewer();
    }));
    renderViewer();
}
