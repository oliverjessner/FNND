const state = Object.seal({
    feeds: [],
    lists: [],
    topics: [],
    digestSettings: {
        excludedFeedIds: [],
        blockedWords: [],
    },
    editingId: null,
});
const views = document.querySelectorAll('.view');
const navLinks = document.querySelectorAll('.nav-link');
const articlesScroll = document.querySelector('.articles-scroll');
const settingsTabs = document.querySelectorAll('.settings-tab');
const settingsPanels = document.querySelectorAll('.settings-panel');
const settingsTabsWrap = document.querySelector('.settings-tabs-wrap');
const dom = Object.freeze({
    elements: Object.freeze({
        articlesState: document.getElementById('articles-state'),
        articlesList: document.getElementById('articles-list'),
        articlesLoadMore: document.getElementById('articles-load-more'),
        feedsState: document.getElementById('feeds-state'),
        feedsList: document.getElementById('feeds-list'),
        filterList: document.getElementById('filter-list'),
        filterSource: document.getElementById('filter-source'),
        filterTopic: document.getElementById('filter-topic'),
        runFetchBtn: document.getElementById('run-fetch'),
        toggleLayoutBtn: document.getElementById('toggle-layout'),
        layoutOptions: document.querySelectorAll('#toggle-layout .view-toggle-option'),
        feedCount: document.getElementById('feed-count'),
        activeFilterRow: document.getElementById('active-filter-row'),
        activeFilterChips: document.getElementById('active-filter-chips'),
        toastRegion: document.getElementById('toast-region'),
        feedBackToTop: document.getElementById('feed-back-to-top'),
        themeToggleBtn: document.getElementById('theme-toggle'),
        fetchStatus: document.getElementById('fetch-status'),
        articleCountStatus: document.getElementById('article-count-status'),
        settingsFetchNowBtn: document.getElementById('settings-fetch-now'),
        searchInput: document.getElementById('search-input'),
        loadingRow: document.getElementById('loading-row'),
        dashboardLayout: document.getElementById('dashboard-layout'),
        articleViewer: document.getElementById('article-viewer'),
        articleViewerTitle: document.getElementById('article-viewer-title'),
        articleViewerMessage: document.getElementById('article-viewer-message'),
        articleViewerFrame: document.getElementById('article-viewer-frame'),
        articleViewerHideBtn: document.getElementById('article-viewer-hide'),
        articleViewerWidthToggle: document.getElementById('article-viewer-width-toggle'),
        articleViewerWidthOptions: document.querySelectorAll(
            '#article-viewer-width-toggle .article-viewer-width-option',
        ),
        feedForm: document.getElementById('feed-form'),
        feedName: document.getElementById('feed-name'),
        feedWebsite: document.getElementById('feed-website'),
        feedUrl: document.getElementById('feed-url'),
        feedSubmit: document.getElementById('feed-submit'),
        feedCancel: document.getElementById('feed-cancel'),
        feedTest: document.getElementById('feed-test'),
        feedFormStatus: document.getElementById('feed-form-status'),
        listForm: document.getElementById('list-form'),
        listName: document.getElementById('list-name'),
        listDescription: document.getElementById('list-description'),
        listColor: document.getElementById('list-color'),
        listSubmit: document.getElementById('list-submit'),
        listCancel: document.getElementById('list-cancel'),
        listFormStatus: document.getElementById('list-form-status'),
        listsState: document.getElementById('lists-state'),
        listsList: document.getElementById('lists-list'),
        topicsState: document.getElementById('topics-state'),
        topicsList: document.getElementById('topics-list'),
        topicForm: document.getElementById('topic-form'),
        topicSlug: document.getElementById('topic-slug'),
        topicLabel: document.getElementById('topic-label'),
        topicStrong: document.getElementById('topic-strong'),
        topicMedium: document.getElementById('topic-medium'),
        topicWeak: document.getElementById('topic-weak'),
        topicSubmit: document.getElementById('topic-submit'),
        topicCancel: document.getElementById('topic-cancel'),
        topicFormStatus: document.getElementById('topic-form-status'),
        topicsJsonInput: document.getElementById('topics-json-input'),
        topicsJsonValidateBtn: document.getElementById('topics-json-validate'),
        topicsJsonSaveBtn: document.getElementById('topics-json-save'),
        topicsJsonStatus: document.getElementById('topics-json-status'),
        topicsReprocessBtn: document.getElementById('topics-reprocess'),
        topicsReprocessStatus: document.getElementById('topics-reprocess-status'),
    }),
    views: Object.freeze({
        main: document.getElementById('view-main'),
        digest: document.getElementById('view-digest'),
        settings: document.getElementById('view-settings'),
    }),
    templates: Object.freeze({
        feedItem: document.getElementById('feed-item-template'),
        listItem: document.getElementById('list-item-template'),
        articleCard: document.getElementById('article-card-template'),
    }),
    digest: Object.freeze({
        state: document.getElementById('digest-state'),
        list: document.getElementById('digest-list'),
        layout: document.getElementById('digest-layout'),
        viewer: document.getElementById('digest-article-viewer'),
        viewerTitle: document.getElementById('digest-article-viewer-title'),
        viewerMessage: document.getElementById('digest-article-viewer-message'),
        viewerFrame: document.getElementById('digest-article-viewer-frame'),
        viewerHideBtn: document.getElementById('digest-article-viewer-hide'),
        viewerWidthToggle: document.getElementById('digest-article-viewer-width-toggle'),
        viewerWidthOptions: document.querySelectorAll(
            '#digest-article-viewer-width-toggle .article-viewer-width-option',
        ),
        subtitle: document.getElementById('digest-subtitle'),
        markAllBtn: document.getElementById('digest-mark-all'),
        bulkMenu: document.getElementById('digest-bulk-menu'),
        rangeToggle: document.getElementById('digest-range-toggle'),
        rangeOptions: document.querySelectorAll('#digest-range-toggle .digest-range-option'),
        sortToggle: document.getElementById('digest-sort-toggle'),
        sortOptions: document.querySelectorAll('#digest-sort-toggle .digest-sort-option'),
        cluster: document.getElementById('digest-cluster-template'),
        header: document.querySelector('.digest-header'),
        settings: Object.freeze({
            feedsState: document.getElementById('digest-settings-feeds-state'),
            feedsList: document.getElementById('digest-settings-feeds-list'),
            saveFeeds: document.getElementById('digest-settings-save-feeds'),
            blockWordInput: document.getElementById('digest-block-word-input'),
            blockWordAdd: document.getElementById('digest-block-word-add'),
            blockWordsState: document.getElementById('digest-block-words-state'),
            blockWordsList: document.getElementById('digest-block-words-list'),
        }),
    }),
    modal: Object.freeze({
        modalBackdrop: document.getElementById('modal-backdrop'),
        modalListSelect: document.getElementById('modal-list-select'),
        modalClose: document.getElementById('modal-close'),
        modalCancel: document.getElementById('modal-cancel'),
        modalConfirm: document.getElementById('modal-confirm'),
        modalExistingLists: document.getElementById('modal-existing-lists'),
    }),
});
const localStorageKeys = Object.freeze({
    layoutKey: 'fnnd.layout',
    digestSortKey: 'fnnd.digestSort',
    digestRangeKey: 'fnnd.digestRange',
    themeKey: 'fnnd.theme',
    viewerWidthKey: 'fnnd.viewerWidth',
});
const CONFIG = Object.freeze({
    ARTICLES_PAGE_SIZE: 50,
    SEARCH_DEBOUNCE_MS: 400,
    SEARCH_LOADING_DELAY_MS: 150,
    MAX_WORD_LENGTH: 120,
    MAX_SEARCH_QUERY_LENGTH: 300,
    RETRY_COUNT: 3,
    RETRY_DELAY_MS: 500,
});
const digestRuntime = Object.seal({
    lastPayload: null,
    needsRefresh: true,
    loadPromise: null,
    requestId: 0,
    activeRequestController: null,
    activeRequestRange: '',
    lastRenderFingerprint: '',
    pendingMutationEventsToSkip: 0,
    isRangeSwitchLoading: false,
    articleById: new Map(),
});
const articlesRuntime = Object.seal({
    lastRequestKey: '',
    lastRenderFingerprint: '',
    requestId: 0,
    activeRequestController: null,
    articleById: new Map(),
    articles: [],
    hasMore: false,
    isLoadingMore: false,
    focusedArticleId: null,
});
const modalRuntime = Object.seal({
    lastFocusedElement: null,
});
const dateTimeFormatter = new Intl.DateTimeFormat('de-DE', { dateStyle: 'medium', timeStyle: 'short' });
let isListLayout = localStorage.getItem(localStorageKeys.layoutKey) === 'list';
let searchTimer = null;
let searchLoadingTimer = null;
let listEditingId = null;
let topicEditingSlug = null;
let sse = null;
let digestSortDirection = localStorage.getItem(localStorageKeys.digestSortKey) === 'asc' ? 'asc' : 'desc';
let digestRange = normalizeDigestRange(localStorage.getItem(localStorageKeys.digestRangeKey));
let isDarkTheme = localStorage.getItem(localStorageKeys.themeKey) !== 'light';
let articlesNeedsRefresh = true;
let pendingArticleIds = [];
let clearDashboardPromise = null;
let isStickySubnavScrollUpdateScheduled = false;
let viewerOpen = false;
let viewerUrl = null;
let activeArticleId = null;
let viewerTitle = 'Article viewer';
let viewerMessage = '';
let viewerWidth = normalizeViewerWidth(localStorage.getItem(localStorageKeys.viewerWidthKey));
let toastTimer = null;

function normalizeDigestRange(value) {
    const normalized = String(value || '')
        .trim()
        .toLowerCase();

    if (normalized === 'month') {
        return 'month';
    }
    if (normalized === 'week') {
        return 'week';
    }

    return 'day';
}

function normalizeViewerWidth(value) {
    const normalized = String(value || '')
        .trim()
        .replace('%', '');

    if (normalized === '35' || normalized === '25') {
        return '35';
    }

    return '50';
}

function getEffectiveViewerWidth() {
    return viewerWidth;
}

function getViewerWidthOptions() {
    const options = [];
    const dashboardWidthOptions = dom.elements.articleViewerWidthOptions;
    const digestWidthOptions = dom.digest.viewerWidthOptions;

    if (dashboardWidthOptions && dashboardWidthOptions.length > 0) {
        options.push(...dashboardWidthOptions);
    }
    if (digestWidthOptions && digestWidthOptions.length > 0) {
        options.push(...digestWidthOptions);
    }

    return options;
}

function updateViewerWidthUi() {
    const widthOptions = getViewerWidthOptions();
    const effectiveViewerWidth = getEffectiveViewerWidth();

    if (!widthOptions || widthOptions.length === 0) {
        return;
    }

    widthOptions.forEach(option => {
        const isActive = normalizeViewerWidth(option.dataset.viewerWidth) === effectiveViewerWidth;

        option.classList.toggle('is-active', isActive);
        option.setAttribute('aria-pressed', String(isActive));
    });
}

function updateViewerWidthAvailability() {
    const widthOptions = getViewerWidthOptions();

    if (!widthOptions || widthOptions.length === 0) {
        return;
    }

    widthOptions.forEach(option => {
        option.disabled = false;
        option.removeAttribute('title');
    });
}

function applyViewerWidthState() {
    const layoutElements = [dom.elements.dashboardLayout, dom.digest.layout];
    const effectiveViewerWidth = getEffectiveViewerWidth();

    if (!layoutElements.some(Boolean)) {
        return;
    }

    layoutElements.forEach(layoutElement => {
        if (!layoutElement) {
            return;
        }

        layoutElement.dataset.viewerWidth = effectiveViewerWidth;
    });
    updateViewerWidthUi();
}

function getDigestRangeLabel(range = digestRange) {
    const normalized = normalizeDigestRange(range);

    if (normalized === 'month') {
        return 'Month';
    }

    return normalized === 'week' ? 'Week' : 'Day';
}

function getDigestEmptyStateMessage(range = digestRange) {
    const normalized = normalizeDigestRange(range);

    if (normalized === 'month') {
        return 'No articles have been saved for this month yet.';
    }

    return normalized === 'week'
        ? 'No articles have been saved for this week yet.'
        : 'No articles have been saved for today yet.';
}

function updateDigestRangeUi() {
    const activeRange = normalizeDigestRange(digestRange);

    if (!dom.digest.rangeOptions || dom.digest.rangeOptions.length === 0) {
        return;
    }

    dom.digest.rangeOptions.forEach(option => {
        const isActive = normalizeDigestRange(option.dataset.digestRange) === activeRange;
        option.classList.toggle('is-active', isActive);
        option.setAttribute('aria-pressed', String(isActive));
    });
}

function setDigestRangeToggleDisabled(disabled) {
    if (!dom.digest.rangeOptions || dom.digest.rangeOptions.length === 0) {
        return;
    }

    dom.digest.rangeOptions.forEach(option => {
        option.disabled = Boolean(disabled);
    });

    if (dom.digest.rangeToggle) {
        dom.digest.rangeToggle.setAttribute('aria-disabled', String(Boolean(disabled)));
        dom.digest.rangeToggle.setAttribute('aria-busy', String(Boolean(disabled)));
    }
}

function updateDigestSortUi() {
    if (!dom.digest.sortOptions || dom.digest.sortOptions.length === 0) {
        return;
    }

    dom.digest.sortOptions.forEach(option => {
        const isActive = option.dataset.digestSort === digestSortDirection;
        option.classList.toggle('is-active', isActive);
        option.setAttribute('aria-pressed', String(isActive));
    });
}

function getDigestClusterSortTime(cluster) {
    const representative = cluster?.representative || cluster?.items?.[0];
    const publishedAt = representative?.publishedAt;
    const timestamp = publishedAt ? new Date(publishedAt).getTime() : 0;

    return Number.isFinite(timestamp) ? timestamp : 0;
}

function sortDigestClusters(clusters) {
    return [...clusters].sort((left, right) => {
        const leftTime = getDigestClusterSortTime(left);
        const rightTime = getDigestClusterSortTime(right);
        return digestSortDirection === 'asc' ? leftTime - rightTime : rightTime - leftTime;
    });
}

function getDigestArticleIds(payload) {
    const ids = new Set();

    if (!payload || !Array.isArray(payload.clusters)) {
        return [];
    }

    payload.clusters.forEach(cluster => {
        const items = Array.isArray(cluster?.items) ? cluster.items : [];
        items.forEach(item => {
            const id = Number(item?.id);

            if (Number.isInteger(id) && id > 0) {
                ids.add(id);
            }
        });
    });

    return Array.from(ids);
}

function updateDigestMarkAllButton(payload = digestRuntime.lastPayload) {
    const articleIds = getDigestArticleIds(payload);
    const total = articleIds.length;

    if (!dom.digest.markAllBtn) {
        return;
    }

    dom.digest.markAllBtn.disabled = total === 0;
    writeContent(
        dom.digest.markAllBtn,
        total > 0 ? `Mark visible as digested (${total})` : 'Mark visible as digested',
    );
}

function getNormalizedArticleIds(value) {
    const ids = new Set();

    if (!Array.isArray(value)) {
        return [];
    }

    value.forEach(id => {
        const normalized = Number(id);

        if (Number.isInteger(normalized) && normalized > 0) {
            ids.add(normalized);
        }
    });

    return Array.from(ids);
}

function serializeArticleIdsForDataset(articleIds) {
    return getNormalizedArticleIds(articleIds).join(',');
}

function parseArticleIdsFromDataset(value) {
    const rawIds = String(value || '')
        .split(',')
        .map(entry => entry.trim())
        .filter(Boolean);

    return getNormalizedArticleIds(rawIds);
}

function getDigestClusterArticleIds(cluster) {
    if (!cluster || !Array.isArray(cluster.items)) {
        return [];
    }

    return getNormalizedArticleIds(cluster.items.map(item => item?.id));
}

function getDigestClusterTopics(clusterItems) {
    const topicsMap = new Map();
    const items = Array.isArray(clusterItems) ? clusterItems : [];

    items.forEach(item => {
        const itemTopics = Array.isArray(item?.topics) ? item.topics : [];

        itemTopics.forEach(topic => {
            const slug = String(topic?.slug || '')
                .trim()
                .toLowerCase();
            const label = String(topic?.label || slug || '').trim();
            const key = slug || label.toLowerCase();
            const score = Number(topic?.score || 0);
            const existing = topicsMap.get(key);

            if (!label) {
                return;
            }

            if (!existing || score > existing.score) {
                topicsMap.set(key, {
                    slug,
                    label,
                    score: Number.isFinite(score) ? score : 0,
                });
            }
        });
    });

    return Array.from(topicsMap.values()).sort(
        (left, right) => right.score - left.score || left.label.localeCompare(right.label),
    );
}

function removeClusterFromDigestPayloadByArticleIds(articleIds) {
    const normalizedIds = getNormalizedArticleIds(articleIds);

    if (
        !digestRuntime.lastPayload ||
        !Array.isArray(digestRuntime.lastPayload.clusters) ||
        normalizedIds.length === 0
    ) {
        return false;
    }

    const ids = new Set(normalizedIds);
    const nextClusters = digestRuntime.lastPayload.clusters.filter(cluster => {
        const items = Array.isArray(cluster?.items) ? cluster.items : [];

        return !items.some(item => ids.has(Number(item?.id)));
    });

    if (nextClusters.length === digestRuntime.lastPayload.clusters.length) {
        return false;
    }

    const totalArticles = nextClusters.reduce((sum, cluster) => {
        const items = Array.isArray(cluster?.items) ? cluster.items : [];

        return sum + items.length;
    }, 0);

    digestRuntime.lastPayload = {
        ...digestRuntime.lastPayload,
        clusters: nextClusters,
        totalClusters: nextClusters.length,
        totalArticles,
    };
    digestRuntime.lastRenderFingerprint = getDigestPayloadFingerprint(digestRuntime.lastPayload);
    digestRuntime.needsRefresh = false;

    return true;
}

function applyDigestLocalMutationUi() {
    if (!dom.digest.list || !dom.digest.state) {
        return;
    }

    const clusterElements = dom.digest.list.querySelectorAll('.digest-cluster');

    if (clusterElements.length === 0) {
        writeContent(dom.digest.state, getDigestEmptyStateMessage(digestRuntime.lastPayload?.variant || digestRange));
        show(dom.digest.state);
    } else {
        hide(dom.digest.state);
    }

    renderDigestSubtitle(digestRuntime.lastPayload);
    return updateDigestMarkAllButton(digestRuntime.lastPayload);
}

async function markDigestArticlesByIds(articleIds, triggerBtn, triggerLabel = 'Mark as digested', options = {}) {
    const ids = getNormalizedArticleIds(articleIds);
    const { refresh = true, skipNextDigestEvent = false, toastMessage = '' } = options;
    const previousLabel = triggerBtn ? triggerBtn.textContent : '';

    if (ids.length === 0) {
        return false;
    }

    if (triggerBtn) {
        triggerBtn.disabled = true;
        triggerBtn.textContent = 'Marking…';
    }
    if (skipNextDigestEvent) {
        digestRuntime.pendingMutationEventsToSkip += 1;
    }

    try {
        await apiFetch('/api/articles/digest/mark-all-digested', {
            method: 'POST',
            body: JSON.stringify({ articleIds: ids }),
        });
        const defaultToastMessage = `${ids.length.toLocaleString('en-US')} ${ids.length === 1 ? 'source' : 'sources'} marked as digested`;
        showToast(toastMessage || defaultToastMessage, {
            actionLabel: 'Undo',
            onAction: async () => {
                await apiFetch('/api/articles/digest/restore', {
                    method: 'POST',
                    body: JSON.stringify({ articleIds: ids }),
                });
                digestRuntime.needsRefresh = true;
                await loadDigest({ force: true });
            },
        });
        if (refresh) {
            digestRuntime.needsRefresh = true;
            await loadDigest({ force: true });
        }
        return true;
    } catch (err) {
        if (skipNextDigestEvent && digestRuntime.pendingMutationEventsToSkip > 0) {
            digestRuntime.pendingMutationEventsToSkip -= 1;
        }
        if (triggerBtn) {
            triggerBtn.disabled = false;
            triggerBtn.textContent = previousLabel || triggerLabel;
        }
        alert(`Digested fehlgeschlagen: ${err.message}`);
        return false;
    }
}

async function markAllVisibleAsDigested() {
    const articleIds = getDigestArticleIds(digestRuntime.lastPayload);
    if (dom.digest.bulkMenu) {
        dom.digest.bulkMenu.open = false;
    }
    await markDigestArticlesByIds(articleIds, dom.digest.markAllBtn, 'Mark visible as digested');

    if (!dom.digest.markAllBtn) {
        return;
    }

    updateDigestMarkAllButton(digestRuntime.lastPayload);
}

function setView(name) {
    views.forEach(view => {
        view.classList.toggle('is-active', view.id === `view-${name}`);
    });
    navLinks.forEach(link => {
        link.classList.toggle('is-active', link.dataset.view === name);
    });
    scrollArticlesToTop();
    updateStickySubnavScrollState();

    if (name === 'main' || name === 'digest') {
        renderArticleViewer();
    }

    if (name === 'main' && articlesNeedsRefresh) {
        loadArticles();
    }
    if (name === 'digest') {
        loadDigest();
    }
}

function applyLayoutState() {
    const applyToggleState = (toggleBtn, enabled, { onLabel, offLabel, onValue, offValue }, dataKey) => {
        if (!toggleBtn) {
            return;
        }
        toggleBtn.classList.toggle('is-on', enabled);
        toggleBtn.setAttribute('aria-pressed', String(enabled));
        if (dataKey) {
            toggleBtn.dataset[dataKey] = enabled ? onValue : offValue;
        }
        const label = toggleBtn.querySelector('.toggle-label');
        if (label) {
            label.textContent = enabled ? onLabel : offLabel;
        }
    };

    dom.elements.articlesList.classList.toggle('is-list', isListLayout);
    dom.elements.layoutOptions.forEach(option => {
        const isActive = option.dataset.layout === (isListLayout ? 'list' : 'cards');
        option.classList.toggle('is-active', isActive);
        option.setAttribute('aria-pressed', String(isActive));
    });

    if (dom.elements.themeToggleBtn) {
        applyToggleState(
            dom.elements.themeToggleBtn,
            isDarkTheme,
            {
                onLabel: 'Dark',
                offLabel: 'Light',
                onValue: 'dark',
                offValue: 'light',
            },
            'themeMode',
        );
    }

    updateViewerWidthAvailability();
    applyViewerWidthState();
}

function applyThemeState() {
    document.documentElement.setAttribute('data-theme', isDarkTheme ? 'dark' : 'light');
    return applyLayoutState();
}

function getPageScrollTop() {
    return window.scrollY || document.documentElement.scrollTop || 0;
}

function updateStickySubnavScrollState() {
    const isScrolled = getPageScrollTop() > 2;

    if (settingsTabsWrap) {
        const settingsView = dom.views.settings;
        const isSettingsVisible = settingsView?.classList.contains('is-active');
        settingsTabsWrap.classList.toggle('is-scrolled', Boolean(isSettingsVisible && isScrolled));
    }

    if (dom.digest.header) {
        const digestView = dom.views.digest;
        const isDigestVisible = digestView?.classList.contains('is-active');
        dom.digest.header.classList.toggle('is-scrolled', Boolean(isDigestVisible && isScrolled));
    }

    if (dom.elements.feedBackToTop) {
        const isFeedVisible = dom.views.main?.classList.contains('is-active');
        dom.elements.feedBackToTop.classList.toggle('hide', !(isFeedVisible && getPageScrollTop() > 480));
    }
}

function scheduleStickySubnavScrollUpdate() {
    if (isStickySubnavScrollUpdateScheduled) {
        return;
    }

    isStickySubnavScrollUpdateScheduled = true;
    window.requestAnimationFrame(() => {
        isStickySubnavScrollUpdateScheduled = false;
        updateStickySubnavScrollState();
    });
}

function formatDate(value) {
    const timestamp = Date.parse(value);

    if (!value) return '—';
    if (!Number.isFinite(timestamp)) return '—';

    return dateTimeFormatter.format(timestamp);
}

function formatTriageTime(value, referenceDate = new Date()) {
    const date = new Date(value);
    if (!value || !Number.isFinite(date.getTime())) {
        return '—';
    }

    const time = new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit' }).format(date);
    const startToday = new Date(referenceDate);
    startToday.setHours(0, 0, 0, 0);
    const startArticleDay = new Date(date);
    startArticleDay.setHours(0, 0, 0, 0);
    const dayDifference = Math.round((startToday.getTime() - startArticleDay.getTime()) / 86400000);

    if (dayDifference === 0) {
        return time;
    }
    if (dayDifference === 1) {
        return `Yesterday · ${time}`;
    }

    const day = new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric' }).format(date);
    return `${day} · ${time}`;
}

function showToast(message, { actionLabel = '', onAction = null } = {}) {
    if (!dom.elements.toastRegion) {
        return;
    }
    if (toastTimer) {
        clearTimeout(toastTimer);
        toastTimer = null;
    }

    clearElement(dom.elements.toastRegion);
    const toast = document.createElement('div');
    toast.className = 'toast';
    const text = document.createElement('span');
    text.textContent = message;
    toast.appendChild(text);

    if (actionLabel && typeof onAction === 'function') {
        const action = document.createElement('button');
        action.type = 'button';
        action.textContent = actionLabel;
        action.addEventListener('click', async () => {
            action.disabled = true;
            try {
                await onAction();
                toast.remove();
            } catch (err) {
                action.disabled = false;
                showToast(`Undo failed: ${err.message}`);
            }
        });
        toast.appendChild(action);
    }

    dom.elements.toastRegion.appendChild(toast);
    toastTimer = setTimeout(() => {
        toast.remove();
        toastTimer = null;
    }, 6000);
}

async function loadFetchStatus() {
    try {
        const status = await apiFetch('/api/fetch/status');
        if (!status || !status.at) {
            writeContent(dom.elements.fetchStatus, 'Last fetch: —');
        } else {
            const date = formatDate(status.at);
            const suffix = status.error ? ` (Error: ${status.error})` : ` (${status.totalNew} new)`;
            writeContent(dom.elements.fetchStatus, `Last fetch: ${date}${suffix}`);
        }
    } catch {
        writeContent(dom.elements.fetchStatus, 'Last fetch: —');
    }

    if (!dom.elements.articleCountStatus) {
        return;
    }

    try {
        const stats = await apiFetch('/api/articles/stats');
        const total = Number(stats?.total || 0);
        writeContent(dom.elements.articleCountStatus, `Saved articles: ${total.toLocaleString('de-DE')}`);
    } catch {
        writeContent(dom.elements.articleCountStatus, 'Saved articles: —');
    }
}

async function runManualFetch(triggerBtn) {
    const previousLabel = triggerBtn.textContent;

    if (!triggerBtn) {
        return;
    }

    triggerBtn.disabled = true;
    triggerBtn.textContent = 'fetching…';

    try {
        await apiFetch('/api/fetch/run', { method: 'POST' });
        articlesNeedsRefresh = true;
        if (isViewActive('main')) {
            await loadArticles();
        }
        requestDigestRefresh({ force: true });
        await loadFetchStatus();
    } catch (err) {
        alert(`Fetch fehlgeschlagen: ${err.message}`);
    } finally {
        triggerBtn.disabled = false;
        triggerBtn.textContent = previousLabel;
    }
}

function show(element) {
    if (!element) {
        return;
    }

    element.classList.remove('hide');
}

function hide(element) {
    if (!element) {
        return;
    }

    element.classList.add('hide');
}

function writeContent(element, message) {
    if (!element) {
        return;
    }

    element.textContent = message == null ? '' : String(message);
}

function setStatus(element, message) {
    writeContent(element, message || '');
}

function normalizeDigestSettingFeedIds(value) {
    const ids = new Set();

    if (!Array.isArray(value)) {
        return [];
    }

    value.forEach(feedId => {
        const normalized = Number(feedId);

        if (Number.isInteger(normalized) && normalized > 0) {
            ids.add(normalized);
        }
    });

    return Array.from(ids);
}

function getSelectedDigestExcludedFeedIds() {
    const selected = [];
    const checkboxes = dom.digest.settings.feedsList.querySelectorAll('input[type="checkbox"][data-feed-id]');

    if (!dom.digest.settings.feedsList) {
        return [];
    }

    checkboxes.forEach(input => {
        if (!input.checked) {
            return;
        }
        const feedId = Number(input.dataset.feedId);
        if (Number.isInteger(feedId) && feedId > 0) {
            selected.push(feedId);
        }
    });

    return normalizeDigestSettingFeedIds(selected);
}

function clearElement(element) {
    if (element) {
        element.replaceChildren();
    }
}

function appendSelectOption(select, value, label) {
    const option = document.createElement('option');
    option.value = value;
    option.textContent = label;
    select.appendChild(option);
    return option;
}

function renderDigestSettings() {
    const excludedSet = new Set(
        normalizeDigestSettingFeedIds(state.digestSettings?.excludedFeedIds || []).map(feedId => String(feedId)),
    );

    if (!dom.digest.settings.feedsList || !dom.digest.settings.blockWordsList) {
        return;
    }

    clearElement(dom.digest.settings.feedsList);

    if (!Array.isArray(state.feeds) || state.feeds.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'state';
        empty.textContent = 'No feeds yet.';
        dom.digest.settings.feedsList.appendChild(empty);
    } else {
        state.feeds.forEach(feed => {
            const feedId = Number(feed.id);
            const row = document.createElement('label');
            row.className = 'settings-digest-feed-item';

            const checkbox = document.createElement('input');
            checkbox.type = 'checkbox';
            checkbox.dataset.feedId = String(feedId);
            checkbox.checked = excludedSet.has(String(feedId));
            row.appendChild(checkbox);

            const textWrap = document.createElement('span');
            textWrap.className = 'settings-digest-feed-item-text';

            const title = document.createElement('span');
            title.className = 'settings-digest-feed-item-title';
            title.textContent = feed.name || 'Unnamed feed';

            const meta = document.createElement('span');
            meta.className = 'settings-digest-feed-item-meta';
            meta.textContent = feed.feedUrl || feed.websiteUrl || '';

            textWrap.appendChild(title);
            textWrap.appendChild(meta);
            row.appendChild(textWrap);

            dom.digest.settings.feedsList.appendChild(row);
        });
    }

    clearElement(dom.digest.settings.blockWordsList);

    const blockedWords = Array.isArray(state.digestSettings?.blockedWords) ? state.digestSettings.blockedWords : [];

    if (blockedWords.length === 0) {
        const empty = document.createElement('div');
        empty.className = 'state';
        empty.textContent = 'No blocked words yet.';
        dom.digest.settings.blockWordsList.appendChild(empty);
        return;
    }

    blockedWords.forEach(item => {
        const row = document.createElement('div');
        row.className = 'settings-digest-word-item';

        const wordLabel = document.createElement('span');
        wordLabel.className = 'settings-digest-word-label';
        wordLabel.textContent = item.word;
        row.appendChild(wordLabel);

        const removeBtn = document.createElement('button');
        removeBtn.type = 'button';
        removeBtn.className = 'btn danger';
        removeBtn.textContent = 'remove';
        removeBtn.addEventListener('click', async () => {
            try {
                await apiFetch(`/api/digest-settings/blocked-words/${item.id}`, { method: 'DELETE' });
                setStatus(dom.digest.settings.blockWordsState, '');
                await loadDigestSettings({ silent: true });
                requestDigestRefresh({ force: true });
            } catch (err) {
                setStatus(dom.digest.settings.blockWordsState, `Error: ${err.message}`);
            }
        });
        row.appendChild(removeBtn);

        dom.digest.settings.blockWordsList.appendChild(row);
    });
}

async function loadDigestSettings({ silent = false } = {}) {
    if (!dom.digest.settings.feedsState || !dom.digest.settings.blockWordsState) {
        return;
    }

    if (!silent) {
        setStatus(dom.digest.settings.feedsState, 'Loading…');
        setStatus(dom.digest.settings.blockWordsState, 'Loading…');
    }

    try {
        const payload = await apiFetch('/api/digest-settings');
        state.digestSettings = {
            excludedFeedIds: normalizeDigestSettingFeedIds(payload?.excludedFeedIds || []),
            blockedWords: Array.isArray(payload?.blockedWords) ? payload.blockedWords : [],
        };
        setStatus(dom.digest.settings.feedsState, '');
        setStatus(dom.digest.settings.blockWordsState, '');
        renderDigestSettings();
    } catch (err) {
        setStatus(dom.digest.settings.feedsState, `Error: ${err.message}`);
        setStatus(dom.digest.settings.blockWordsState, `Error: ${err.message}`);
    }
}

async function saveDigestExcludedFeeds() {
    if (!dom.digest.settings.saveFeeds) {
        return;
    }

    const selectedFeedIds = getSelectedDigestExcludedFeedIds();
    const previousLabel = dom.digest.settings.saveFeeds.textContent;

    dom.digest.settings.saveFeeds.disabled = true;
    dom.digest.settings.saveFeeds.textContent = 'saving…';
    setStatus(dom.digest.settings.feedsState, 'Saving…');

    try {
        const payload = await apiFetch('/api/digest-settings/excluded-feeds', {
            method: 'PUT',
            body: JSON.stringify({ feedIds: selectedFeedIds }),
        });

        state.digestSettings = {
            excludedFeedIds: normalizeDigestSettingFeedIds(payload?.excludedFeedIds || []),
            blockedWords: Array.isArray(payload?.blockedWords) ? payload.blockedWords : [],
        };
        setStatus(dom.digest.settings.feedsState, `Saved (${state.digestSettings.excludedFeedIds.length} excluded).`);
        setStatus(dom.digest.settings.blockWordsState, '');
        renderDigestSettings();
        requestDigestRefresh({ force: true });
    } catch (err) {
        setStatus(dom.digest.settings.feedsState, `Error: ${err.message}`);
    } finally {
        dom.digest.settings.saveFeeds.disabled = false;
        dom.digest.settings.saveFeeds.textContent = previousLabel;
    }
}

async function addDigestBlockedWord() {
    if (!dom.digest.settings.blockWordInput || !dom.digest.settings.blockWordAdd) {
        return;
    }

    const word = String(dom.digest.settings.blockWordInput.value || '')
        .trim()
        .replace(/\s+/g, ' ')
        .slice(0, CONFIG.MAX_WORD_LENGTH);

    if (word.length < 2) {
        setStatus(dom.digest.settings.blockWordsState, 'Please enter at least 2 characters.');
        return;
    }

    const previousLabel = dom.digest.settings.blockWordAdd.textContent;
    dom.digest.settings.blockWordAdd.disabled = true;
    dom.digest.settings.blockWordAdd.textContent = 'adding…';
    setStatus(dom.digest.settings.blockWordsState, 'Saving…');

    try {
        await apiFetch('/api/digest-settings/blocked-words', {
            method: 'POST',
            body: JSON.stringify({ word }),
        });
        dom.digest.settings.blockWordInput.value = '';
        setStatus(dom.digest.settings.blockWordsState, '');
        await loadDigestSettings({ silent: true });
        requestDigestRefresh({ force: true });
    } catch (err) {
        setStatus(dom.digest.settings.blockWordsState, `Error: ${err.message}`);
    } finally {
        dom.digest.settings.blockWordAdd.disabled = false;
        dom.digest.settings.blockWordAdd.textContent = previousLabel;
    }
}

function parseTopicKeywordsInput(value) {
    if (Array.isArray(value)) {
        return value.map(entry => String(entry || '').trim()).filter(Boolean);
    }

    return String(value || '')
        .split(/[\n,]/)
        .map(entry => entry.trim())
        .filter(Boolean);
}

function formatTopicKeywordsInput(value) {
    if (!Array.isArray(value) || value.length === 0) {
        return '';
    }

    return value.join('\n');
}

function getTopicFormPayload() {
    return {
        slug: String(dom.elements.topicSlug?.value || '').trim(),
        label: String(dom.elements.topicLabel?.value || '').trim(),
        strong: parseTopicKeywordsInput(dom.elements.topicStrong?.value || ''),
        medium: parseTopicKeywordsInput(dom.elements.topicMedium?.value || ''),
        weak: parseTopicKeywordsInput(dom.elements.topicWeak?.value || ''),
    };
}

function resetTopicForm() {
    topicEditingSlug = null;
    if (dom.elements.topicSlug) dom.elements.topicSlug.value = '';
    if (dom.elements.topicLabel) dom.elements.topicLabel.value = '';
    if (dom.elements.topicStrong) dom.elements.topicStrong.value = '';
    if (dom.elements.topicMedium) dom.elements.topicMedium.value = '';
    if (dom.elements.topicWeak) dom.elements.topicWeak.value = '';
    if (dom.elements.topicSubmit) dom.elements.topicSubmit.textContent = 'save topic';
    setStatus(dom.elements.topicFormStatus, '');
}

function startTopicEdit(topic) {
    topicEditingSlug = topic.slug;
    if (dom.elements.topicSlug) dom.elements.topicSlug.value = topic.slug || '';
    if (dom.elements.topicLabel) dom.elements.topicLabel.value = topic.label || '';
    if (dom.elements.topicStrong) dom.elements.topicStrong.value = formatTopicKeywordsInput(topic.strong);
    if (dom.elements.topicMedium) dom.elements.topicMedium.value = formatTopicKeywordsInput(topic.medium);
    if (dom.elements.topicWeak) dom.elements.topicWeak.value = formatTopicKeywordsInput(topic.weak);
    if (dom.elements.topicSubmit) dom.elements.topicSubmit.textContent = 'save changes';
    setStatus(dom.elements.topicFormStatus, `Editing topic: ${topic.slug}`);
}

function renderTopics() {
    if (!dom.elements.topicsList || !dom.elements.topicsState) {
        return;
    }

    clearElement(dom.elements.topicsList);

    if (!Array.isArray(state.topics) || state.topics.length === 0) {
        writeContent(dom.elements.topicsState, 'No topics configured yet.');
        show(dom.elements.topicsState);
        return;
    }

    hide(dom.elements.topicsState);

    state.topics.forEach(topic => {
        const item = document.createElement('div');
        item.className = 'list-item settings-topic-item';

        const content = document.createElement('div');
        content.className = 'settings-topic-item-main';

        const title = document.createElement('div');
        title.className = 'settings-topic-item-title';

        const label = document.createElement('span');
        label.textContent = topic.label || topic.slug;

        const slug = document.createElement('span');
        slug.className = 'settings-topic-item-slug';
        slug.textContent = topic.slug;

        title.appendChild(label);
        title.appendChild(slug);

        const meta = document.createElement('div');
        meta.className = 'settings-topic-item-meta';
        meta.textContent = `strong: ${(topic.strong || []).length} · medium: ${(topic.medium || []).length} · weak: ${(topic.weak || []).length}`;

        content.appendChild(title);
        content.appendChild(meta);

        const actions = document.createElement('div');
        actions.className = 'list-actions';

        const editBtn = document.createElement('button');
        editBtn.type = 'button';
        editBtn.className = 'btn ghost';
        editBtn.textContent = 'edit';
        editBtn.addEventListener('click', () => startTopicEdit(topic));

        const deleteBtn = document.createElement('button');
        deleteBtn.type = 'button';
        deleteBtn.className = 'btn danger';
        deleteBtn.textContent = 'remove';
        deleteBtn.addEventListener('click', async () => {
            if (!confirm(['Delete topic "', topic.label || topic.slug, '"?'].join(''))) {
                return;
            }
            try {
                await apiFetch(`/api/topics/${encodeURIComponent(topic.slug)}`, { method: 'DELETE' });
                if (topicEditingSlug === topic.slug) {
                    resetTopicForm();
                }
                await loadTopics();
                await loadTopicRulesJson();
            } catch (err) {
                setStatus(dom.elements.topicsState, `Error: ${err.message}`);
                show(dom.elements.topicsState);
            }
        });

        actions.appendChild(editBtn);
        actions.appendChild(deleteBtn);

        item.appendChild(content);
        item.appendChild(actions);
        dom.elements.topicsList.appendChild(item);
    });
}

async function loadTopics() {
    if (!dom.elements.topicsState) {
        return;
    }
    show(dom.elements.topicsState);
    writeContent(dom.elements.topicsState, 'Loading…');
    try {
        const payload = await apiFetch('/api/topics');
        state.topics = Array.isArray(payload?.topics) ? payload.topics : [];
        renderTopics();
        renderTopicFilterOptions();
    } catch (err) {
        writeContent(dom.elements.topicsState, `Error: ${err.message}`);
    }
}

async function loadTopicRulesJson() {
    if (!dom.elements.topicsJsonInput) {
        return;
    }
    try {
        const payload = await apiFetch('/api/topics/rules');
        dom.elements.topicsJsonInput.value = payload?.raw || JSON.stringify(payload?.rules || {}, null, 2);
        setStatus(dom.elements.topicsJsonStatus, '');
    } catch (err) {
        setStatus(dom.elements.topicsJsonStatus, `Error: ${err.message}`);
    }
}

async function validateTopicRulesJson() {
    if (!dom.elements.topicsJsonInput || !dom.elements.topicsJsonValidateBtn) {
        return;
    }
    const previousLabel = dom.elements.topicsJsonValidateBtn.textContent;
    dom.elements.topicsJsonValidateBtn.disabled = true;
    dom.elements.topicsJsonValidateBtn.textContent = 'validating…';
    setStatus(dom.elements.topicsJsonStatus, 'Validating…');

    try {
        const payload = await apiFetch('/api/topics/validate', {
            method: 'POST',
            body: JSON.stringify({ json: dom.elements.topicsJsonInput.value }),
        });
        setStatus(
            dom.elements.topicsJsonStatus,
            `Valid (${payload.topicCount} topic${payload.topicCount === 1 ? '' : 's'})`,
        );
    } catch (err) {
        setStatus(dom.elements.topicsJsonStatus, `Invalid JSON: ${err.message}`);
    } finally {
        dom.elements.topicsJsonValidateBtn.disabled = false;
        dom.elements.topicsJsonValidateBtn.textContent = previousLabel;
    }
}

async function saveTopicRulesJson() {
    if (!dom.elements.topicsJsonInput || !dom.elements.topicsJsonSaveBtn) {
        return;
    }
    const previousLabel = dom.elements.topicsJsonSaveBtn.textContent;
    dom.elements.topicsJsonSaveBtn.disabled = true;
    dom.elements.topicsJsonSaveBtn.textContent = 'saving…';
    setStatus(dom.elements.topicsJsonStatus, 'Saving…');

    try {
        const payload = await apiFetch('/api/topics/rules', {
            method: 'PUT',
            body: JSON.stringify({ json: dom.elements.topicsJsonInput.value }),
        });
        dom.elements.topicsJsonInput.value = payload?.raw || dom.elements.topicsJsonInput.value;
        setStatus(dom.elements.topicsJsonStatus, `Saved (${(payload?.topics || []).length} topics)`);
        await loadTopics();
        resetTopicForm();
    } catch (err) {
        setStatus(dom.elements.topicsJsonStatus, `Error: ${err.message}`);
    } finally {
        dom.elements.topicsJsonSaveBtn.disabled = false;
        dom.elements.topicsJsonSaveBtn.textContent = previousLabel;
    }
}

async function submitTopicForm(event) {
    event.preventDefault();
    if (!dom.elements.topicForm || !dom.elements.topicSubmit) {
        return;
    }

    const payload = getTopicFormPayload();
    if (!payload.slug || !payload.label) {
        setStatus(dom.elements.topicFormStatus, 'Slug and label are required.');
        return;
    }

    dom.elements.topicSubmit.disabled = true;
    setStatus(dom.elements.topicFormStatus, 'Saving…');

    try {
        if (topicEditingSlug) {
            await apiFetch(`/api/topics/${encodeURIComponent(topicEditingSlug)}`, {
                method: 'PUT',
                body: JSON.stringify(payload),
            });
        } else {
            await apiFetch('/api/topics', {
                method: 'POST',
                body: JSON.stringify(payload),
            });
        }

        resetTopicForm();
        await loadTopics();
        await loadTopicRulesJson();
    } catch (err) {
        setStatus(dom.elements.topicFormStatus, `Error: ${err.message}`);
    } finally {
        dom.elements.topicSubmit.disabled = false;
    }
}

async function reprocessTopicsForAllArticles() {
    if (!dom.elements.topicsReprocessBtn) {
        return;
    }

    const previousLabel = dom.elements.topicsReprocessBtn.textContent;
    dom.elements.topicsReprocessBtn.disabled = true;
    dom.elements.topicsReprocessBtn.textContent = 'running…';
    setStatus(dom.elements.topicsReprocessStatus, 'Reprocessing…');

    try {
        const result = await apiFetch('/api/topics/reprocess', { method: 'POST' });
        setStatus(
            dom.elements.topicsReprocessStatus,
            [
                'Done: ',
                result.processed,
                ' processed · ',
                result.assignedArticles,
                ' with topics · ',
                result.topicAssignments,
                ' assignments',
            ].join(''),
        );
    } catch (err) {
        setStatus(dom.elements.topicsReprocessStatus, `Error: ${err.message}`);
    } finally {
        dom.elements.topicsReprocessBtn.disabled = false;
        dom.elements.topicsReprocessBtn.textContent = previousLabel;
    }
}

async function apiFetch(url, options = {}) {
    const headers = new Headers(options.headers || {});

    if (options.body && !headers.has('Content-Type')) {
        headers.set('Content-Type', 'application/json');
    }

    const res = await fetch(url, {
        ...options,
        headers,
    });

    if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        const error = body.error || `HTTP ${res.status}`;

        throw new Error(error);
    }

    if (res.status === 204) return null;

    return res.json();
}

function isAbortError(err) {
    return Boolean(
        err &&
        (err.name === 'AbortError' ||
            err.code === 'ABORT_ERR' ||
            String(err.message || '')
                .toLowerCase()
                .includes('aborted')),
    );
}

function renderFeeds() {
    clearElement(dom.elements.feedsList);

    if (state.feeds.length === 0) {
        writeContent(dom.elements.feedsState, 'No feeds yet.');
        show(dom.elements.feedsState);
        return;
    }

    hide(dom.elements.feedsState);

    const template = dom.templates.feedItem;

    state.feeds.forEach(feed => {
        const node = template.content.cloneNode(true);
        const logoEl = node.querySelector('.feed-logo');
        const nameEl = node.querySelector('.feed-name');

        if (feed.logoDataUrl) {
            logoEl.src = feed.logoDataUrl;
            show(logoEl);
        } else {
            hide(logoEl);
        }
        nameEl.textContent = feed.name;
        node.querySelector('.list-meta').textContent = `${feed.websiteUrl} · ${feed.feedUrl}`;

        node.querySelector('.btn-edit').addEventListener('click', () => {
            state.editingId = feed.id;
            dom.elements.feedName.value = feed.name;
            dom.elements.feedWebsite.value = feed.websiteUrl;
            dom.elements.feedUrl.value = feed.feedUrl;
            dom.elements.feedSubmit.textContent = 'Save changes';
            setStatus(dom.elements.feedFormStatus, 'Edit mode active.');
        });

        node.querySelector('.btn-delete').addEventListener('click', async () => {
            if (!confirm(['Delete feed "', feed.name, '"?'].join(''))) return;
            try {
                await apiFetch(`/api/feeds/${feed.id}`, { method: 'DELETE' });
                await loadFeeds();
                await loadArticles();
            } catch (err) {
                alert(err.message);
            }
        });

        dom.elements.feedsList.appendChild(node);
    });
}

function renderLists() {
    clearElement(dom.elements.listsList);

    if (state.lists.length === 0) {
        writeContent(dom.elements.listsState, 'No lists yet.');
        show(dom.elements.listsState);
        return;
    }

    hide(dom.elements.listsState);
    const template = dom.templates.listItem;

    state.lists.forEach(list => {
        const node = template.content.cloneNode(true);
        const nameEl = node.querySelector('.list-name');
        const dotEl = node.querySelector('.list-color-dot');

        if (nameEl) {
            nameEl.textContent = list.name;
        }
        if (dotEl) {
            dotEl.style.background = list.color || '#1d1d1f';
        }
        node.querySelector('.list-meta').textContent = list.description || '';

        node.querySelector('.btn-edit').addEventListener('click', () => {
            listEditingId = list.id;
            dom.elements.listName.value = list.name;
            dom.elements.listDescription.value = list.description || '';
            dom.elements.listColor.value = list.color || '#1d1d1f';
            dom.elements.listSubmit.textContent = 'Save changes';
            setStatus(dom.elements.listFormStatus, 'Edit mode active.');
        });

        node.querySelector('.btn-delete').addEventListener('click', async () => {
            if (!confirm(['Delete list "', list.name, '"?'].join(''))) {
                return;
            }
            try {
                await apiFetch(`/api/lists/${list.id}`, { method: 'DELETE' });
                await loadLists();
            } catch (err) {
                alert(err.message);
            }
        });

        dom.elements.listsList.appendChild(node);
    });
}

async function openListModal(articleIdsOrId) {
    const articleIds = Array.isArray(articleIdsOrId)
        ? getNormalizedArticleIds(articleIdsOrId)
        : getNormalizedArticleIds([articleIdsOrId]);
    const isMultiArticleSelection = articleIds.length > 1;
    const modalArticleIds = [...articleIds];
    const wasOpen = isListModalOpen();

    if (articleIds.length === 0) {
        return;
    }
    if (!wasOpen && document.activeElement instanceof HTMLElement) {
        modalRuntime.lastFocusedElement = document.activeElement;
    }

    pendingArticleIds = articleIds;
    clearElement(dom.modal.modalListSelect);
    appendSelectOption(dom.modal.modalListSelect, '', 'Choose list');

    let existingIds = new Set();
    let existingLists = [];
    try {
        const payload = await apiFetch('/api/articles/lists/bulk', {
            method: 'POST',
            body: JSON.stringify({ articleIds }),
        });
        const payloadCommonLists = Array.isArray(payload?.commonLists) ? payload.commonLists : [];
        const payloadListsByArticleId =
            payload && typeof payload.listsByArticleId === 'object' && payload.listsByArticleId
                ? payload.listsByArticleId
                : {};
        const singleArticleLists = Array.isArray(payloadListsByArticleId[String(articleIds[0])])
            ? payloadListsByArticleId[String(articleIds[0])]
            : [];

        existingLists = articleIds.length === 1 ? singleArticleLists : payloadCommonLists;
        existingIds = new Set(existingLists.map(item => String(item.id)));
    } catch (err) {
        existingIds = new Set();
        existingLists = [];
    }

    state.lists.forEach(list => {
        const option = document.createElement('option');
        option.value = list.id;
        option.textContent = existingIds.has(String(list.id))
            ? `${list.name} (${isMultiArticleSelection ? 'already in all' : 'already'})`
            : list.name;
        option.disabled = existingIds.has(String(list.id));
        dom.modal.modalListSelect.appendChild(option);
    });

    if (dom.modal.modalExistingLists) {
        clearElement(dom.modal.modalExistingLists);
        if (existingLists.length === 0) {
            dom.modal.modalExistingLists.textContent = '—';
        } else {
            existingLists.forEach(item => {
                const chip = document.createElement('span');
                chip.className = 'modal-chip';
                const dot = document.createElement('span');
                dot.className = 'modal-chip-dot';
                dot.style.background = item.color || '#1d1d1f';
                const text = document.createElement('span');
                text.textContent = item.name;
                chip.appendChild(dot);
                chip.appendChild(text);

                const removeBtn = document.createElement('button');
                removeBtn.type = 'button';
                removeBtn.className = 'modal-chip-remove';
                removeBtn.textContent = '×';
                removeBtn.title = `Remove ${item.name}`;
                removeBtn.setAttribute(
                    'aria-label',
                    `Remove ${item.name} from selected article${isMultiArticleSelection ? 's' : ''}`,
                );
                removeBtn.addEventListener('click', async event => {
                    event.preventDefault();
                    event.stopPropagation();
                    removeBtn.disabled = true;
                    try {
                        await apiFetch(`/api/lists/${item.id}/items/bulk-delete`, {
                            method: 'POST',
                            body: JSON.stringify({ articleIds: modalArticleIds }),
                        });
                        await openListModal(modalArticleIds);
                    } catch (err) {
                        removeBtn.disabled = false;
                        alert(`Remove from list failed: ${err.message}`);
                    }
                });
                chip.appendChild(removeBtn);
                dom.modal.modalExistingLists.appendChild(chip);
            });
        }
    }

    dom.modal.modalBackdrop.classList.add('is-open');
    dom.modal.modalBackdrop.setAttribute('aria-hidden', 'false');
    focusListModalInitialTarget();
}

function closeListModal() {
    if (!isListModalOpen()) {
        pendingArticleIds = [];
        modalRuntime.lastFocusedElement = null;
        return;
    }

    pendingArticleIds = [];
    dom.modal.modalBackdrop.classList.remove('is-open');
    dom.modal.modalBackdrop.setAttribute('aria-hidden', 'true');
    restoreFocusAfterListModalClose();
}

function resetListForm() {
    listEditingId = null;
    dom.elements.listName.value = '';
    dom.elements.listDescription.value = '';
    dom.elements.listColor.value = '#1d1d1f';
    dom.elements.listSubmit.textContent = 'Save list';
    setStatus(dom.elements.listFormStatus, '');
}

async function loadLists() {
    show(dom.elements.listsState);
    writeContent(dom.elements.listsState, 'Loading…');
    try {
        state.lists = await apiFetch('/api/lists');
        renderLists();
        renderListFilterOptions();
    } catch (err) {
        writeContent(dom.elements.listsState, `Error: ${err.message}`);
    }
}

function renderFilterOptions() {
    const selected = dom.elements.filterSource.value;
    clearElement(dom.elements.filterSource);
    appendSelectOption(dom.elements.filterSource, '', 'Source: All');
    state.feeds.forEach(feed => {
        const option = document.createElement('option');
        option.value = feed.id;
        option.textContent = `Source: ${feed.name}`;
        dom.elements.filterSource.appendChild(option);
    });
    dom.elements.filterSource.value = selected;
}

function renderListFilterOptions() {
    const selected = dom.elements.filterList.value;
    clearElement(dom.elements.filterList);
    appendSelectOption(dom.elements.filterList, '', 'List: All');
    state.lists.forEach(list => {
        const option = document.createElement('option');
        option.value = list.id;
        option.textContent = `List: ${list.name}`;
        dom.elements.filterList.appendChild(option);
    });
    dom.elements.filterList.value = selected;
}

function renderTopicFilterOptions() {
    if (!dom.elements.filterTopic) {
        return;
    }
    const selected = dom.elements.filterTopic.value;
    clearElement(dom.elements.filterTopic);
    appendSelectOption(dom.elements.filterTopic, '', 'Topic: All');
    state.topics.forEach(topic => {
        const option = document.createElement('option');
        option.value = topic.slug;
        option.textContent = `Topic: ${topic.label || topic.slug}`;
        dom.elements.filterTopic.appendChild(option);
    });
    dom.elements.filterTopic.value = selected;
}

function findFeedIdBySourceName(sourceName) {
    const normalizedSourceName = String(sourceName || '')
        .trim()
        .toLowerCase();

    if (!normalizedSourceName) {
        return '';
    }

    const matchedFeed = state.feeds.find(feed => {
        const normalizedFeedName = String(feed?.name || '')
            .trim()
            .toLowerCase();

        return normalizedFeedName === normalizedSourceName;
    });

    return matchedFeed ? String(matchedFeed.id) : '';
}

function isListModalOpen() {
    return Boolean(dom.modal.modalBackdrop?.classList.contains('is-open'));
}

function getListModalFocusableElements() {
    const selectors = [
        'button:not([disabled])',
        'a[href]',
        'input:not([disabled])',
        'select:not([disabled])',
        'textarea:not([disabled])',
        '[tabindex]:not([tabindex="-1"])',
    ];
    const modalNode = dom.modal.modalBackdrop?.querySelector('.modal');
    const focusables = modalNode ? Array.from(modalNode.querySelectorAll(selectors.join(','))) : [];

    return focusables.filter(node => node instanceof HTMLElement);
}

function focusListModalInitialTarget() {
    const focusables = getListModalFocusableElements();
    const preferred =
        dom.modal.modalListSelect && !dom.modal.modalListSelect.disabled ? dom.modal.modalListSelect : focusables[0];

    if (!(preferred instanceof HTMLElement)) {
        return;
    }

    preferred.focus({ preventScroll: true });
}

function restoreFocusAfterListModalClose() {
    const previous = modalRuntime.lastFocusedElement;
    modalRuntime.lastFocusedElement = null;

    if (!(previous instanceof HTMLElement)) {
        return;
    }
    if (!document.contains(previous)) {
        return;
    }

    previous.focus({ preventScroll: true });
}

function handleListModalKeydown(event) {
    if (!isListModalOpen()) {
        return;
    }

    if (event.key === 'Escape') {
        event.preventDefault();
        closeListModal();
        return;
    }
    if (event.key !== 'Tab') {
        return;
    }

    const focusables = getListModalFocusableElements();

    if (focusables.length === 0) {
        event.preventDefault();
        if (dom.modal.modalBackdrop instanceof HTMLElement) {
            dom.modal.modalBackdrop.focus({ preventScroll: true });
        }
        return;
    }

    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    const active = document.activeElement;

    if (event.shiftKey && active === first) {
        event.preventDefault();
        last.focus({ preventScroll: true });
        return;
    }
    if (!event.shiftKey && active === last) {
        event.preventDefault();
        first.focus({ preventScroll: true });
    }
}

function getArticlesPayloadFingerprint(articles) {
    if (!Array.isArray(articles) || articles.length === 0) {
        return 'empty';
    }

    return articles
        .map(article => {
            const articleId = Number(article?.id || 0);
            const feedId = Number(article?.feedId || 0);
            const publishedAt = String(article?.publishedAt || '');
            const sourceName = String(article?.sourceName || '');
            const title = String(article?.title || '');
            const teaser = String(article?.teaser || '');
            const url = String(article?.url || '');
            const saved = Boolean(article?.saved);
            const topics = Array.isArray(article?.topics) ? article.topics : [];
            const topicsFingerprint = topics
                .map(topic => {
                    const slug = String(topic?.slug || '');
                    const label = String(topic?.label || '');
                    const score = Number(topic?.score || 0).toFixed(3);

                    return `${slug}:${label}:${score}`;
                })
                .join(',');

            return `${articleId}|${feedId}|${publishedAt}|${sourceName}|${title}|${teaser}|${url}|${saved}|${topicsFingerprint}`;
        })
        .join('||');
}

function normalizeArticleUrl(value) {
    const rawValue = String(value || '').trim();
    let parsedUrl = null;

    if (!rawValue) {
        return null;
    }

    try {
        parsedUrl = new URL(rawValue);
    } catch {
        return null;
    }

    if (parsedUrl.protocol !== 'http:' && parsedUrl.protocol !== 'https:') {
        return null;
    }

    return parsedUrl.toString();
}

function getArticleById(articleId) {
    const normalizedArticleId = Number(articleId);
    let article = null;

    if (!Number.isInteger(normalizedArticleId) || normalizedArticleId <= 0) {
        return null;
    }

    article = articlesRuntime.articleById.get(normalizedArticleId) || null;

    if (article) {
        return article;
    }

    return digestRuntime.articleById.get(normalizedArticleId) || null;
}

function setActiveArticleCardState() {
    const cards = dom.elements.articlesList?.querySelectorAll('.card[data-article-id]') || [];
    const normalizedActiveArticleId = Number(activeArticleId);
    const hasActiveArticleId = Number.isInteger(normalizedActiveArticleId) && normalizedActiveArticleId > 0;

    cards.forEach(card => {
        const cardArticleId = Number(card.dataset.articleId);
        const isActive = viewerOpen && hasActiveArticleId && cardArticleId === normalizedActiveArticleId;

        card.classList.toggle('is-active', isActive);
    });
}

function setActiveDigestItemCardState() {
    const cards = dom.digest.list?.querySelectorAll('.digest-item-card[data-article-id]') || [];
    const normalizedActiveArticleId = Number(activeArticleId);
    const hasActiveArticleId = Number.isInteger(normalizedActiveArticleId) && normalizedActiveArticleId > 0;
    const isDigestView = isViewActive('digest');

    cards.forEach(card => {
        const cardArticleId = Number(card.dataset.articleId);
        const isActive =
            viewerOpen && isDigestView && hasActiveArticleId && cardArticleId === normalizedActiveArticleId;

        card.classList.toggle('is-active', isActive);
    });
}

function getViewerContexts() {
    const contexts = [];

    if (
        dom.elements.dashboardLayout &&
        dom.elements.articleViewer &&
        dom.elements.articleViewerTitle &&
        dom.elements.articleViewerMessage &&
        dom.elements.articleViewerFrame
    ) {
        contexts.push({
            viewName: 'main',
            layoutElement: dom.elements.dashboardLayout,
            panel: dom.elements.articleViewer,
            title: dom.elements.articleViewerTitle,
            message: dom.elements.articleViewerMessage,
            frame: dom.elements.articleViewerFrame,
        });
    }

    if (
        dom.digest.layout &&
        dom.digest.viewer &&
        dom.digest.viewerTitle &&
        dom.digest.viewerMessage &&
        dom.digest.viewerFrame
    ) {
        contexts.push({
            viewName: 'digest',
            layoutElement: dom.digest.layout,
            panel: dom.digest.viewer,
            title: dom.digest.viewerTitle,
            message: dom.digest.viewerMessage,
            frame: dom.digest.viewerFrame,
        });
    }

    return contexts;
}

function renderArticleViewer() {
    const contexts = getViewerContexts();
    const activeViewName = isViewActive('digest') ? 'digest' : isViewActive('main') ? 'main' : '';
    const hasValidViewerUrl = Boolean(viewerUrl);
    const hasViewerMessage = Boolean(String(viewerMessage || '').trim());

    if (contexts.length === 0) {
        return;
    }

    applyViewerWidthState();
    contexts.forEach(context => {
        const isActiveContext = context.viewName === activeViewName;
        const isVisible = viewerOpen && isActiveContext;

        context.layoutElement.classList.toggle('is-viewer-open', isVisible);
        writeContent(context.title, viewerTitle || 'Article viewer');

        if (isVisible) {
            show(context.panel);
        } else {
            hide(context.panel);
            hide(context.message);
            hide(context.frame);
            writeContent(context.message, '');
            context.frame.removeAttribute('src');
            context.frame.removeAttribute('data-loaded-url');
            return;
        }

        if (hasValidViewerUrl) {
            const loadedUrl = String(context.frame.dataset.loadedUrl || '');

            if (loadedUrl !== viewerUrl) {
                context.frame.src = viewerUrl;
                context.frame.dataset.loadedUrl = viewerUrl;
            }
            hide(context.message);
            show(context.frame);
            return;
        }

        context.frame.removeAttribute('src');
        context.frame.removeAttribute('data-loaded-url');
        hide(context.frame);
        writeContent(context.message, hasViewerMessage ? viewerMessage : 'No article selected.');
        show(context.message);
    });

    setActiveArticleCardState();
    setActiveDigestItemCardState();
}

function hideArticleViewer() {
    viewerOpen = false;
    viewerUrl = null;
    activeArticleId = null;
    viewerTitle = 'Article viewer';
    viewerMessage = '';

    renderArticleViewer();
}

function openArticleInViewer(articleId) {
    const article = getArticleById(articleId);
    const normalizedArticleId = Number(article?.id);
    const articleTitle = String(article?.title || 'Untitled').trim() || 'Untitled';
    const normalizedUrl = normalizeArticleUrl(article?.url);

    if (!Number.isInteger(normalizedArticleId) || normalizedArticleId <= 0) {
        return;
    }

    viewerOpen = true;
    viewerUrl = normalizedUrl;
    activeArticleId = normalizedArticleId;
    viewerTitle = articleTitle;
    viewerMessage = normalizedUrl ? '' : 'This article has no valid URL for the in-app viewer.';

    renderArticleViewer();
}

function openArticleExternal(articleId) {
    const article = getArticleById(articleId);
    const normalizedArticleId = Number(article?.id);
    const articleTitle = String(article?.title || 'Untitled').trim() || 'Untitled';
    const normalizedUrl = normalizeArticleUrl(article?.url);

    if (!Number.isInteger(normalizedArticleId) || normalizedArticleId <= 0) {
        return;
    }

    if (!normalizedUrl) {
        viewerOpen = true;
        viewerUrl = null;
        activeArticleId = normalizedArticleId;
        viewerTitle = articleTitle;
        viewerMessage = 'This article has no valid URL to open externally.';
        renderArticleViewer();
        return;
    }

    window.open(normalizedUrl, '_blank', 'noopener,noreferrer');
}

function openDigestItemCardInViewer(itemCard) {
    const articleId = Number(itemCard?.dataset.articleId);
    const hasArticleId = Number.isInteger(articleId) && articleId > 0;
    const itemTitle = String(itemCard?.dataset.itemTitle || 'Untitled').trim() || 'Untitled';
    const normalizedUrl = normalizeArticleUrl(itemCard?.dataset.itemUrl);

    if (hasArticleId) {
        openArticleInViewer(articleId);
        return;
    }

    viewerOpen = true;
    viewerUrl = normalizedUrl;
    activeArticleId = null;
    viewerTitle = itemTitle;
    viewerMessage = normalizedUrl ? '' : 'This article has no valid URL for the in-app viewer.';
    renderArticleViewer();
}

async function openDashboardWithTopicFilter(topicSlug, { switchToMain = false } = {}) {
    if (!dom.elements.filterTopic) {
        return;
    }

    const normalizedTopicSlug = String(topicSlug || '')
        .trim()
        .toLowerCase();

    if (!normalizedTopicSlug) {
        return;
    }

    let optionExists = Array.from(dom.elements.filterTopic.options).some(
        option => option.value === normalizedTopicSlug,
    );
    if (!optionExists) {
        await loadTopics();
        optionExists = Array.from(dom.elements.filterTopic.options).some(
            option => option.value === normalizedTopicSlug,
        );
        if (!optionExists) {
            return;
        }
    }

    dom.elements.filterTopic.value = normalizedTopicSlug;
    if (switchToMain) {
        setView('main');
        if (!articlesNeedsRefresh) {
            await loadArticles();
        }
        return;
    }

    await loadArticles();
}

async function openDashboardWithSourceFilter({ feedId, sourceName } = {}) {
    const numericFeedId = Number(feedId);
    let resolvedFeedId = Number.isInteger(numericFeedId) && numericFeedId > 0 ? String(numericFeedId) : '';

    if (!resolvedFeedId) {
        resolvedFeedId = findFeedIdBySourceName(sourceName);
    }

    if (!resolvedFeedId) {
        return;
    }

    let optionExists = Array.from(dom.elements.filterSource.options).some(option => option.value === resolvedFeedId);
    if (!optionExists) {
        await loadFeeds();
        optionExists = Array.from(dom.elements.filterSource.options).some(option => option.value === resolvedFeedId);
        if (!optionExists) {
            return;
        }
    }

    dom.elements.filterSource.value = resolvedFeedId;
    setView('main');
    await loadArticles();
}

async function loadFeeds() {
    show(dom.elements.feedsState);
    writeContent(dom.elements.feedsState, 'Loading…');
    try {
        state.feeds = await apiFetch('/api/feeds');
        renderFeeds();
        renderFilterOptions();
        renderDigestSettings();
    } catch (err) {
        writeContent(dom.elements.feedsState, `Error: ${err.message}`);
    }
}

function renderArticles(articles, { requestKey = '' } = {}) {
    const normalizedArticles = Array.isArray(articles) ? articles : [];
    const normalizedRequestKey = String(requestKey || '');
    const nextFingerprint = getArticlesPayloadFingerprint(normalizedArticles);
    const articleById = new Map();
    const shouldSkipRender =
        articlesRuntime.lastRequestKey === normalizedRequestKey &&
        articlesRuntime.lastRenderFingerprint === nextFingerprint;

    if (shouldSkipRender) {
        setActiveArticleCardState();
        return false;
    }

    articlesRuntime.lastRequestKey = normalizedRequestKey;
    articlesRuntime.lastRenderFingerprint = nextFingerprint;
    articlesRuntime.articleById = articleById;
    clearElement(dom.elements.articlesList);

    if (normalizedArticles.length === 0) {
        writeContent(dom.elements.articlesState, 'Nothing found, try other search input or delete all');
        show(dom.elements.articlesState);
        renderArticleViewer();
        return true;
    }

    hide(dom.elements.articlesState);
    const template = dom.templates.articleCard;
    const fragment = document.createDocumentFragment();

    normalizedArticles.forEach(article => {
        const node = template.content.cloneNode(true);
        const articleId = Number(article.id);
        const hasArticleId = Number.isInteger(articleId) && articleId > 0;
        const card = node.querySelector('.card');

        if (hasArticleId) {
            articleById.set(articleId, article);
        }

        if (card) {
            if (hasArticleId) {
                card.dataset.articleId = String(articleId);
                card.setAttribute('aria-label', article.title || 'Untitled article');
            } else {
                card.removeAttribute('data-article-id');
            }
        }

        const dateElement = node.querySelector('.meta-date');
        dateElement.textContent = formatTriageTime(article.publishedAt);
        if (article.publishedAt) {
            dateElement.dateTime = article.publishedAt;
            dateElement.title = formatDate(article.publishedAt);
        }

        const metaSource = node.querySelector('.meta-source');
        const sourceLogo = node.querySelector('.source-logo');
        const sourceName = node.querySelector('.source-name');
        const articleFeedId = Number(article.feedId);
        const hasFeedId = Number.isInteger(articleFeedId) && articleFeedId > 0;

        if (article.sourceLogoDataUrl) {
            sourceLogo.src = article.sourceLogoDataUrl;
            show(sourceLogo);
        } else {
            hide(sourceLogo);
        }

        sourceName.textContent = article.sourceName || '—';
        if (metaSource) {
            if (hasFeedId || article.sourceName) {
                metaSource.classList.add('is-clickable');
                metaSource.disabled = false;
                metaSource.title = `Filter by ${article.sourceName || 'source'}`;
                metaSource.dataset.sourceName = String(article.sourceName || '');
                if (hasFeedId) {
                    metaSource.dataset.feedId = String(articleFeedId);
                } else {
                    metaSource.removeAttribute('data-feed-id');
                }
            } else {
                metaSource.classList.remove('is-clickable');
                metaSource.disabled = true;
                metaSource.removeAttribute('data-feed-id');
                metaSource.removeAttribute('data-source-name');
            }
        }

        const titleButton = node.querySelector('.article-title-button');
        titleButton.textContent = article.title || 'Untitled';
        node.querySelector('.teaser').textContent = article.teaser || '';

        const contentEl = node.querySelector('.content');
        const articleTopics = Array.isArray(article.topics) ? article.topics : [];
        if (contentEl && articleTopics.length > 0) {
            const topicRow = document.createElement('div');
            topicRow.className = 'article-topics';

            articleTopics.forEach(topic => {
                const chip = document.createElement('button');
                chip.type = 'button';
                chip.className = 'article-topic-chip';
                chip.textContent = topic.label || topic.slug || 'topic';
                chip.title = `${topic.label || topic.slug} (${Number(topic.score || 0).toFixed(2)})`;

                const topicSlug = String(topic.slug || '')
                    .trim()
                    .toLowerCase();
                if (topicSlug) {
                    chip.dataset.topicSlug = topicSlug;
                    chip.disabled = false;
                    if (dom.elements.filterTopic && dom.elements.filterTopic.value === topicSlug) {
                        chip.classList.add('is-active');
                    }
                } else {
                    chip.disabled = true;
                    chip.removeAttribute('data-topic-slug');
                }

                topicRow.appendChild(chip);
            });

            contentEl.appendChild(topicRow);
        }

        const readBtn = node.querySelector('.btn-read');
        const externalBtn = node.querySelector('.btn-open-external');
        const saveBtn = node.querySelector('.article-save-btn');
        const dismissBtn = node.querySelector('.article-dismiss-btn');

        if (readBtn) {
            if (hasArticleId) {
                readBtn.dataset.articleId = String(articleId);
                readBtn.disabled = false;
            } else {
                readBtn.disabled = true;
                readBtn.removeAttribute('data-article-id');
            }
        }

        if (externalBtn) {
            if (hasArticleId) {
                externalBtn.dataset.articleId = String(articleId);
                externalBtn.disabled = false;
                externalBtn.setAttribute('aria-label', `Open ${article.title || 'article'} externally`);
            } else {
                externalBtn.disabled = true;
                externalBtn.removeAttribute('data-article-id');
                externalBtn.setAttribute('aria-label', 'Open externally');
            }
        }

        if (saveBtn) {
            if (hasArticleId) {
                saveBtn.dataset.articleId = String(articleId);
                saveBtn.disabled = false;
                saveBtn.classList.toggle('is-saved', Boolean(article.saved));
                saveBtn.textContent = article.saved ? 'Saved' : 'Save';
            } else {
                saveBtn.disabled = true;
                saveBtn.removeAttribute('data-article-id');
            }
        }

        if (dismissBtn) {
            if (hasArticleId) {
                dismissBtn.dataset.articleId = String(articleId);
                dismissBtn.disabled = false;
            } else {
                dismissBtn.disabled = true;
                dismissBtn.removeAttribute('data-article-id');
            }
        }

        fragment.appendChild(node);
    });

    articlesRuntime.articleById = articleById;
    dom.elements.articlesList.appendChild(fragment);
    renderArticleViewer();
    return true;
}

function getFeedCards() {
    return Array.from(dom.elements.articlesList?.querySelectorAll('.feed-card[data-article-id]') || []);
}

function setFocusedFeedArticle(articleId, { focus = false, scroll = false } = {}) {
    const normalizedId = Number(articleId);
    const cards = getFeedCards();
    const target = cards.find(card => Number(card.dataset.articleId) === normalizedId) || null;
    if (!target) {
        return null;
    }

    articlesRuntime.focusedArticleId = normalizedId;
    cards.forEach(card => card.classList.toggle('is-keyboard-active', card === target));
    if (focus) {
        target.focus({ preventScroll: true });
    }
    if (scroll) {
        target.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }
    return target;
}

function moveFeedArticleSelection(direction) {
    const cards = getFeedCards();
    if (cards.length === 0) {
        return;
    }
    const currentIndex = cards.findIndex(card => Number(card.dataset.articleId) === articlesRuntime.focusedArticleId);
    const startIndex = direction > 0 ? 0 : cards.length - 1;
    const nextIndex = currentIndex < 0 ? startIndex : Math.max(0, Math.min(cards.length - 1, currentIndex + direction));
    setFocusedFeedArticle(Number(cards[nextIndex].dataset.articleId), { focus: true, scroll: true });
}

async function dismissFeedArticle(articleId) {
    const normalizedId = Number(articleId);
    const card = getFeedCards().find(item => Number(item.dataset.articleId) === normalizedId);
    if (!Number.isInteger(normalizedId) || normalizedId <= 0 || !card) {
        return;
    }

    const nextCard = card.nextElementSibling || card.previousElementSibling;
    card.classList.add('is-removing');
    card.remove();
    articlesRuntime.articleById.delete(normalizedId);
    articlesRuntime.articles = articlesRuntime.articles.filter(article => Number(article?.id) !== normalizedId);
    if (nextCard?.dataset?.articleId) {
        setFocusedFeedArticle(Number(nextCard.dataset.articleId));
    }

    try {
        await apiFetch(`/api/articles/${normalizedId}/dismissed`, {
            method: 'PATCH',
            body: JSON.stringify({ dismissed: true }),
        });
        await loadFeedCount();
        showToast('Article dismissed', {
            actionLabel: 'Undo',
            onAction: async () => {
                await apiFetch(`/api/articles/${normalizedId}/dismissed`, {
                    method: 'PATCH',
                    body: JSON.stringify({ dismissed: false }),
                });
                articlesRuntime.lastRenderFingerprint = '';
                await loadArticles({ showLoadingRow: false });
            },
        });
    } catch (err) {
        articlesRuntime.lastRenderFingerprint = '';
        await loadArticles({ showLoadingRow: false });
        showToast(`Dismiss failed: ${err.message}`);
    }
}

function renderDigestClusters(payload) {
    if (!dom.digest.list || !dom.digest.state || !dom.digest.cluster) {
        return;
    }

    const activeRange = normalizeDigestRange(payload?.variant || digestRange);
    const clusters = sortDigestClusters(Array.isArray(payload?.clusters) ? payload.clusters : []);
    const digestArticleById = new Map();
    clearElement(dom.digest.list);
    digestRuntime.articleById = digestArticleById;

    if (clusters.length === 0) {
        writeContent(dom.digest.state, getDigestEmptyStateMessage(activeRange));
        show(dom.digest.state);
        renderArticleViewer();
        return;
    }

    hide(dom.digest.state);
    const fragment = document.createDocumentFragment();

    clusters.forEach(cluster => {
        const representative = cluster.representative || cluster.items?.[0] || {};
        const items = Array.isArray(cluster.items) ? cluster.items : [];
        const node = dom.digest.cluster.content.cloneNode(true);
        const countEl = node.querySelector('.digest-cluster-count');
        const dateEl = node.querySelector('.digest-cluster-date');
        const titleEl = node.querySelector('.digest-cluster-title');
        const sourcesEl = node.querySelector('.digest-cluster-sources');
        const itemsGridEl = node.querySelector('.digest-items-grid');
        const clusterTopics = getDigestClusterTopics(items);
        const sortedDates = items
            .map(item => new Date(item.publishedAt))
            .filter(date => Number.isFinite(date.getTime()))
            .sort((left, right) => left.getTime() - right.getTime());
        const firstReport = sortedDates[0] || null;
        const latestReport = sortedDates[sortedDates.length - 1] || null;

        if (countEl) {
            const sourceCount = new Set(items.map(item => String(item.sourceName || '').trim()).filter(Boolean)).size;
            countEl.textContent = `${sourceCount || 1} ${sourceCount === 1 ? 'source' : 'sources'}`;
        }
        if (dateEl) {
            const latest = latestReport ? formatTriageTime(latestReport.toISOString()) : '—';
            const first = firstReport ? formatTriageTime(firstReport.toISOString()) : '—';
            dateEl.textContent = `Latest ${latest} · First report ${first}`;
        }
        if (titleEl) {
            titleEl.textContent = cluster.clusterTitle || representative.title || 'Untitled';
        }
        if (sourcesEl) {
            clearElement(sourcesEl);
            const sourcesMap = new Map();
            items.forEach(item => {
                const key = String(item.sourceName || 'Unknown source');

                if (!sourcesMap.has(key)) {
                    const itemFeedId = Number(item.feedId);
                    sourcesMap.set(key, {
                        name: key,
                        logo: item.sourceLogoDataUrl || null,
                        feedId: Number.isInteger(itemFeedId) && itemFeedId > 0 ? itemFeedId : null,
                    });
                }
            });
            const sources = Array.from(sourcesMap.values());

            sourcesEl.classList.toggle('is-single-source', sources.length === 1);

            if (sources.length === 0) {
                const empty = document.createElement('span');
                empty.className = 'digest-source-chip';
                empty.textContent = 'Unknown source';
                sourcesEl.appendChild(empty);
            } else {
                sources.forEach(source => {
                    const chip = document.createElement('button');
                    chip.type = 'button';
                    chip.className = 'digest-source-chip';
                    chip.dataset.sourceName = source.name;
                    chip.classList.add('is-clickable');
                    chip.setAttribute('aria-label', `Filter feed by ${source.name}`);
                    if (source.feedId) chip.dataset.feedId = String(source.feedId);

                    if (source.logo) {
                        const logo = document.createElement('img');
                        logo.className = 'digest-source-logo';
                        logo.src = source.logo;
                        logo.alt = '';
                        chip.appendChild(logo);
                    }

                    const text = document.createElement('span');
                    text.className = 'digest-source-name';
                    text.textContent = source.name;
                    chip.appendChild(text);
                    sourcesEl.appendChild(chip);
                });
            }
        }

        if (itemsGridEl) {
            const clusterArticleIds = getDigestClusterArticleIds(cluster);
            clearElement(itemsGridEl);
            itemsGridEl.classList.toggle('is-single-item', items.length === 1);
            items.forEach(item => {
                const card = document.createElement('article');
                const normalizedItemUrl = normalizeArticleUrl(item.url);
                const hasUrl = Boolean(normalizedItemUrl);
                const itemArticleId = Number(item.id);
                const hasItemArticleId = Number.isInteger(itemArticleId) && itemArticleId > 0;
                card.className = hasUrl ? 'digest-item-card digest-item-card-link' : 'digest-item-card';
                card.dataset.itemTitle = String(item.title || 'Untitled');
                card.dataset.sourceName = String(item.sourceName || 'Unknown source');

                if (hasUrl) {
                    card.setAttribute('role', 'link');
                    card.tabIndex = 0;
                    card.dataset.itemUrl = normalizedItemUrl;
                } else {
                    card.removeAttribute('role');
                    card.removeAttribute('tabindex');
                    card.removeAttribute('data-item-url');
                }

                if (hasItemArticleId) {
                    digestArticleById.set(itemArticleId, item);
                    card.dataset.articleId = String(itemArticleId);
                } else {
                    card.removeAttribute('data-article-id');
                }

                const meta = document.createElement('div');
                meta.className = 'digest-item-card-meta';

                const sourceWrap = document.createElement('span');
                sourceWrap.className = 'digest-item-source-wrap';

                if (item.sourceLogoDataUrl) {
                    const sourceLogo = document.createElement('img');
                    sourceLogo.className = 'digest-item-source-logo';
                    sourceLogo.src = item.sourceLogoDataUrl;
                    sourceLogo.alt = '';
                    sourceWrap.appendChild(sourceLogo);
                }

                const source = document.createElement('span');
                source.className = 'digest-item-source';
                source.textContent = item.sourceName || '—';
                sourceWrap.appendChild(source);

                const published = document.createElement('span');
                published.className = 'digest-item-date';
                published.textContent = formatTriageTime(item.publishedAt);

                meta.appendChild(sourceWrap);
                meta.appendChild(published);

                const itemTitle = document.createElement('h4');
                itemTitle.className = 'digest-item-title';
                itemTitle.textContent = item.title || 'Untitled';

                const itemContent = document.createElement('div');
                itemContent.className = 'digest-item-content';
                itemContent.appendChild(itemTitle);

                const itemDescription = document.createElement('p');
                itemDescription.className = 'digest-item-teaser';
                itemDescription.textContent =
                    item.teaser || item.summary || item.description || 'No description available.';
                itemContent.appendChild(itemDescription);

                const openButton = document.createElement('button');
                openButton.type = 'button';
                openButton.className = 'btn ghost digest-item-open-btn';
                openButton.textContent = 'Open link ↗';
                openButton.disabled = !hasUrl;
                if (hasItemArticleId) openButton.dataset.articleId = String(itemArticleId);
                if (hasUrl) openButton.dataset.itemUrl = normalizedItemUrl;

                card.appendChild(meta);
                card.appendChild(itemContent);
                card.appendChild(openButton);
                itemsGridEl.appendChild(card);
            });

            const clusterCard = node.querySelector('.digest-cluster');
            if (clusterCard) {
                const clusterFooter = document.createElement('div');
                clusterFooter.className = 'digest-cluster-footer';

                const clusterTopicsEl = document.createElement('div');
                clusterTopicsEl.className = 'digest-cluster-topics';

                if (clusterTopics.length > 0) {
                    clusterTopics.forEach(topic => {
                        const chip = document.createElement('button');
                        chip.type = 'button';
                        chip.className = 'digest-topic-chip';
                        chip.textContent = topic.label;
                        chip.title = topic.slug || topic.label;

                        if (topic.slug) {
                            chip.classList.add('is-clickable');
                            chip.dataset.topicSlug = topic.slug;
                            if (dom.elements.filterTopic && dom.elements.filterTopic.value === topic.slug) {
                                chip.classList.add('is-active');
                            }
                        } else {
                            chip.disabled = true;
                            chip.removeAttribute('data-topic-slug');
                        }

                        clusterTopicsEl.appendChild(chip);
                    });
                }

                const clusterActions = document.createElement('div');
                clusterActions.className = 'digest-cluster-actions';

                const clusterAddBtn = document.createElement('button');
                clusterAddBtn.type = 'button';
                clusterAddBtn.className = 'btn ghost digest-cluster-add-btn';
                clusterAddBtn.textContent = 'Save story';
                clusterAddBtn.disabled = clusterArticleIds.length === 0;
                if (clusterAddBtn.disabled) {
                    clusterAddBtn.removeAttribute('data-article-ids');
                } else {
                    clusterAddBtn.dataset.articleIds = serializeArticleIdsForDataset(clusterArticleIds);
                }

                const clusterDigestBtn = document.createElement('button');
                clusterDigestBtn.type = 'button';
                clusterDigestBtn.className = 'btn ghost digest-cluster-digest-btn';
                clusterDigestBtn.textContent = 'Mark as digested';
                clusterDigestBtn.disabled = clusterArticleIds.length === 0;

                if (clusterDigestBtn.disabled) {
                    clusterDigestBtn.removeAttribute('data-article-ids');
                } else {
                    clusterDigestBtn.dataset.articleIds = serializeArticleIdsForDataset(clusterArticleIds);
                }

                clusterActions.appendChild(clusterAddBtn);
                clusterActions.appendChild(clusterDigestBtn);
                clusterFooter.appendChild(clusterTopicsEl);
                clusterFooter.appendChild(clusterActions);
                clusterCard.appendChild(clusterFooter);
            }
        }

        fragment.appendChild(node);
    });

    dom.digest.list.appendChild(fragment);
    renderArticleViewer();
}

function renderDigestSubtitle(payload) {
    if (!dom.digest.subtitle) {
        return;
    }

    const activeRange = normalizeDigestRange(payload?.variant || digestRange);
    const totalArticles = Number(payload?.totalArticles || 0);
    const totalClusters = Number(payload?.totalClusters || 0);

    writeContent(
        dom.digest.subtitle,
        `${getDigestRangeLabel(activeRange)} · ${totalClusters.toLocaleString('en-US')} stories · ${totalArticles.toLocaleString('en-US')} sources`,
    );
}

function isViewActive(name) {
    const view = dom.views[name] || null;

    return Boolean(view?.classList.contains('is-active'));
}

function getDigestPayloadFingerprint(payload) {
    if (!payload || !Array.isArray(payload.clusters)) {
        return '';
    }

    const clusterFingerprint = payload.clusters
        .map(cluster => {
            const representativeId = Number(cluster?.representative?.id || 0);
            const clusterCount = Number(cluster?.clusterCount || 0);
            const representativeDate = cluster?.representative?.publishedAt || '';

            return `${representativeId}:${clusterCount}:${representativeDate}`;
        })
        .join('|');

    return [
        normalizeDigestRange(payload?.variant || 'day'),
        payload.startIso || '',
        payload.endIso || '',
        Number(payload.totalArticles || 0),
        Number(payload.totalClusters || 0),
        clusterFingerprint,
    ].join('::');
}

function requestDigestRefresh({ force = false } = {}) {
    digestRuntime.needsRefresh = true;
    if (isViewActive('digest')) {
        void loadDigest({ force, silent: true });
    }
}

async function loadDigest({ force = false, silent = false } = {}) {
    const activeRange = normalizeDigestRange(digestRange);
    const payloadRange = normalizeDigestRange(digestRuntime.lastPayload?.variant || 'day');
    const hasMatchingPayload = digestRuntime.lastPayload && payloadRange === activeRange;
    const hasActiveLoad = Boolean(digestRuntime.loadPromise);

    if (!dom.digest.state || !dom.digest.list) {
        return;
    }

    if (!force && !digestRuntime.needsRefresh && hasMatchingPayload) {
        return;
    }

    if (hasActiveLoad && digestRuntime.activeRequestRange === activeRange && !force) {
        return digestRuntime.loadPromise;
    }
    if (hasActiveLoad && digestRuntime.activeRequestController) {
        digestRuntime.activeRequestController.abort();
    }

    const requestId = digestRuntime.requestId + 1;
    const controller = new AbortController();

    const showLoadingState = !silent || !digestRuntime.lastPayload;
    digestRuntime.requestId = requestId;
    digestRuntime.activeRequestController = controller;
    digestRuntime.activeRequestRange = activeRange;

    if (showLoadingState) {
        clearElement(dom.digest.list);
        writeContent(dom.digest.state, 'Loading…');
        show(dom.digest.state);
        updateDigestMarkAllButton({ clusters: [] });
    }

    let loadPromise = null;
    loadPromise = (async () => {
        try {
            const query = new URLSearchParams({ variant: activeRange });
            const payload = await apiFetch(`/api/articles/digest?${query.toString()}`, { signal: controller.signal });

            if (requestId !== digestRuntime.requestId) {
                return;
            }

            const normalizedPayload = {
                ...payload,
                variant: normalizeDigestRange(payload?.variant || activeRange),
            };
            const nextFingerprint = getDigestPayloadFingerprint(normalizedPayload);
            const hasDigestChanged = nextFingerprint !== digestRuntime.lastRenderFingerprint;

            digestRuntime.lastPayload = normalizedPayload;
            digestRuntime.needsRefresh = false;
            renderDigestSubtitle(normalizedPayload);

            if (hasDigestChanged || showLoadingState || force) {
                renderDigestClusters(normalizedPayload);
                digestRuntime.lastRenderFingerprint = nextFingerprint;
            }

            updateDigestMarkAllButton(normalizedPayload);
        } catch (err) {
            if (isAbortError(err)) {
                return;
            }
            if (requestId !== digestRuntime.requestId) {
                return;
            }

            if (!digestRuntime.lastPayload) {
                digestRuntime.lastPayload = null;
                digestRuntime.lastRenderFingerprint = '';
                writeContent(dom.digest.state, `Error: ${err.message}`);
                show(dom.digest.state);
                updateDigestMarkAllButton({ clusters: [] });
            }
            if (dom.digest.subtitle) {
                writeContent(dom.digest.subtitle, 'Failed to load digest.');
            }
        } finally {
            if (digestRuntime.activeRequestController === controller) {
                digestRuntime.activeRequestController = null;
            }
            if (digestRuntime.loadPromise === loadPromise) {
                digestRuntime.loadPromise = null;
            }
            if (digestRuntime.requestId === requestId && digestRuntime.activeRequestRange === activeRange) {
                digestRuntime.activeRequestRange = '';
            }
        }
    })();
    digestRuntime.loadPromise = loadPromise;

    return loadPromise;
}

function updateArticlesPaginationUi() {
    if (!dom.elements.articlesLoadMore) {
        return;
    }

    dom.elements.articlesLoadMore.classList.toggle('hide', !articlesRuntime.hasMore);
    dom.elements.articlesLoadMore.disabled = articlesRuntime.isLoadingMore;
    dom.elements.articlesLoadMore.textContent = articlesRuntime.isLoadingMore ? 'Loading more…' : 'Load more';
}

async function loadArticles({ showLoadingRow = true, append = false } = {}) {
    if (append && (!articlesRuntime.hasMore || articlesRuntime.isLoadingMore)) {
        return;
    }

    const params = new URLSearchParams();
    const selectedList = dom.elements.filterList.value;
    const selected = dom.elements.filterSource.value;
    const selectedTopic = dom.elements.filterTopic?.value || '';
    renderActiveFilterChips();

    hide(dom.elements.articlesState);
    writeContent(dom.elements.articlesState, '');
    if (showLoadingRow) {
        show(dom.elements.loadingRow);
    }

    if (selected) {
        params.set('feedId', selected);
    }
    if (selectedList) {
        params.set('listId', selectedList);
    }
    if (selectedTopic) {
        params.set('topic', selectedTopic);
    }

    const query = dom.elements.searchInput.value.trim();

    if (query) {
        params.set('query', query);
    }
    const requestKey = params.toString();
    const offset = append ? articlesRuntime.articles.length : 0;
    params.set('limit', String(CONFIG.ARTICLES_PAGE_SIZE + 1));
    params.set('offset', String(offset));
    const requestId = articlesRuntime.requestId + 1;
    const controller = new AbortController();

    if (articlesRuntime.activeRequestController) {
        articlesRuntime.activeRequestController.abort();
    }
    articlesRuntime.requestId = requestId;
    articlesRuntime.activeRequestController = controller;
    articlesRuntime.isLoadingMore = append;
    if (!append) {
        articlesRuntime.hasMore = false;
    }
    updateArticlesPaginationUi();

    try {
        const page = await apiFetch(`/api/articles?${params.toString()}`, { signal: controller.signal });

        if (requestId !== articlesRuntime.requestId) {
            return;
        }

        const normalizedPage = Array.isArray(page) ? page : [];
        const visiblePage = normalizedPage.slice(0, CONFIG.ARTICLES_PAGE_SIZE);
        const combined = append
            ? [...articlesRuntime.articles, ...visiblePage].filter(
                  (article, index, articles) =>
                      articles.findIndex(candidate => Number(candidate?.id) === Number(article?.id)) === index,
              )
            : visiblePage;

        articlesRuntime.articles = combined;
        articlesRuntime.hasMore = normalizedPage.length > CONFIG.ARTICLES_PAGE_SIZE;
        hide(dom.elements.loadingRow);
        renderArticles(combined, { requestKey });
        await loadFeedCount();
        articlesNeedsRefresh = false;
    } catch (err) {
        if (isAbortError(err)) {
            return;
        }
        if (requestId !== articlesRuntime.requestId) {
            return;
        }

        hide(dom.elements.loadingRow);
        if (append) {
            showToast(`Could not load more articles: ${err.message}`);
            return;
        }
        writeContent(dom.elements.articlesState, `Error: ${err.message}`);
        show(dom.elements.articlesState);
        articlesNeedsRefresh = true;
    } finally {
        articlesRuntime.isLoadingMore = false;
        updateArticlesPaginationUi();
        if (articlesRuntime.activeRequestController === controller) {
            articlesRuntime.activeRequestController = null;
        }
    }
}

async function loadFeedCount() {
    if (!dom.elements.feedCount) {
        return;
    }
    try {
        const stats = await apiFetch('/api/articles/stats');
        const unread = Number(stats?.unread || 0);
        writeContent(dom.elements.feedCount, `${unread.toLocaleString('en-US')} unread`);
    } catch {
        writeContent(dom.elements.feedCount, 'Unread count unavailable');
    }
}

function normalizeSearchQuery(value) {
    return String(value || '')
        .trim()
        .replace(/\s+/g, ' ')
        .slice(0, CONFIG.MAX_SEARCH_QUERY_LENGTH);
}

function hasRenderedArticles() {
    if (!dom.elements.articlesList) {
        return false;
    }

    return dom.elements.articlesList.children.length > 0;
}

function scrollArticlesToTop() {
    if (articlesScroll) {
        articlesScroll.scrollTop = 0;
    }
    window.scrollTo(0, 0);
}

function hasDashboardFiltersApplied() {
    const hasQuery = normalizeSearchQuery(dom.elements.searchInput?.value || '').length > 0;
    const hasListFilter = Boolean(dom.elements.filterList?.value);
    const hasSourceFilter = Boolean(dom.elements.filterSource?.value);
    const hasTopicFilter = Boolean(dom.elements.filterTopic?.value);

    return hasQuery || hasListFilter || hasSourceFilter || hasTopicFilter;
}

function getSelectedOptionLabel(select, prefix) {
    const label = String(select?.selectedOptions?.[0]?.textContent || '').trim();
    return label.replace(new RegExp(`^${prefix}:\\s*`, 'i'), '') || label;
}

function renderActiveFilterChips() {
    if (!dom.elements.activeFilterRow || !dom.elements.activeFilterChips) {
        return;
    }

    const filters = [];
    const query = normalizeSearchQuery(dom.elements.searchInput?.value || '');
    if (query) {
        filters.push({ key: 'query', label: `“${query}”` });
    }
    if (dom.elements.filterTopic?.value) {
        filters.push({ key: 'topic', label: getSelectedOptionLabel(dom.elements.filterTopic, 'Topic') });
    }
    if (dom.elements.filterSource?.value) {
        filters.push({ key: 'source', label: getSelectedOptionLabel(dom.elements.filterSource, 'Source') });
    }
    if (dom.elements.filterList?.value) {
        filters.push({ key: 'list', label: getSelectedOptionLabel(dom.elements.filterList, 'List') });
    }

    clearElement(dom.elements.activeFilterChips);
    filters.forEach(filter => {
        const chip = document.createElement('button');
        chip.type = 'button';
        chip.className = 'active-filter-chip';
        chip.dataset.filterKey = filter.key;
        chip.setAttribute('aria-label', `Remove ${filter.label} filter`);
        chip.textContent = `${filter.label} ×`;
        dom.elements.activeFilterChips.appendChild(chip);
    });
    dom.elements.activeFilterRow.classList.toggle('hide', filters.length === 0);
}

async function clearSingleDashboardFilter(key) {
    if (key === 'query') {
        dom.elements.searchInput.value = '';
    } else if (key === 'topic' && dom.elements.filterTopic) {
        dom.elements.filterTopic.value = '';
    } else if (key === 'source') {
        dom.elements.filterSource.value = '';
    } else if (key === 'list') {
        dom.elements.filterList.value = '';
    }
    renderActiveFilterChips();
    await loadArticles();
}

async function clearDashboardFilters() {
    if (clearDashboardPromise) {
        return clearDashboardPromise;
    }
    const hasFiltersApplied = hasDashboardFiltersApplied();
    const hasPendingSearchWork = Boolean(searchTimer || searchLoadingTimer);

    if (!hasFiltersApplied && !hasPendingSearchWork) {
        return;
    }

    clearDashboardPromise = (async () => {
        if (searchTimer) {
            clearTimeout(searchTimer);
            searchTimer = null;
        }
        if (searchLoadingTimer) {
            clearTimeout(searchLoadingTimer);
            searchLoadingTimer = null;
        }
        hide(dom.elements.loadingRow);

        if (!hasFiltersApplied) {
            return;
        }

        dom.elements.searchInput.value = '';
        dom.elements.filterList.value = '';
        dom.elements.filterSource.value = '';
        if (dom.elements.filterTopic) {
            dom.elements.filterTopic.value = '';
        }
        renderActiveFilterChips();
        scrollArticlesToTop();
        await loadArticles();
    })();

    try {
        await clearDashboardPromise;
    } finally {
        clearDashboardPromise = null;
    }
}

async function searchFromSelection(value) {
    const query = normalizeSearchQuery(value);

    if (!query) {
        return;
    }

    if (searchTimer) {
        clearTimeout(searchTimer);
        searchTimer = null;
    }

    dom.elements.searchInput.value = query;
    setView('main');
    await loadArticles();
}

function resetForm() {
    state.editingId = null;
    dom.elements.feedName.value = '';
    dom.elements.feedWebsite.value = '';
    dom.elements.feedUrl.value = '';
    dom.elements.feedSubmit.textContent = 'Feed speichern';
    setStatus(dom.elements.feedFormStatus, '');
}

function closeSse() {
    if (!sse) {
        return;
    }

    sse.close();
    sse = null;
}

function abortInFlightRequests() {
    if (articlesRuntime.activeRequestController) {
        articlesRuntime.activeRequestController.abort();
        articlesRuntime.activeRequestController = null;
    }
    if (digestRuntime.activeRequestController) {
        digestRuntime.activeRequestController.abort();
        digestRuntime.activeRequestController = null;
    }
}

function setupSse() {
    if (sse) {
        return;
    }

    sse = new EventSource('/api/events');
    sse.addEventListener('update', event => {
        try {
            const payload = JSON.parse(event.data || '{}');
            const eventName = payload.event || '';
            if (eventName === 'fetch.completed') {
                articlesNeedsRefresh = true;
                if (isViewActive('main')) {
                    loadArticles();
                }
                requestDigestRefresh();
                loadFetchStatus();
            }
            if (eventName === 'articles.updated') {
                const source = payload?.data?.source || '';
                if (
                    (source === 'digest' || source === 'daily-digest') &&
                    digestRuntime.pendingMutationEventsToSkip > 0
                ) {
                    digestRuntime.pendingMutationEventsToSkip -= 1;
                    return;
                }
                requestDigestRefresh();
            }
            if (eventName.startsWith('webhook.')) {
                requestDigestRefresh();
            }
            if (eventName === 'feeds.updated') {
                loadFeeds();
                loadDigestSettings({ silent: true });
            }
            if (eventName === 'lists.updated') {
                loadLists();
            }
            if (eventName === 'lists.items.updated') {
                articlesNeedsRefresh = true;
                if (isViewActive('main')) {
                    loadArticles();
                }
            }
            if (eventName === 'digest.settings.updated') {
                loadDigestSettings({ silent: true });
                requestDigestRefresh({ force: true });
            }
            if (eventName === 'topics.updated') {
                loadTopics();
                loadTopicRulesJson();
            }
            if (eventName === 'topics.reprocessed') {
                loadTopics();
            }
        } catch {
            return;
        }
    });
}

async function boot() {
    applyThemeState();
    applyLayoutState();
    applyViewerWidthState();
    renderArticleViewer();
    updateStickySubnavScrollState();
    await loadFeeds();
    await loadDigestSettings();
    await loadLists();
    await loadTopics();
    await loadTopicRulesJson();
    await loadArticles();
    await loadDigest({ force: true });
    await loadFetchStatus();
    return setupSse();
}

navLinks.forEach(link => {
    link.addEventListener('click', () => setView(link.dataset.view));
});

settingsTabs.forEach(tab => {
    tab.addEventListener('click', () => {
        settingsTabs.forEach(button => {
            button.classList.toggle('is-active', button === tab);
        });
        settingsPanels.forEach(panel => {
            panel.classList.toggle('is-active', panel.id === `settings-${tab.dataset.settings}`);
        });
    });
});

dom.elements.feedForm.addEventListener('submit', async event => {
    event.preventDefault();
    dom.elements.feedSubmit.disabled = true;
    setStatus(dom.elements.feedFormStatus, 'Saving…');

    const payload = {
        name: dom.elements.feedName.value.trim(),
        websiteUrl: dom.elements.feedWebsite.value.trim(),
        feedUrl: dom.elements.feedUrl.value.trim(),
    };

    try {
        if (state.editingId) {
            await apiFetch(`/api/feeds/${state.editingId}`, {
                method: 'PUT',
                body: JSON.stringify(payload),
            });
        } else {
            await apiFetch('/api/feeds', {
                method: 'POST',
                body: JSON.stringify(payload),
            });
        }
        resetForm();
        await loadFeeds();
    } catch (err) {
        setStatus(dom.elements.feedFormStatus, `Error: ${err.message}`);
    } finally {
        dom.elements.feedSubmit.disabled = false;
    }
});

dom.elements.feedCancel.addEventListener('click', () => resetForm());

dom.elements.listForm.addEventListener('submit', async event => {
    event.preventDefault();
    dom.elements.listSubmit.disabled = true;
    setStatus(dom.elements.listFormStatus, 'Saving…');

    const colorValue =
        dom.elements.listColor && dom.elements.listColor.value ? dom.elements.listColor.value.trim() : '#1d1d1f';
    const normalizedColor = colorValue.startsWith('#') ? colorValue : `#${colorValue}`;

    const payload = {
        name: dom.elements.listName.value.trim(),
        description: dom.elements.listDescription.value.trim(),
        color: normalizedColor || '#1d1d1f',
    };

    try {
        if (listEditingId) {
            await apiFetch(`/api/lists/${listEditingId}`, {
                method: 'PUT',
                body: JSON.stringify(payload),
            });
        } else {
            await apiFetch('/api/lists', {
                method: 'POST',
                body: JSON.stringify(payload),
            });
        }
        resetListForm();
        await loadLists();
    } catch (err) {
        setStatus(dom.elements.listFormStatus, `Error: ${err.message}`);
    } finally {
        dom.elements.listSubmit.disabled = false;
    }
});

dom.elements.listCancel.addEventListener('click', () => resetListForm());

if (dom.elements.topicForm) {
    dom.elements.topicForm.addEventListener('submit', submitTopicForm);
}
if (dom.elements.topicCancel) {
    dom.elements.topicCancel.addEventListener('click', () => resetTopicForm());
}
if (dom.elements.topicsJsonValidateBtn) {
    dom.elements.topicsJsonValidateBtn.addEventListener('click', async () => {
        await validateTopicRulesJson();
    });
}
if (dom.elements.topicsJsonSaveBtn) {
    dom.elements.topicsJsonSaveBtn.addEventListener('click', async () => {
        await saveTopicRulesJson();
    });
}
if (dom.elements.topicsReprocessBtn) {
    dom.elements.topicsReprocessBtn.addEventListener('click', async () => {
        await reprocessTopicsForAllArticles();
    });
}

dom.modal.modalClose.addEventListener('click', () => closeListModal());
dom.modal.modalCancel.addEventListener('click', () => closeListModal());
dom.modal.modalBackdrop.setAttribute('tabindex', '-1');
dom.modal.modalBackdrop.addEventListener('keydown', handleListModalKeydown);
dom.modal.modalBackdrop.addEventListener('click', event => {
    if (event.target === dom.modal.modalBackdrop) {
        closeListModal();
    }
});
dom.modal.modalConfirm.addEventListener('click', async () => {
    const listId = dom.modal.modalListSelect.value;

    if (!listId || pendingArticleIds.length === 0) {
        alert('Please choose a list.');
        return;
    }
    try {
        await apiFetch(`/api/lists/${listId}/items/bulk`, {
            method: 'POST',
            body: JSON.stringify({ articleIds: pendingArticleIds }),
        });
        closeListModal();
        articlesRuntime.lastRenderFingerprint = '';
        if (isViewActive('main')) {
            await loadArticles({ showLoadingRow: false });
        }
        showToast('Saved to list');
    } catch (err) {
        alert(err.message);
    }
});

dom.elements.feedTest.addEventListener('click', async () => {
    const url = dom.elements.feedUrl.value.trim();

    if (!url) {
        setStatus(dom.elements.feedFormStatus, 'Please enter a feed URL.');
        return;
    }

    dom.elements.feedTest.disabled = true;
    setStatus(dom.elements.feedFormStatus, 'Testing feed…');
    try {
        const result = await apiFetch(`/api/feeds/test/url?url=${encodeURIComponent(url)}`);
        const titles = result.sampleTitles?.length ? `Examples: ${result.sampleTitles.join(' · ')}` : '';
        setStatus(dom.elements.feedFormStatus, `OK: ${result.itemCount} Items. ${titles}`);
    } catch (err) {
        setStatus(dom.elements.feedFormStatus, `Error: ${err.message}`);
    } finally {
        dom.elements.feedTest.disabled = false;
    }
});

dom.elements.filterSource.addEventListener('change', () => loadArticles());
dom.elements.filterList.addEventListener('change', () => loadArticles());
if (dom.elements.filterTopic) {
    dom.elements.filterTopic.addEventListener('change', () => loadArticles());
}
if (dom.elements.articlesLoadMore) {
    dom.elements.articlesLoadMore.addEventListener('click', () => {
        void loadArticles({ showLoadingRow: false, append: true });
    });

    if ('IntersectionObserver' in window) {
        const articlesPaginationObserver = new IntersectionObserver(
            entries => {
                if (entries.some(entry => entry.isIntersecting) && isViewActive('main')) {
                    void loadArticles({ showLoadingRow: false, append: true });
                }
            },
            { rootMargin: '320px 0px' },
        );
        articlesPaginationObserver.observe(dom.elements.articlesLoadMore);
    }
}
if (dom.elements.feedBackToTop) {
    dom.elements.feedBackToTop.addEventListener('click', () => {
        const behavior = window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth';
        articlesScroll?.scrollTo({ top: 0, behavior });
        window.scrollTo({ top: 0, behavior });
    });
}
const viewerHideButtons = [dom.elements.articleViewerHideBtn, dom.digest.viewerHideBtn];
const viewerWidthOptions = getViewerWidthOptions();

viewerHideButtons.forEach(button => {
    if (!button) {
        return;
    }

    button.addEventListener('click', () => {
        hideArticleViewer();
    });
});

if (viewerWidthOptions.length > 0) {
    updateViewerWidthAvailability();
    updateViewerWidthUi();
    viewerWidthOptions.forEach(option => {
        option.addEventListener('click', () => {
            if (option.disabled) {
                return;
            }

            const nextWidth = normalizeViewerWidth(option.dataset.viewerWidth);

            if (nextWidth === viewerWidth) {
                return;
            }

            viewerWidth = nextWidth;
            localStorage.setItem(localStorageKeys.viewerWidthKey, viewerWidth);
            applyViewerWidthState();
            renderArticleViewer();
        });
    });
} else {
    localStorage.setItem(localStorageKeys.viewerWidthKey, viewerWidth);
}
dom.elements.articlesList.addEventListener('click', async event => {
    const readButton = event.target.closest('.btn-read[data-article-id]');
    if (readButton && dom.elements.articlesList.contains(readButton)) {
        event.preventDefault();
        event.stopPropagation();

        const articleId = Number(readButton.dataset.articleId);

        if (!Number.isInteger(articleId) || articleId <= 0) {
            return;
        }

        openArticleInViewer(articleId);
        return;
    }

    const externalButton = event.target.closest('.btn-open-external[data-article-id]');
    if (externalButton && dom.elements.articlesList.contains(externalButton)) {
        event.preventDefault();
        event.stopPropagation();

        const articleId = Number(externalButton.dataset.articleId);

        if (!Number.isInteger(articleId) || articleId <= 0) {
            return;
        }

        openArticleExternal(articleId);
        return;
    }

    const saveButton = event.target.closest('.article-save-btn[data-article-id]');
    if (saveButton && dom.elements.articlesList.contains(saveButton)) {
        event.preventDefault();
        event.stopPropagation();

        const articleId = Number(saveButton.dataset.articleId);

        if (!Number.isInteger(articleId) || articleId <= 0) {
            return;
        }

        await openListModal(articleId);
        return;
    }

    const dismissButton = event.target.closest('.article-dismiss-btn[data-article-id]');
    if (dismissButton && dom.elements.articlesList.contains(dismissButton)) {
        event.preventDefault();
        event.stopPropagation();
        await dismissFeedArticle(Number(dismissButton.dataset.articleId));
        return;
    }

    const sourceButton = event.target.closest('.meta-source.is-clickable');
    if (sourceButton && dom.elements.articlesList.contains(sourceButton)) {
        event.preventDefault();
        event.stopPropagation();

        const feedId = sourceButton.dataset.feedId || '';
        const sourceName = sourceButton.dataset.sourceName || '';

        await openDashboardWithSourceFilter({ feedId, sourceName });
        return;
    }

    const topicButton = event.target.closest('.article-topic-chip[data-topic-slug]');
    if (topicButton && dom.elements.articlesList.contains(topicButton)) {
        event.preventDefault();
        event.stopPropagation();

        const topicSlug = String(topicButton.dataset.topicSlug || '')
            .trim()
            .toLowerCase();

        if (!topicSlug) {
            return;
        }

        await openDashboardWithTopicFilter(topicSlug);
        return;
    }

    const card = event.target.closest('.feed-card[data-article-id]');
    if (card && dom.elements.articlesList.contains(card)) {
        setFocusedFeedArticle(Number(card.dataset.articleId));
    }
});
dom.elements.articlesList.addEventListener('focusin', event => {
    const card = event.target.closest('.feed-card[data-article-id]');
    if (card) {
        setFocusedFeedArticle(Number(card.dataset.articleId));
    }
});

dom.digest.list.addEventListener('click', async event => {
    const sourceChip = event.target.closest('.digest-source-chip.is-clickable');
    if (sourceChip && dom.digest.list.contains(sourceChip)) {
        event.preventDefault();
        event.stopPropagation();

        const feedId = sourceChip.dataset.feedId || '';
        const sourceName = sourceChip.dataset.sourceName || '';

        await openDashboardWithSourceFilter({ feedId, sourceName });
        return;
    }

    const openButton = event.target.closest('.digest-item-open-btn[data-item-url]');
    if (openButton && dom.digest.list.contains(openButton)) {
        event.preventDefault();
        event.stopPropagation();

        const articleId = Number(openButton.dataset.articleId);
        if (Number.isInteger(articleId) && articleId > 0) {
            openArticleExternal(articleId);
        } else {
            window.open(openButton.dataset.itemUrl, '_blank', 'noopener,noreferrer');
        }
        return;
    }

    const topicChip = event.target.closest('.digest-topic-chip.is-clickable[data-topic-slug]');
    if (topicChip && dom.digest.list.contains(topicChip)) {
        event.preventDefault();
        event.stopPropagation();

        const topicSlug = String(topicChip.dataset.topicSlug || '')
            .trim()
            .toLowerCase();

        if (!topicSlug) {
            return;
        }

        await openDashboardWithTopicFilter(topicSlug, { switchToMain: true });
        return;
    }

    const clusterAddButton = event.target.closest('.digest-cluster-add-btn[data-article-ids]');
    if (clusterAddButton && dom.digest.list.contains(clusterAddButton)) {
        event.preventDefault();
        event.stopPropagation();

        const articleIds = parseArticleIdsFromDataset(clusterAddButton.dataset.articleIds);
        if (articleIds.length === 0) {
            return;
        }

        await openListModal(articleIds);
        return;
    }

    const clusterDigestButton = event.target.closest('.digest-cluster-digest-btn[data-article-ids]');
    if (clusterDigestButton && dom.digest.list.contains(clusterDigestButton)) {
        event.preventDefault();
        event.stopPropagation();

        const articleIds = parseArticleIdsFromDataset(clusterDigestButton.dataset.articleIds);
        if (articleIds.length === 0) {
            return;
        }

        const clusterCard = clusterDigestButton.closest('.digest-cluster');
        const ok = await markDigestArticlesByIds(articleIds, clusterDigestButton, 'Mark as digested', {
            refresh: false,
            skipNextDigestEvent: true,
            toastMessage: 'Story marked as digested',
        });
        if (!ok) {
            return;
        }

        const removed = removeClusterFromDigestPayloadByArticleIds(articleIds);
        if (removed && clusterCard?.isConnected) {
            clusterCard.remove();
            applyDigestLocalMutationUi();
        }
        return;
    }

    const itemCard = event.target.closest('.digest-item-card-link[data-item-url]');
    if (itemCard && dom.digest.list.contains(itemCard)) {
        if (event.target.closest('button, a, input, select, textarea')) {
            return;
        }

        const url = String(itemCard.dataset.itemUrl || '');

        if (!url) {
            return;
        }

        openDigestItemCardInViewer(itemCard);
    }
});
dom.digest.list.addEventListener('keydown', event => {
    if (event.key !== 'Enter' && event.key !== ' ') {
        return;
    }

    const itemCard = event.target.closest('.digest-item-card-link[data-item-url]');
    if (!itemCard || !dom.digest.list.contains(itemCard)) {
        return;
    }

    event.preventDefault();

    const url = String(itemCard.dataset.itemUrl || '');
    if (!url) {
        return;
    }

    openDigestItemCardInViewer(itemCard);
});
dom.elements.layoutOptions.forEach(option => {
    option.addEventListener('click', () => {
        isListLayout = option.dataset.layout === 'list';
        localStorage.setItem(localStorageKeys.layoutKey, isListLayout ? 'list' : 'cards');
        applyLayoutState();
    });
});
if (dom.elements.themeToggleBtn) {
    dom.elements.themeToggleBtn.addEventListener('click', () => {
        isDarkTheme = !isDarkTheme;
        localStorage.setItem(localStorageKeys.themeKey, isDarkTheme ? 'dark' : 'light');
        applyThemeState();
    });
}
dom.elements.runFetchBtn.addEventListener('click', async () => {
    await clearDashboardFilters();
});
if (dom.elements.activeFilterChips) {
    dom.elements.activeFilterChips.addEventListener('click', async event => {
        const chip = event.target.closest('.active-filter-chip[data-filter-key]');
        if (!chip) {
            return;
        }
        await clearSingleDashboardFilter(chip.dataset.filterKey);
    });
}
if (dom.elements.settingsFetchNowBtn) {
    dom.elements.settingsFetchNowBtn.addEventListener('click', async () => {
        await runManualFetch(dom.elements.settingsFetchNowBtn);
    });
}

if (dom.digest.rangeToggle && dom.digest.rangeOptions.length > 0) {
    updateDigestRangeUi();
    setDigestRangeToggleDisabled(false);
    dom.digest.rangeOptions.forEach(option => {
        option.addEventListener('click', async () => {
            if (digestRuntime.isRangeSwitchLoading) {
                return;
            }
            const nextRange = normalizeDigestRange(option.dataset.digestRange);
            if (nextRange === digestRange) {
                return;
            }
            digestRange = nextRange;
            localStorage.setItem(localStorageKeys.digestRangeKey, digestRange);
            updateDigestRangeUi();
            digestRuntime.needsRefresh = true;
            digestRuntime.isRangeSwitchLoading = true;
            setDigestRangeToggleDisabled(true);
            try {
                await loadDigest({ force: true });
            } finally {
                digestRuntime.isRangeSwitchLoading = false;
                setDigestRangeToggleDisabled(false);
            }
        });
    });
} else {
    localStorage.setItem(localStorageKeys.digestRangeKey, digestRange);
}

if (dom.digest.sortToggle && dom.digest.sortOptions.length > 0) {
    updateDigestSortUi();
    dom.digest.sortOptions.forEach(option => {
        option.addEventListener('click', () => {
            const nextDirection = option.dataset.digestSort === 'asc' ? 'asc' : 'desc';

            if (nextDirection === digestSortDirection) {
                return;
            }
            digestSortDirection = nextDirection;
            localStorage.setItem(localStorageKeys.digestSortKey, digestSortDirection);
            updateDigestSortUi();

            if (digestRuntime.lastPayload) {
                renderDigestClusters(digestRuntime.lastPayload);
                return;
            }
            loadDigest();
        });
    });
} else {
    localStorage.setItem(localStorageKeys.digestSortKey, digestSortDirection);
}

if (dom.digest.markAllBtn) {
    updateDigestMarkAllButton(digestRuntime.lastPayload);
    dom.digest.markAllBtn.addEventListener('click', async () => {
        await markAllVisibleAsDigested();
    });
}
if (dom.digest.settings.saveFeeds) {
    dom.digest.settings.saveFeeds.addEventListener('click', async () => {
        await saveDigestExcludedFeeds();
    });
}
if (dom.digest.settings.blockWordAdd) {
    dom.digest.settings.blockWordAdd.addEventListener('click', async () => {
        await addDigestBlockedWord();
    });
}
if (dom.digest.settings.blockWordInput) {
    dom.digest.settings.blockWordInput.addEventListener('keydown', async event => {
        if (event.key !== 'Enter') {
            return;
        }
        event.preventDefault();
        await addDigestBlockedWord();
    });
}

dom.elements.searchInput.addEventListener('input', () => {
    renderActiveFilterChips();
    if (searchTimer) {
        clearTimeout(searchTimer);
        searchTimer = null;
    }
    if (searchLoadingTimer) {
        clearTimeout(searchLoadingTimer);
        searchLoadingTimer = null;
    }

    hide(dom.elements.loadingRow);
    searchLoadingTimer = setTimeout(() => {
        searchLoadingTimer = null;
        if (!hasRenderedArticles()) {
            show(dom.elements.loadingRow);
        }
    }, CONFIG.SEARCH_LOADING_DELAY_MS);

    searchTimer = setTimeout(() => {
        searchTimer = null;
        loadArticles({ showLoadingRow: false });
    }, CONFIG.SEARCH_DEBOUNCE_MS);
});

window.__nbsSearchSelection = value => {
    searchFromSelection(value);
};

window.addEventListener('scroll', scheduleStickySubnavScrollUpdate, { passive: true });
window.addEventListener('beforeunload', () => {
    abortInFlightRequests();
    closeSse();
});
window.addEventListener('pagehide', () => {
    abortInFlightRequests();
    closeSse();
});
window.addEventListener('keydown', event => {
    if (event.defaultPrevented) {
        return;
    }
    const target = event.target;
    const isTextEntry =
        target instanceof HTMLElement &&
        (target.matches('input, select, textarea') || target.isContentEditable);
    if (isTextEntry || event.metaKey || event.ctrlKey || event.altKey || isListModalOpen()) {
        return;
    }

    if (isViewActive('main')) {
        const key = String(event.key || '').toLowerCase();
        if (key === 'j' || key === 'k') {
            event.preventDefault();
            moveFeedArticleSelection(key === 'j' ? 1 : -1);
            return;
        }

        const cards = getFeedCards();
        const selectedId =
            articlesRuntime.focusedArticleId || Number(cards.find(card => card instanceof HTMLElement)?.dataset.articleId);
        if (Number.isInteger(selectedId) && selectedId > 0) {
            if (key === 's') {
                event.preventDefault();
                void openListModal(selectedId);
                return;
            }
            if (key === 'd') {
                event.preventDefault();
                void dismissFeedArticle(selectedId);
                return;
            }
            if (event.key === 'Enter' && !(target instanceof HTMLElement && target.matches('button, a'))) {
                event.preventDefault();
                openArticleInViewer(selectedId);
                return;
            }
        }
    }
    if (event.key !== 'Escape') {
        return;
    }
    if (!isViewActive('main') && !isViewActive('digest')) {
        return;
    }
    if (viewerOpen) {
        event.preventDefault();
        hideArticleViewer();
        return;
    }
    if (!isViewActive('main')) {
        return;
    }
    if (!hasDashboardFiltersApplied()) {
        return;
    }

    event.preventDefault();
    void clearDashboardFilters();
});

boot();
