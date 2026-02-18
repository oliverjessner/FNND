const state = Object.seal({
    feeds: [],
    lists: [],
    editingId: null,
});
const views = document.querySelectorAll('.view');
const navLinks = document.querySelectorAll('.nav-link');
const articlesState = document.getElementById('articles-state');
const articlesList = document.getElementById('articles-list');
const articlesScroll = document.querySelector('.articles-scroll');
const digestState = document.getElementById('digest-state');
const digestList = document.getElementById('digest-list');
const digestTitle = document.getElementById('digest-title');
const digestSubtitle = document.getElementById('digest-subtitle');
const digestMarkAllBtn = document.getElementById('digest-mark-all');
const digestSortToggle = document.getElementById('digest-sort-toggle');
const digestSortOptions = document.querySelectorAll('.digest-sort-option');
const digestTemplate = document.getElementById('digest-cluster-template');
const digestHeader = document.querySelector('.digest-header');
const feedsState = document.getElementById('feeds-state');
const feedsList = document.getElementById('feeds-list');
const filterList = document.getElementById('filter-list');
const filterSource = document.getElementById('filter-source');
const runFetchBtn = document.getElementById('run-fetch');
const toggleLayoutBtn = document.getElementById('toggle-layout');
const fetchStatus = document.getElementById('fetch-status');
const articleCountStatus = document.getElementById('article-count-status');
const searchInput = document.getElementById('search-input');
const loadingRow = document.getElementById('loading-row');
const feedForm = document.getElementById('feed-form');
const feedName = document.getElementById('feed-name');
const feedWebsite = document.getElementById('feed-website');
const feedUrl = document.getElementById('feed-url');
const feedSubmit = document.getElementById('feed-submit');
const feedCancel = document.getElementById('feed-cancel');
const feedTest = document.getElementById('feed-test');
const feedFormStatus = document.getElementById('feed-form-status');
const listForm = document.getElementById('list-form');
const listName = document.getElementById('list-name');
const listDescription = document.getElementById('list-description');
const listColor = document.getElementById('list-color');
const listSubmit = document.getElementById('list-submit');
const listCancel = document.getElementById('list-cancel');
const listFormStatus = document.getElementById('list-form-status');
const listsState = document.getElementById('lists-state');
const listsList = document.getElementById('lists-list');
const settingsTabs = document.querySelectorAll('.settings-tab');
const settingsPanels = document.querySelectorAll('.settings-panel');
const settingsTabsWrap = document.querySelector('.settings-tabs-wrap');
const modalBackdrop = document.getElementById('modal-backdrop');
const modalListSelect = document.getElementById('modal-list-select');
const modalClose = document.getElementById('modal-close');
const modalCancel = document.getElementById('modal-cancel');
const modalConfirm = document.getElementById('modal-confirm');
const modalExistingLists = document.getElementById('modal-existing-lists');
const LAYOUT_KEY = 'fnnd.layout';
const DIGEST_SORT_KEY = 'fnnd.digestSort';

let loadingStartedAt = 0;
let isListLayout = localStorage.getItem(LAYOUT_KEY) === 'list';
let searchTimer = null;
let listEditingId = null;
let pendingArticleId = null;
let sse = null;
let digestSortDirection = localStorage.getItem(DIGEST_SORT_KEY) === 'asc' ? 'asc' : 'desc';
let lastDigestPayload = null;
let digestNeedsRefresh = true;
let digestLoadPromise = null;
let lastDigestRenderFingerprint = '';
let articlesNeedsRefresh = true;
let pendingDigestMutationEventsToSkip = 0;

function updateDigestSortUi() {
    if (!digestSortOptions || digestSortOptions.length === 0) {
        return;
    }
    digestSortOptions.forEach(option => {
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
        const leftCount = Number(left?.clusterCount || 0);
        const rightCount = Number(right?.clusterCount || 0);
        const countDiff = digestSortDirection === 'asc' ? leftCount - rightCount : rightCount - leftCount;
        if (countDiff !== 0) {
            return countDiff;
        }

        const leftTime = getDigestClusterSortTime(left);
        const rightTime = getDigestClusterSortTime(right);
        return rightTime - leftTime;
    });
}

function getDigestArticleIds(payload) {
    if (!payload || !Array.isArray(payload.clusters)) {
        return [];
    }

    const ids = new Set();
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

function updateDigestMarkAllButton(payload = lastDigestPayload) {
    if (!digestMarkAllBtn) {
        return;
    }

    const articleIds = getDigestArticleIds(payload);
    const total = articleIds.length;
    digestMarkAllBtn.disabled = total === 0;
    digestMarkAllBtn.textContent = total > 0 ? `Mark all as digested (${total})` : 'Mark all as digested';
}

function getNormalizedArticleIds(value) {
    if (!Array.isArray(value)) {
        return [];
    }
    const ids = new Set();
    value.forEach(id => {
        const normalized = Number(id);
        if (Number.isInteger(normalized) && normalized > 0) {
            ids.add(normalized);
        }
    });
    return Array.from(ids);
}

function getDigestClusterArticleIds(cluster) {
    if (!cluster || !Array.isArray(cluster.items)) {
        return [];
    }
    return getNormalizedArticleIds(cluster.items.map(item => item?.id));
}

function removeClusterFromDigestPayloadByArticleIds(articleIds) {
    const normalizedIds = getNormalizedArticleIds(articleIds);
    if (!lastDigestPayload || !Array.isArray(lastDigestPayload.clusters) || normalizedIds.length === 0) {
        return false;
    }

    const ids = new Set(normalizedIds);
    const nextClusters = lastDigestPayload.clusters.filter(cluster => {
        const items = Array.isArray(cluster?.items) ? cluster.items : [];
        return !items.some(item => ids.has(Number(item?.id)));
    });

    if (nextClusters.length === lastDigestPayload.clusters.length) {
        return false;
    }

    const totalArticles = nextClusters.reduce((sum, cluster) => {
        const items = Array.isArray(cluster?.items) ? cluster.items : [];
        return sum + items.length;
    }, 0);

    lastDigestPayload = {
        ...lastDigestPayload,
        clusters: nextClusters,
        totalClusters: nextClusters.length,
        totalArticles,
    };
    lastDigestRenderFingerprint = getDigestPayloadFingerprint(lastDigestPayload);
    digestNeedsRefresh = false;

    return true;
}

function applyDigestLocalMutationUi() {
    if (!digestList || !digestState) {
        return;
    }
    const clusterElements = digestList.querySelectorAll('.digest-cluster');
    if (clusterElements.length === 0) {
        digestState.textContent = 'Für heute sind noch keine Artikel gespeichert.';
        digestState.style.display = 'block';
    } else {
        digestState.style.display = 'none';
    }
    renderDigestSubtitle(lastDigestPayload);
    updateDigestMarkAllButton(lastDigestPayload);
}

async function markDigestArticlesByIds(articleIds, triggerBtn, triggerLabel = 'Digest topic', options = {}) {
    const ids = getNormalizedArticleIds(articleIds);
    if (ids.length === 0) {
        return false;
    }
    const { refresh = true, skipNextDigestEvent = false } = options;

    const previousLabel = triggerBtn ? triggerBtn.textContent : '';
    if (triggerBtn) {
        triggerBtn.disabled = true;
        triggerBtn.textContent = 'Digesting…';
    }
    if (skipNextDigestEvent) {
        pendingDigestMutationEventsToSkip += 1;
    }

    try {
        await apiFetch('/api/articles/daily-digest/mark-all-digested', {
            method: 'POST',
            body: JSON.stringify({ articleIds: ids }),
        });
        if (refresh) {
            digestNeedsRefresh = true;
            await loadDailyDigest({ force: true });
        }
        return true;
    } catch (err) {
        if (skipNextDigestEvent && pendingDigestMutationEventsToSkip > 0) {
            pendingDigestMutationEventsToSkip -= 1;
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
    if (!digestMarkAllBtn) {
        return;
    }
    const articleIds = getDigestArticleIds(lastDigestPayload);
    await markDigestArticlesByIds(articleIds, digestMarkAllBtn, 'Mark all as digested');
    updateDigestMarkAllButton(lastDigestPayload);
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
    if (name === 'main' && articlesNeedsRefresh) {
        loadArticles();
    }
    if (name === 'digest') {
        loadDailyDigest();
    }
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

function applyLayoutState() {
    articlesList.classList.toggle('is-list', isListLayout);
    toggleLayoutBtn.classList.toggle('is-on', isListLayout);
    toggleLayoutBtn.dataset.layout = isListLayout ? 'list' : 'cards';
    toggleLayoutBtn.setAttribute('aria-pressed', String(isListLayout));
    const label = toggleLayoutBtn.querySelector('.toggle-label');
    if (label) {
        label.textContent = isListLayout ? 'Liste' : 'Cards';
    }
}

function getPageScrollTop() {
    return window.scrollY || document.documentElement.scrollTop || 0;
}

function updateStickySubnavScrollState() {
    const isScrolled = getPageScrollTop() > 2;

    if (settingsTabsWrap) {
        const settingsView = document.getElementById('view-settings');
        const isSettingsVisible = settingsView?.classList.contains('is-active');
        settingsTabsWrap.classList.toggle('is-scrolled', Boolean(isSettingsVisible && isScrolled));
    }

    if (digestHeader) {
        const digestView = document.getElementById('view-digest');
        const isDigestVisible = digestView?.classList.contains('is-active');
        digestHeader.classList.toggle('is-scrolled', Boolean(isDigestVisible && isScrolled));
    }
}

function formatDate(value) {
    if (!value) return '—';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '—';
    return date.toLocaleString('de-DE', { dateStyle: 'medium', timeStyle: 'short' });
}

async function loadFetchStatus() {
    try {
        const status = await apiFetch('/api/fetch/status');
        if (!status || !status.at) {
            fetchStatus.textContent = 'Letzter Fetch: —';
        } else {
            const date = formatDate(status.at);
            const suffix = status.error ? ` (Fehler: ${status.error})` : ` (${status.totalNew} neu)`;
            fetchStatus.textContent = `Letzter Fetch: ${date}${suffix}`;
        }
    } catch {
        fetchStatus.textContent = 'Letzter Fetch: —';
    }

    if (!articleCountStatus) {
        return;
    }

    try {
        const stats = await apiFetch('/api/articles/stats');
        const total = Number(stats?.total || 0);
        articleCountStatus.textContent = `Gespeicherte Artikel: ${total.toLocaleString('de-DE')}`;
    } catch {
        articleCountStatus.textContent = 'Gespeicherte Artikel: —';
    }
}

function setStatus(element, message) {
    element.textContent = message || '';
}

async function apiFetch(url, options = {}) {
    const res = await fetch(url, {
        headers: { 'Content-Type': 'application/json' },
        ...options,
    });
    if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        const error = body.error || 'Server error';
        throw new Error(error);
    }
    if (res.status === 204) return null;
    return res.json();
}

function renderFeeds() {
    feedsList.innerHTML = '';

    if (state.feeds.length === 0) {
        feedsState.textContent = 'Noch keine Feeds vorhanden.';
        feedsState.style.display = 'block';
        return;
    }

    feedsState.style.display = 'none';

    const template = document.getElementById('feed-item-template');

    state.feeds.forEach(feed => {
        const node = template.content.cloneNode(true);
        const logoEl = node.querySelector('.feed-logo');
        const nameEl = node.querySelector('.feed-name');
        if (feed.logoDataUrl) {
            logoEl.src = feed.logoDataUrl;
            logoEl.style.display = 'inline-block';
        } else {
            logoEl.style.display = 'none';
        }
        nameEl.textContent = feed.name;
        node.querySelector('.list-meta').textContent = `${feed.websiteUrl} · ${feed.feedUrl}`;

        node.querySelector('.btn-edit').addEventListener('click', () => {
            state.editingId = feed.id;
            feedName.value = feed.name;
            feedWebsite.value = feed.websiteUrl;
            feedUrl.value = feed.feedUrl;
            feedSubmit.textContent = 'Änderungen speichern';
            setStatus(feedFormStatus, 'Bearbeitungsmodus aktiv.');
        });

        node.querySelector('.btn-delete').addEventListener('click', async () => {
            if (!confirm(`Feed "${feed.name}" löschen?`)) return;
            try {
                await apiFetch(`/api/feeds/${feed.id}`, { method: 'DELETE' });
                await loadFeeds();
                await loadArticles();
            } catch (err) {
                alert(err.message);
            }
        });

        feedsList.appendChild(node);
    });
}

function renderLists() {
    listsList.innerHTML = '';

    if (state.lists.length === 0) {
        listsState.textContent = 'Noch keine Listen vorhanden.';
        listsState.style.display = 'block';
        return;
    }

    listsState.style.display = 'none';
    const template = document.getElementById('list-item-template');

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
            listName.value = list.name;
            listDescription.value = list.description || '';
            listColor.value = list.color || '#1d1d1f';
            listSubmit.textContent = 'Änderungen speichern';
            setStatus(listFormStatus, 'Bearbeitungsmodus aktiv.');
        });

        node.querySelector('.btn-delete').addEventListener('click', async () => {
            if (!confirm(`Liste "${list.name}" löschen?`)) {
                return;
            }
            try {
                await apiFetch(`/api/lists/${list.id}`, { method: 'DELETE' });
                await loadLists();
            } catch (err) {
                alert(err.message);
            }
        });

        listsList.appendChild(node);
    });
}

async function openListModal(articleId) {
    pendingArticleId = articleId;
    modalListSelect.innerHTML = '';
    const placeholder = document.createElement('option');
    placeholder.value = '';
    placeholder.textContent = 'Liste auswählen';
    modalListSelect.appendChild(placeholder);

    let existingIds = new Set();
    let existingLists = [];
    try {
        existingLists = await apiFetch(`/api/articles/${articleId}/lists`);
        existingIds = new Set(existingLists.map(item => String(item.id)));
    } catch (err) {
        existingIds = new Set();
        existingLists = [];
    }

    state.lists.forEach(list => {
        const option = document.createElement('option');
        option.value = list.id;
        option.textContent = existingIds.has(String(list.id)) ? `${list.name} (bereits)` : list.name;
        option.disabled = existingIds.has(String(list.id));
        modalListSelect.appendChild(option);
    });

    if (modalExistingLists) {
        modalExistingLists.innerHTML = '';
        if (existingLists.length === 0) {
            modalExistingLists.textContent = '—';
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
                modalExistingLists.appendChild(chip);
            });
        }
    }

    modalBackdrop.classList.add('is-open');
    modalBackdrop.setAttribute('aria-hidden', 'false');
}

function closeListModal() {
    pendingArticleId = null;
    modalBackdrop.classList.remove('is-open');
    modalBackdrop.setAttribute('aria-hidden', 'true');
}

function resetListForm() {
    listEditingId = null;
    listName.value = '';
    listDescription.value = '';
    listColor.value = '#1d1d1f';
    listSubmit.textContent = 'Liste speichern';
    setStatus(listFormStatus, '');
}

async function loadLists() {
    listsState.style.display = 'block';
    listsState.textContent = 'Lädt…';
    try {
        state.lists = await apiFetch('/api/lists');
        renderLists();
        renderListFilterOptions();
    } catch (err) {
        listsState.textContent = `Fehler: ${err.message}`;
    }
}

function renderFilterOptions() {
    const selected = filterSource.value;
    filterSource.innerHTML = '<option value="">all sources</option>';
    state.feeds.forEach(feed => {
        const option = document.createElement('option');
        option.value = feed.id;
        option.textContent = feed.name;
        filterSource.appendChild(option);
    });
    filterSource.value = selected;
}

function renderListFilterOptions() {
    const selected = filterList.value;
    filterList.innerHTML = '<option value="">all lists</option>';
    state.lists.forEach(list => {
        const option = document.createElement('option');
        option.value = list.id;
        option.textContent = list.name;
        filterList.appendChild(option);
    });
    filterList.value = selected;
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

async function openDashboardWithSourceFilter({ feedId, sourceName } = {}) {
    const numericFeedId = Number(feedId);
    let resolvedFeedId = Number.isInteger(numericFeedId) && numericFeedId > 0 ? String(numericFeedId) : '';

    if (!resolvedFeedId) {
        resolvedFeedId = findFeedIdBySourceName(sourceName);
    }

    if (!resolvedFeedId) {
        return;
    }

    let optionExists = Array.from(filterSource.options).some(option => option.value === resolvedFeedId);
    if (!optionExists) {
        await loadFeeds();
        optionExists = Array.from(filterSource.options).some(option => option.value === resolvedFeedId);
        if (!optionExists) {
            return;
        }
    }

    filterSource.value = resolvedFeedId;
    setView('main');
    await loadArticles();
}

async function loadFeeds() {
    feedsState.style.display = 'block';
    feedsState.textContent = 'Lädt…';
    try {
        state.feeds = await apiFetch('/api/feeds');
        renderFeeds();
        renderFilterOptions();
    } catch (err) {
        feedsState.textContent = `Fehler: ${err.message}`;
    }
}

function renderArticles(articles) {
    articlesList.innerHTML = '';

    if (articles.length === 0) {
        articlesState.textContent = 'Nothing found, try other search input or delete all';
        articlesState.style.display = 'block';
        return;
    }

    articlesState.style.display = 'none';
    const template = document.getElementById('article-card-template');

    articles.forEach(article => {
        const node = template.content.cloneNode(true);

        node.querySelector('.meta-date').textContent = formatDate(article.publishedAt);

        const sourceLogo = node.querySelector('.source-logo');
        const sourceName = node.querySelector('.source-name');

        if (article.sourceLogoDataUrl) {
            sourceLogo.src = article.sourceLogoDataUrl;
            sourceLogo.style.display = 'inline-block';
        } else {
            sourceLogo.style.display = 'none';
        }

        sourceName.textContent = article.sourceName || '—';

        node.querySelector('.title').textContent = article.title || 'Ohne Titel';
        node.querySelector('.teaser').textContent = article.teaser || '';

        const link = node.querySelector('.link');
        const addBtn = node.querySelector('.btn-add');

        if (article.url) {
            link.href = article.url;
        } else {
            link.remove();
        }

        if (addBtn) {
            addBtn.addEventListener('click', async () => {
                await openListModal(article.id);
            });
        }

        return articlesList.appendChild(node);
    });
}

function renderDigestClusters(payload) {
    if (!digestList || !digestState || !digestTemplate) {
        return;
    }

    const clusters = sortDigestClusters(Array.isArray(payload?.clusters) ? payload.clusters : []);
    digestList.innerHTML = '';

    if (clusters.length === 0) {
        digestState.textContent = 'Für heute sind noch keine Artikel gespeichert.';
        digestState.style.display = 'block';
        return;
    }

    digestState.style.display = 'none';

    clusters.forEach(cluster => {
        const representative = cluster.representative || cluster.items?.[0] || {};
        const items = Array.isArray(cluster.items) ? cluster.items : [];
        const node = digestTemplate.content.cloneNode(true);
        const countEl = node.querySelector('.digest-cluster-count');
        const dateEl = node.querySelector('.digest-cluster-date');
        const titleEl = node.querySelector('.digest-cluster-title');
        const sourcesEl = node.querySelector('.digest-cluster-sources');
        const itemsGridEl = node.querySelector('.digest-items-grid');

        if (countEl) {
            countEl.textContent =
                cluster.clusterCount > 1 ? `${cluster.clusterCount} ähnliche Artikel` : '1 Artikel';
        }
        if (dateEl) {
            dateEl.textContent = formatDate(representative.publishedAt);
        }
        if (titleEl) {
            titleEl.textContent = cluster.clusterTitle || representative.title || 'Ohne Titel';
        }
        if (sourcesEl) {
            sourcesEl.innerHTML = '';
            const sourcesMap = new Map();
            items.forEach(item => {
                const key = String(item.sourceName || 'Unbekannte Quelle');
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
                empty.textContent = 'Unbekannte Quelle';
                sourcesEl.appendChild(empty);
            } else {
                sources.forEach(source => {
                    const chip = document.createElement('button');
                    chip.type = 'button';
                    chip.className = 'digest-source-chip';
                    if (source.feedId) {
                        chip.classList.add('is-clickable');
                        chip.title = `Filter by ${source.name}`;
                        chip.addEventListener('click', async event => {
                            event.preventDefault();
                            event.stopPropagation();
                            await openDashboardWithSourceFilter({
                                feedId: source.feedId,
                                sourceName: source.name,
                            });
                        });
                    } else {
                        chip.disabled = true;
                    }

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
            itemsGridEl.innerHTML = '';
            itemsGridEl.classList.toggle('is-single-item', items.length === 1);
            items.forEach(item => {
                const card = document.createElement('article');
                const hasUrl = Boolean(item.url);
                card.className = hasUrl ? 'digest-item-card digest-item-card-link' : 'digest-item-card';

                if (hasUrl) {
                    const openArticle = () => {
                        window.open(item.url, '_blank', 'noopener,noreferrer');
                    };
                    card.setAttribute('role', 'link');
                    card.tabIndex = 0;
                    card.addEventListener('click', event => {
                        if (event.target.closest('.digest-item-actions')) {
                            return;
                        }
                        openArticle();
                    });
                    card.addEventListener('keydown', event => {
                        if ((event.key === 'Enter' || event.key === ' ') && !event.target.closest('.digest-item-actions')) {
                            event.preventDefault();
                            openArticle();
                        }
                    });
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
                published.textContent = formatDate(item.publishedAt);

                meta.appendChild(sourceWrap);
                meta.appendChild(published);

                const itemTitle = document.createElement('h4');
                itemTitle.className = 'digest-item-title';
                itemTitle.textContent = item.title || 'Ohne Titel';

                card.appendChild(meta);
                card.appendChild(itemTitle);
                if (item.teaser) {
                    const itemTeaser = document.createElement('p');
                    itemTeaser.className = 'digest-item-teaser';
                    itemTeaser.textContent = item.teaser;
                    card.appendChild(itemTeaser);
                }

                const actions = document.createElement('div');
                actions.className = 'digest-item-actions';

                if (hasUrl) {
                    const readLink = document.createElement('a');
                    readLink.className = 'digest-item-read-link digest-item-action';
                    readLink.href = item.url;
                    readLink.target = '_blank';
                    readLink.rel = 'noopener noreferrer';
                    readLink.textContent = 'read article';
                    actions.appendChild(readLink);
                }

                if (actions.childElementCount > 0) {
                    card.appendChild(actions);
                }
                itemsGridEl.appendChild(card);
            });

            const clusterCard = node.querySelector('.digest-cluster');
            if (clusterCard) {
                const clusterActions = document.createElement('div');
                clusterActions.className = 'digest-cluster-actions';

                const clusterDigestBtn = document.createElement('button');
                clusterDigestBtn.type = 'button';
                clusterDigestBtn.className = 'btn ghost digest-cluster-digest-btn';
                clusterDigestBtn.textContent =
                    clusterArticleIds.length > 1 ? `Digest topic (${clusterArticleIds.length})` : 'Digest topic';
                clusterDigestBtn.disabled = clusterArticleIds.length === 0;

                if (!clusterDigestBtn.disabled) {
                    clusterDigestBtn.addEventListener('click', async event => {
                        event.preventDefault();
                        event.stopPropagation();
                        const ok = await markDigestArticlesByIds(clusterArticleIds, clusterDigestBtn, 'Digest topic', {
                            refresh: false,
                            skipNextDigestEvent: true,
                        });
                        if (!ok) {
                            return;
                        }
                        const removed = removeClusterFromDigestPayloadByArticleIds(clusterArticleIds);
                        if (removed && clusterCard.isConnected) {
                            clusterCard.remove();
                            applyDigestLocalMutationUi();
                        }
                    });
                }

                clusterActions.appendChild(clusterDigestBtn);
                clusterCard.appendChild(clusterActions);
            }
        }

        digestList.appendChild(node);
    });
}

function renderDigestSubtitle(payload) {
    if (!digestSubtitle && !digestTitle) {
        return;
    }

    const totalArticles = Number(payload?.totalArticles || 0);
    const totalClusters = Number(payload?.totalClusters || 0);
    const startDate = payload?.startIso ? new Date(payload.startIso) : new Date();
    const dayLabel = Number.isNaN(startDate.getTime())
        ? new Date().toLocaleDateString('de-DE', { dateStyle: 'full' })
        : startDate.toLocaleDateString('de-DE', { dateStyle: 'full' });

    if (digestTitle) {
        digestTitle.textContent = dayLabel;
    }
    if (digestSubtitle) {
        digestSubtitle.textContent = `${totalArticles.toLocaleString('de-DE')} Artikel · ${totalClusters.toLocaleString('de-DE')} Cluster`;
    }
}

function isViewActive(name) {
    const view = document.getElementById(`view-${name}`);
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
        payload.startIso || '',
        payload.endIso || '',
        Number(payload.totalArticles || 0),
        Number(payload.totalClusters || 0),
        clusterFingerprint,
    ].join('::');
}

function requestDigestRefresh({ force = false } = {}) {
    digestNeedsRefresh = true;
    if (isViewActive('digest')) {
        void loadDailyDigest({ force, silent: true });
    }
}

async function loadDailyDigest({ force = false, silent = false } = {}) {
    if (!digestState || !digestList) {
        return;
    }

    if (!force && !digestNeedsRefresh && lastDigestPayload) {
        return;
    }

    if (digestLoadPromise) {
        return digestLoadPromise;
    }

    const showLoadingState = !silent || !lastDigestPayload;
    if (showLoadingState) {
        digestList.innerHTML = '';
        digestState.textContent = 'Lädt…';
        digestState.style.display = 'block';
        updateDigestMarkAllButton({ clusters: [] });
    }

    digestLoadPromise = (async () => {
        try {
            const payload = await apiFetch('/api/articles/daily-digest');
            const nextFingerprint = getDigestPayloadFingerprint(payload);
            const hasDigestChanged = nextFingerprint !== lastDigestRenderFingerprint;

            lastDigestPayload = payload;
            digestNeedsRefresh = false;
            renderDigestSubtitle(payload);

            if (hasDigestChanged || showLoadingState || force) {
                renderDigestClusters(payload);
                lastDigestRenderFingerprint = nextFingerprint;
            }

            updateDigestMarkAllButton(payload);
        } catch (err) {
            if (!lastDigestPayload) {
                lastDigestPayload = null;
                lastDigestRenderFingerprint = '';
                digestState.textContent = `Fehler: ${err.message}`;
                digestState.style.display = 'block';
                updateDigestMarkAllButton({ clusters: [] });
            }
            if (digestSubtitle) {
                digestSubtitle.textContent = 'Daily Digest konnte nicht geladen werden.';
            }
        } finally {
            digestLoadPromise = null;
        }
    })();

    return digestLoadPromise;
}

async function loadArticles() {
    const params = new URLSearchParams();
    const selectedList = filterList.value;
    const selected = filterSource.value;

    articlesState.style.display = 'none';
    articlesState.textContent = '';
    loadingRow.style.display = 'flex';

    loadingStartedAt = Date.now();

    if (selected) {
        params.set('feedId', selected);
    }
    if (selectedList) {
        params.set('listId', selectedList);
    }

    const query = searchInput.value.trim();

    if (query) {
        params.set('query', query);
    }

    try {
        const articles = await apiFetch(`/api/articles?${params.toString()}`);

        loadingRow.style.display = 'none';
        renderArticles(articles);
        articlesNeedsRefresh = false;
    } catch (err) {
        articlesState.textContent = `Fehler: ${err.message}`;
        articlesState.style.display = 'block';
        articlesNeedsRefresh = true;
    }
}

function normalizeSearchQuery(value) {
    return String(value || '')
        .trim()
        .replace(/\s+/g, ' ')
        .slice(0, 300);
}

function scrollArticlesToTop() {
    if (articlesScroll) {
        articlesScroll.scrollTop = 0;
    }
    window.scrollTo(0, 0);
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

    searchInput.value = query;
    setView('main');
    await loadArticles();
}

function resetForm() {
    state.editingId = null;
    feedName.value = '';
    feedWebsite.value = '';
    feedUrl.value = '';
    feedSubmit.textContent = 'Feed speichern';
    setStatus(feedFormStatus, '');
}

feedForm.addEventListener('submit', async event => {
    event.preventDefault();
    feedSubmit.disabled = true;
    setStatus(feedFormStatus, 'Speichern…');

    const payload = {
        name: feedName.value.trim(),
        websiteUrl: feedWebsite.value.trim(),
        feedUrl: feedUrl.value.trim(),
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
        setStatus(feedFormStatus, `Fehler: ${err.message}`);
    } finally {
        feedSubmit.disabled = false;
    }
});

feedCancel.addEventListener('click', () => resetForm());

listForm.addEventListener('submit', async event => {
    event.preventDefault();
    listSubmit.disabled = true;
    setStatus(listFormStatus, 'Speichern…');

    const colorValue = listColor && listColor.value ? listColor.value.trim() : '#1d1d1f';
    const normalizedColor = colorValue.startsWith('#') ? colorValue : `#${colorValue}`;

    const payload = {
        name: listName.value.trim(),
        description: listDescription.value.trim(),
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
        setStatus(listFormStatus, `Fehler: ${err.message}`);
    } finally {
        listSubmit.disabled = false;
    }
});

listCancel.addEventListener('click', () => resetListForm());

modalClose.addEventListener('click', () => closeListModal());
modalCancel.addEventListener('click', () => closeListModal());
modalBackdrop.addEventListener('click', event => {
    if (event.target === modalBackdrop) {
        closeListModal();
    }
});
modalConfirm.addEventListener('click', async () => {
    const listId = modalListSelect.value;
    if (!listId || !pendingArticleId) {
        alert('Bitte eine Liste auswählen.');
        return;
    }
    try {
        await apiFetch(`/api/lists/${listId}/items`, {
            method: 'POST',
            body: JSON.stringify({ articleId: pendingArticleId }),
        });
        closeListModal();
    } catch (err) {
        alert(err.message);
    }
});

feedTest.addEventListener('click', async () => {
    const url = feedUrl.value.trim();
    if (!url) {
        setStatus(feedFormStatus, 'Bitte eine Feed-URL eingeben.');
        return;
    }

    feedTest.disabled = true;
    setStatus(feedFormStatus, 'Teste Feed…');
    try {
        const result = await apiFetch(`/api/feeds/test/url?url=${encodeURIComponent(url)}`);
        const titles = result.sampleTitles?.length ? `Beispiele: ${result.sampleTitles.join(' · ')}` : '';
        setStatus(feedFormStatus, `OK: ${result.itemCount} Items. ${titles}`);
    } catch (err) {
        setStatus(feedFormStatus, `Fehler: ${err.message}`);
    } finally {
        feedTest.disabled = false;
    }
});

filterSource.addEventListener('change', () => loadArticles());
filterList.addEventListener('change', () => loadArticles());
toggleLayoutBtn.addEventListener('click', () => {
    isListLayout = !isListLayout;
    localStorage.setItem(LAYOUT_KEY, isListLayout ? 'list' : 'cards');
    applyLayoutState();
});
runFetchBtn.addEventListener('click', async () => {
    runFetchBtn.disabled = true;
    runFetchBtn.textContent = 'fetching…';

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
        runFetchBtn.disabled = false;
        runFetchBtn.textContent = 'fetch now';
    }
});

if (digestSortToggle && digestSortOptions.length > 0) {
    updateDigestSortUi();
    digestSortOptions.forEach(option => {
        option.addEventListener('click', () => {
            const nextDirection = option.dataset.digestSort === 'asc' ? 'asc' : 'desc';
            if (nextDirection === digestSortDirection) {
                return;
            }
            digestSortDirection = nextDirection;
            localStorage.setItem(DIGEST_SORT_KEY, digestSortDirection);
            updateDigestSortUi();

            if (lastDigestPayload) {
                renderDigestClusters(lastDigestPayload);
                return;
            }
            loadDailyDigest();
        });
    });
} else {
    localStorage.setItem(DIGEST_SORT_KEY, digestSortDirection);
}

if (digestMarkAllBtn) {
    updateDigestMarkAllButton(lastDigestPayload);
    digestMarkAllBtn.addEventListener('click', async () => {
        await markAllVisibleAsDigested();
    });
}

searchInput.addEventListener('input', () => {
    if (searchTimer) {
        clearTimeout(searchTimer);
    }

    articlesList.innerHTML = '';
    articlesState.style.display = 'none';
    articlesState.textContent = '';
    loadingRow.style.display = 'flex';
    loadingStartedAt = Date.now();

    searchTimer = setTimeout(() => {
        loadArticles();
    }, 3000);
});

window.__nbsSearchSelection = value => {
    searchFromSelection(value);
};

async function boot() {
    applyLayoutState();
    updateStickySubnavScrollState();
    await loadFeeds();
    await loadLists();
    await loadArticles();
    await loadDailyDigest({ force: true });
    await loadFetchStatus();
    setupSse();
}

boot();

window.addEventListener('scroll', updateStickySubnavScrollState, { passive: true });

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
                if (source === 'daily-digest' && pendingDigestMutationEventsToSkip > 0) {
                    pendingDigestMutationEventsToSkip -= 1;
                    return;
                }
                requestDigestRefresh();
            }
            if (eventName.startsWith('webhook.')) {
                requestDigestRefresh();
            }
            if (eventName === 'feeds.updated') {
                loadFeeds();
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
        } catch {
            return;
        }
    });
}
