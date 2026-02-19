import simhashPkg from '@biu/simhash';

const { simhash, similarity } = simhashPkg;

function readNumberEnv(name, fallback) {
    const value = Number(process.env[name]);
    return Number.isFinite(value) ? value : fallback;
}

const DIGEST_SIMILARITY_THRESHOLD_STRICT = readNumberEnv('DIGEST_SIMILARITY_STRICT', 0.76);
const DIGEST_SIMILARITY_THRESHOLD_SOFT = readNumberEnv('DIGEST_SIMILARITY_SOFT', 0.64);
const DIGEST_TOKEN_OVERLAP_THRESHOLD = readNumberEnv('DIGEST_TOKEN_OVERLAP', 0.26);
const DIGEST_STRICT_MIN_TOKEN_OVERLAP = readNumberEnv('DIGEST_STRICT_MIN_OVERLAP', 0.25);
const DIGEST_STRICT_ANCHOR_MIN_MATCH = readNumberEnv('DIGEST_STRICT_ANCHOR_MIN_MATCH', 1);
const DIGEST_TITLE_ANCHOR_MIN_MATCH = readNumberEnv('DIGEST_TITLE_ANCHOR_MIN_MATCH', 2);
const DIGEST_SOFT_MATCH_MAX_HOURS = readNumberEnv('DIGEST_SOFT_MAX_HOURS', 18);
const DIGEST_TITLE_ANCHOR_STOPWORDS = new Set([
    'a',
    'an',
    'and',
    'are',
    'as',
    'at',
    'auf',
    'aus',
    'be',
    'bei',
    'das',
    'dem',
    'den',
    'der',
    'des',
    'die',
    'ein',
    'eine',
    'einem',
    'einen',
    'einer',
    'es',
    'for',
    'from',
    'für',
    'have',
    'has',
    'how',
    'ich',
    'im',
    'in',
    'into',
    'is',
    'ist',
    'it',
    'mit',
    'nach',
    'noch',
    'not',
    'of',
    'on',
    'or',
    'sind',
    'that',
    'the',
    'this',
    'today',
    'top',
    'und',
    'von',
    'vs',
    'was',
    'watch',
    'were',
    'wie',
    'with',
    'you',
    'your',
    'zu',
    'zum',
    'anzeige',
    'best',
    'deal',
    'first',
    'free',
    'latest',
    'new',
    'review',
    'save',
    'test',
    'tested',
    'amazon',
    'euro',
    'rabatt',
    'prozent',
    'deal',
    'deals',
    'angebot',
    'angebote',
    'unter',
    'weniger',
    'mehr',
    'jetzt',
    'sichern',
    'kaufen',
    'günstig',
    'news',
    'update',
    'updates',
    'breaking',
    'live',
    'video',
    'videos',
    'report',
    'reports',
    'says',
    'said',
    'will',
    'latest',
    'first',
]);

export const NORMALIZED_PUBLISHED_AT_SQL = `
    CASE
        WHEN articles.publishedAt IS NULL THEN NULL
        WHEN typeof(articles.publishedAt) IN ('integer', 'real') THEN datetime(
            CASE
                WHEN CAST(articles.publishedAt AS INTEGER) > 9999999999
                    THEN CAST(articles.publishedAt AS INTEGER) / 1000
                ELSE CAST(articles.publishedAt AS INTEGER)
            END,
            'unixepoch'
        )
        WHEN trim(articles.publishedAt) != '' AND trim(articles.publishedAt) NOT GLOB '*[^0-9]*' THEN datetime(
            CASE
                WHEN length(trim(articles.publishedAt)) > 10
                    THEN CAST(trim(articles.publishedAt) AS INTEGER) / 1000
                ELSE CAST(trim(articles.publishedAt) AS INTEGER)
            END,
            'unixepoch'
        )
        ELSE datetime(articles.publishedAt)
    END
`;

export function mapArticleRow(row) {
    const logoDataUrl =
        row.sourceLogo && row.sourceLogoMime
            ? `data:${row.sourceLogoMime};base64,${row.sourceLogo.toString('base64')}`
            : null;
    const { sourceLogo, sourceLogoMime, ...rest } = row;
    return { ...rest, sourceLogoDataUrl: logoDataUrl };
}

export function getTodayRangeIso() {
    const start = new Date();
    const end = new Date(start);

    start.setHours(0, 0, 0, 0);
    end.setDate(end.getDate() + 1);

    return { startIso: start.toISOString(), endIso: end.toISOString() };
}

function stripHtml(value) {
    return String(value || '').replace(/<[^>]*>/g, ' ');
}

function buildFingerprintText(article) {
    return [
        article.title,
        article.teaser,
        article.summary,
        article.description,
        article.contentSnippet,
        article.content_snippet,
        article.content,
    ]
        .filter(value => typeof value === 'string' && value.trim())
        .join(' ');
}

function normalizeFingerprintText(value) {
    return stripHtml(value)
        .toLowerCase()
        .normalize('NFKC')
        .replace(/[^\p{L}\p{N}\s]/gu, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

function tokenizeFingerprintText(value, { keepStopwords = false } = {}) {
    return String(value || '')
        .split(' ')
        .map(token => token.trim())
        .filter(token => token.length > 2)
        .filter(token => !/^\d+$/.test(token))
        .filter(token => keepStopwords || !DIGEST_TITLE_ANCHOR_STOPWORDS.has(token));
}

function buildSimhashTokens(tokens) {
    if (tokens.length === 0) {
        return [];
    }

    const tokenWeightMap = new Map();
    tokens.forEach(token => {
        tokenWeightMap.set(token, (tokenWeightMap.get(token) || 0) + 1);
    });

    return Array.from(tokenWeightMap, ([text, weight]) => ({ text, weight }));
}

function toTimestampMs(value) {
    const date = new Date(value);
    const timestamp = date.getTime();
    return Number.isFinite(timestamp) ? timestamp : null;
}

function canonicalizeArticleUrl(value) {
    if (!value) {
        return null;
    }

    try {
        const url = new URL(String(value));
        url.hash = '';
        url.searchParams.delete('utm_source');
        url.searchParams.delete('utm_medium');
        url.searchParams.delete('utm_campaign');
        url.searchParams.delete('utm_term');
        url.searchParams.delete('utm_content');
        url.searchParams.delete('gclid');
        url.searchParams.delete('fbclid');
        return url.toString();
    } catch {
        return null;
    }
}

function getTokenOverlapScore(leftSet, rightSet) {
    if (!leftSet || !rightSet || leftSet.size === 0 || rightSet.size === 0) {
        return 0;
    }

    let intersection = 0;
    leftSet.forEach(token => {
        if (rightSet.has(token)) {
            intersection += 1;
        }
    });

    return intersection / Math.min(leftSet.size, rightSet.size);
}

function getSetIntersectionCount(leftSet, rightSet) {
    if (!leftSet || !rightSet || leftSet.size === 0 || rightSet.size === 0) {
        return 0;
    }

    let intersection = 0;
    leftSet.forEach(token => {
        if (rightSet.has(token)) {
            intersection += 1;
        }
    });

    return intersection;
}

function isAnchorToken(token) {
    if (typeof token !== 'string') {
        return false;
    }
    if (token.length < 5) {
        return false;
    }
    if (DIGEST_TITLE_ANCHOR_STOPWORDS.has(token)) {
        return false;
    }
    if (/^\d+$/.test(token)) {
        return false;
    }
    return true;
}

function isSoftDigestMatchAllowed(
    currentFingerprint,
    clusterFingerprint,
    similarityScore,
    overlapScore,
    titleAnchorOverlapCount,
) {
    if (similarityScore >= DIGEST_SIMILARITY_THRESHOLD_STRICT) {
        return (
            titleAnchorOverlapCount >= DIGEST_STRICT_ANCHOR_MIN_MATCH && overlapScore >= DIGEST_STRICT_MIN_TOKEN_OVERLAP
        );
    }

    if (overlapScore < DIGEST_TOKEN_OVERLAP_THRESHOLD) {
        return false;
    }
    if (titleAnchorOverlapCount < DIGEST_TITLE_ANCHOR_MIN_MATCH) {
        return false;
    }

    if (!currentFingerprint.publishedAtMs || !clusterFingerprint.publishedAtMs) {
        return true;
    }

    const maxDistanceMs = DIGEST_SOFT_MATCH_MAX_HOURS * 60 * 60 * 1000;
    return Math.abs(currentFingerprint.publishedAtMs - clusterFingerprint.publishedAtMs) <= maxDistanceMs;
}

function createDigestFingerprint(article) {
    const normalizedText = normalizeFingerprintText(buildFingerprintText(article));
    const words = tokenizeFingerprintText(normalizedText);
    const effectiveWords = words.length > 0 ? words : tokenizeFingerprintText(normalizedText, { keepStopwords: true });
    const normalizedTitle = normalizeFingerprintText(article.title || '');
    const titleWords = tokenizeFingerprintText(normalizedTitle);
    const effectiveTitleWords =
        titleWords.length > 0 ? titleWords : tokenizeFingerprintText(normalizedTitle, { keepStopwords: true });
    const tokenSet = new Set(effectiveWords);
    const titleAnchorSet = new Set(titleWords.filter(isAnchorToken));
    const hash = effectiveWords.length > 0 ? simhash(buildSimhashTokens(effectiveWords)) : null;
    return {
        hash,
        tokenSet,
        titleAnchorSet: titleAnchorSet.size > 0 ? titleAnchorSet : new Set(effectiveTitleWords.filter(isAnchorToken)),
        publishedAtMs: toTimestampMs(article.publishedAt),
        canonicalUrl: canonicalizeArticleUrl(article.url),
    };
}

export function clusterDigestArticles(articles) {
    const clusters = [];
    const clusterByCanonicalUrl = new Map();

    articles.forEach(article => {
        const fingerprint = createDigestFingerprint(article);
        const canonicalUrl = fingerprint.canonicalUrl;
        let matchedClusterIndex = canonicalUrl ? clusterByCanonicalUrl.get(canonicalUrl) : undefined;
        let matchedCluster = Number.isInteger(matchedClusterIndex) ? clusters[matchedClusterIndex] : null;

        if (!matchedCluster && fingerprint.hash) {
            let bestCluster = null;
            let bestClusterIndex = -1;
            let bestSimilarity = -1;
            let bestOverlap = -1;
            let bestAnchorMatches = -1;

            for (let clusterIndex = 0; clusterIndex < clusters.length; clusterIndex += 1) {
                const cluster = clusters[clusterIndex];

                for (const member of cluster.members) {
                    if (!member.fingerprint.hash) {
                        continue;
                    }

                    const score = similarity(fingerprint.hash, member.fingerprint.hash);
                    if (score < DIGEST_SIMILARITY_THRESHOLD_SOFT) {
                        continue;
                    }

                    const overlapScore = getTokenOverlapScore(fingerprint.tokenSet, member.fingerprint.tokenSet);
                    const titleAnchorOverlapCount = getSetIntersectionCount(
                        fingerprint.titleAnchorSet,
                        member.fingerprint.titleAnchorSet,
                    );

                    if (
                        !isSoftDigestMatchAllowed(
                            fingerprint,
                            member.fingerprint,
                            score,
                            overlapScore,
                            titleAnchorOverlapCount,
                        )
                    ) {
                        continue;
                    }

                    if (
                        score > bestSimilarity ||
                        (score === bestSimilarity && titleAnchorOverlapCount > bestAnchorMatches) ||
                        (score === bestSimilarity &&
                            titleAnchorOverlapCount === bestAnchorMatches &&
                            overlapScore > bestOverlap)
                    ) {
                        bestCluster = cluster;
                        bestClusterIndex = clusterIndex;
                        bestSimilarity = score;
                        bestOverlap = overlapScore;
                        bestAnchorMatches = titleAnchorOverlapCount;
                    }
                }
            }

            matchedCluster = bestCluster;
            matchedClusterIndex = bestClusterIndex;
        }

        if (matchedCluster) {
            matchedCluster.items.push(article);
            matchedCluster.members.push({ article, fingerprint });
            matchedCluster.clusterCount = matchedCluster.items.length;
            if (canonicalUrl && Number.isInteger(matchedClusterIndex)) {
                clusterByCanonicalUrl.set(canonicalUrl, matchedClusterIndex);
            }
            return;
        }

        const newCluster = {
            representative: article,
            clusterTitle: article.title || 'Ohne Titel',
            clusterCount: 1,
            items: [article],
            members: [{ article, fingerprint }],
        };
        clusters.push(newCluster);
        if (canonicalUrl) {
            clusterByCanonicalUrl.set(canonicalUrl, clusters.length - 1);
        }
    });

    return clusters.map(cluster => ({
        representative: cluster.representative,
        clusterTitle: cluster.clusterTitle,
        clusterCount: cluster.clusterCount,
        items: cluster.items,
    }));
}
