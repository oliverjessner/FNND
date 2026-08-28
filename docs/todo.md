# Database and Digest TODO

## Current baseline

- drop the ~/Library/Application Support/NO-BULLSHIT-RSS/data.db
- and the dev database
- start from 0

- Production database: `~/Library/Application Support/NO-BULLSHIT-RSS/data.db`
- Current size: approximately 73 MB with 46,468 articles.
- Current journal mode: `DELETE`; current busy timeout: `0`.
- Current month can contain more than 8,000 articles, which are clustered on demand.
- Topic rules exist in both SQLite and `topics.rules.json` without a persisted classification version.
- `dailyDigested` currently affects daily, weekly, and monthly digests.
- Keep the existing production database as a backup: old RSS history cannot necessarily be fetched again.

## Product and semantics decisions

- [ ] Choose the canonical digest timezone; proposed default: `Europe/Vienna`.
- [ ] Define daily boundaries in that timezone, including daylight-saving transitions.
- [ ] Confirm ISO weeks run from Monday through Sunday.
- [ ] Decide whether periods without articles should be stored as empty digests.
- [ ] Define when an open digest becomes closed and immutable.
- [ ] Define the late-arrival rebuild window; proposal: 7 daily digests, current and previous week, current and previous month.
- [ ] Decide what should happen to articles older than the late-arrival window.
- [ ] Decide whether blocked words and excluded feeds affect only future generations or also trigger historical rebuilds.
- [ ] Separate the meaning of generated digest content from read, dismissed, and completed user states.
- [ ] Decide whether a read/dismissed action applies globally, per digest period, or per cluster occurrence.
- [ ] Replace or rename the ambiguous `dailyDigested` state.
- [ ] Choose one source of truth for topic definitions: SQLite is recommended; JSON should become import/export or backup only.
- [ ] Decide whether detailed matched-term explanations must be stored permanently or calculated on demand.
- [ ] Define retention rules for article content, old digest generations, and historical digests.

## Schema v2 foundation

- [ ] Create a separate `data-v2.db`; never rebuild destructively over the current production DB.
- [ ] Normalize sources and feeds so source metadata and logos are not duplicated across multiple feed URLs.
- [ ] Add a unique constraint for feed URLs.
- [ ] Model article identity as a feed-scoped external ID: `UNIQUE(feedId, externalId)`.
- [ ] Add a normalized/canonical URL for soft cross-feed duplicate detection.
- [ ] Add a content hash so changed articles can be detected and reprocessed.
- [ ] Replace `INSERT OR IGNORE` with explicit insert/update semantics.
- [ ] Reclassify and reassign an article when title, teaser, content, URL, or publication time changes.
- [ ] Use one consistent UTC timestamp representation for published, fetched, created, updated, and state timestamps.
- [ ] Define a fallback timestamp for articles without a valid `publishedAt`.
- [ ] Add database checks for Boolean/status values and required identifiers.
- [ ] Consider separating large article bodies into `article_content` so feed queries remain narrow.
- [ ] Move mutable user state into an `article_state` table instead of adding more columns to `articles`.
- [ ] Keep `lists` and `list_items`, but verify naming and uniqueness requirements.
- [ ] Keep schema changes in ordered, testable migrations.

## Topic model and processing

- [ ] Give topics a stable numeric ID and keep the slug as a unique editable identifier.
- [ ] Reference `topicId`, not `topicSlug`, from `article_topics`.
- [ ] Add a topic-rule version or deterministic rule hash.
- [ ] Store the classification version used for every classified article or assignment.
- [ ] Mark articles as stale when their classification version differs from the active rule version.
- [ ] Classify every new or materially updated article immediately after persistence.
- [ ] Replace the destructive full topic reprocess with resumable batches.
- [ ] Keep existing assignments visible until replacement assignments are complete.
- [ ] Automatically retry articles whose classification failed during import.
- [ ] Decide whether low-confidence matches should remain preview-only or be persisted separately.
- [ ] Reduce `matchedTermsJson`, which currently accounts for roughly 6 MB of stored text.
- [ ] If explanations remain persisted, store only the minimum audit data or move details to an optional table.
- [ ] Trigger an asynchronous stale-article reprocess after topic-rule changes.
- [ ] Make topic processing idempotent and crash-resumable.

## Materialized digest schema

- [ ] Add `digest_periods` with type, period key, UTC boundaries, timezone, status, active generation, dirty timestamp, generated timestamp, algorithm version, and rules version.
- [ ] Enforce one period per type and start boundary: `UNIQUE(type, startsAt)`.
- [ ] Use explicit period states such as `open`, `building`, `ready`, and `closed`.
- [ ] Add `digest_period_articles` with one row per period/article membership.
- [ ] Enforce `PRIMARY KEY(digestPeriodId, articleId)` for idempotent assignment.
- [ ] Allow every article to belong simultaneously to its daily, weekly, and monthly periods.
- [ ] Base period assignment on `publishedAt`, not fetch time.
- [ ] Add `digest_generations` so a rebuild never mutates the currently visible generation.
- [ ] Store generation version, status, timestamps, source article count, algorithm version, and rules version.
- [ ] Add `digest_clusters` with generation, stable cluster key, title, representative article, count, publication range, fingerprint, and display position.
- [ ] Add `digest_cluster_articles` with cluster, article, position, similarity, and representative flag.
- [ ] Ensure an article belongs to at most one cluster per digest generation.
- [ ] Store article references in digest tables rather than duplicating titles, URLs, bodies, or source data.
- [ ] Add a separate digest/cluster state table for read, dismissed, or completed state.
- [ ] Do not encode user state into generated cluster membership.

## Fetch pipeline

- [ ] Keep all network requests outside database transactions.
- [ ] Collect and normalize fetched items before opening a write transaction.
- [ ] Persist new and changed articles in short transactions using prepared statements or batched writes.
- [ ] Compute topic classification and the digest fingerprint for every new or changed article.
- [ ] Ensure the current daily, weekly, and monthly periods on every scheduled or manual fetch.
- [ ] Finalize periods whose end boundary has passed.
- [ ] Catch up missing periods after the app has been offline.
- [ ] Assign new articles to daily, weekly, and monthly periods in an idempotent operation.
- [ ] Mark only affected periods dirty.
- [ ] If a fetch contains no changed articles, avoid unnecessary digest rebuilds.
- [ ] Persist dirty/building state so an interrupted process can resume after restart.
- [ ] Run digest generation after the article transaction, not inside it.
- [ ] Ensure webhook articles use exactly the same insert, topic, fingerprint, and digest pipeline as RSS articles.
- [ ] Publish UI/SSE updates only after the relevant database transaction commits.

## Digest generation and clustering

- [ ] Keep one clustering implementation shared by stored digests, API, UI, and CLI.
- [ ] Make clustering deterministic with a defined `publishedAt DESC, id DESC` input order.
- [ ] Persist a versioned digest fingerprint for each article at import time.
- [ ] Include canonical URL, normalized title tokens, SimHash, and any required candidate keys in the fingerprint.
- [ ] Recompute fingerprints only when relevant article content or the fingerprint algorithm changes.
- [ ] Avoid comparing every monthly article against every previous cluster.
- [ ] Restrict candidates using canonical URLs, fingerprint buckets, and the existing time-distance rules.
- [ ] Expire clustering candidates that can no longer match because they fall outside the configured time window.
- [ ] Preserve current excluded-feed and blocked-word behavior in the generator.
- [ ] Record a digest algorithm version and filter-rules version on every generation.
- [ ] Build a new generation completely before changing `activeGenerationId`.
- [ ] Switch the active generation atomically in a short transaction.
- [ ] Keep the previous successful generation available if generation fails.
- [ ] Remove superseded generations according to the retention policy.
- [ ] Rebuild only dirty or version-stale periods.
- [ ] Reopen or rebuild closed periods only within the agreed late-arrival window.
- [ ] Decide whether weekly/monthly digests cluster raw articles independently or reuse lower-level cluster fingerprints; independent clustering is safer semantically.

## SQLite runtime and concurrency

- [ ] Enable WAL mode for production database concurrency.
- [ ] Configure a non-zero busy timeout for server, Electron, maintenance tools, and CLI connections.
- [ ] Evaluate `synchronous=NORMAL` together with WAL.
- [ ] Enable foreign-key enforcement on every writable connection.
- [ ] Avoid network or CPU-heavy clustering work while a write transaction is open.
- [ ] Add graceful handling and retry policy for `SQLITE_BUSY`.
- [ ] Use SQLite's backup API or `VACUUM INTO` for live backups; do not blindly copy a live WAL database.
- [ ] Run `PRAGMA optimize` after meaningful bulk changes or on a suitable maintenance schedule.
- [ ] Run `ANALYZE` after schema creation and major data distribution changes.
- [ ] Use `VACUUM` only after material deletes; the current production DB has no freelist space to reclaim.

## Search, queries, and indexes

- [ ] Add an FTS5 index for title, teaser, and optionally content.
- [ ] Define how FTS rows stay synchronized with article inserts, updates, and deletes.
- [ ] Replace `%query%` feed scans with FTS search and deterministic result ordering.
- [ ] Return only required article columns from feed, digest, and CLI queries instead of `articles.*`.
- [ ] Stop returning or base64-encoding the same feed logo for every article row.
- [ ] Load full article content only when a reader or clustering operation needs it.
- [ ] Replace deep `LIMIT/OFFSET` pagination with a `(publishedAt, id)` cursor.
- [ ] Review the overlapping published, active, feed, and undigested article indexes using real query plans.
- [ ] Retain a chronological index for all articles because the CLI can include dismissed articles.
- [ ] Add an active chronological index only if dismissed-state usage makes it selective enough.
- [ ] Add a feed/active chronological index for the feed view if measurements justify it.
- [ ] Replace the nearly full undigested partial index with a digest-specific index combining all actual digest predicates.
- [ ] Verify topic-filter plans and prefer an indexed join when a rare topic currently causes a chronological scan.
- [ ] Add performance budgets for latest-feed, text-search, topic-filter, daily, weekly, and monthly queries.

## API, UI, and CLI integration

- [ ] Make the API read the active stored digest generation instead of clustering on every request.
- [ ] Make the CLI read the same active stored generation as the API and UI.
- [ ] Define API behavior while a new generation is building: return the previous ready generation plus build metadata.
- [ ] Expose period key, period status, generated timestamp, algorithm version, and article/cluster counts.
- [ ] Keep generated digest data independent from UI read/dismissed state.
- [ ] Replace current `dailyDigested` endpoints with explicit period/cluster state endpoints.
- [ ] Ensure digest settings changes mark the correct periods dirty.
- [ ] Show when a digest is stale, rebuilding, ready, or closed.
- [ ] Keep the CLI read-only and prevent it from creating periods or triggering rebuilds.

## Migration and cutover

- [ ] Create a verified backup of the existing production DB before any migration work.
- [ ] Record baseline counts, date ranges, database size, topic assignments, and representative query timings.
- [ ] Build schema v2 in a separate database file.
- [ ] Import sources and feeds while deduplicating feed URLs.
- [ ] Import articles without losing historical entries that feeds can no longer supply.
- [ ] Normalize URLs and derive feed-scoped external IDs during import.
- [ ] Detect and report identity collisions instead of silently ignoring them.
- [ ] Import lists and list items with referential-integrity checks.
- [ ] Import topic definitions into the selected source of truth.
- [ ] Reclassify all imported articles using the initial schema-v2 rule version.
- [ ] Compute digest fingerprints for all imported articles.
- [ ] Generate historical digest periods only for the chosen retention window.
- [ ] Compare old and new article counts, feed counts, topic distributions, and sample digest clusters.
- [ ] Run SQLite integrity and foreign-key checks against the new DB.
- [ ] Measure file size and critical query/runtime performance before cutover.
- [ ] Keep an explicit rollback path to the old DB.
- [ ] Switch Electron/server/CLI to the new DB only after acceptance checks pass.
- [ ] Preserve the old DB as a read-only archive for an agreed period.

## Tests and acceptance criteria

- [ ] Test daily boundaries across normal days and daylight-saving changes.
- [ ] Test ISO week and month boundaries, including year changes.
- [ ] Test first fetch after the app was offline across one or more boundaries.
- [ ] Test late-arriving and corrected articles.
- [ ] Test repeated fetches for idempotent article and period membership counts.
- [ ] Test duplicate GUIDs across different feeds.
- [ ] Test canonical-URL duplicates with tracking parameters.
- [ ] Test an article update triggering topic and digest recalculation.
- [ ] Test topic-rule changes and automatic version-based reprocessing.
- [ ] Test blocked-word and excluded-feed rule changes.
- [ ] Test generation failure before and after the atomic active-version switch.
- [ ] Test restart recovery for dirty and interrupted generations.
- [ ] Test concurrent feed writes, UI reads, and read-only CLI access under WAL.
- [ ] Test that read/dismissed actions never alter generated digest membership.
- [ ] Test that Daily, Weekly, and Monthly can hold different state for the same article.
- [ ] Test FTS synchronization and search result ordering.
- [ ] Test keyset pagination without missing or duplicating equal-timestamp articles.
- [ ] Test migrations exclusively against temporary database fixtures.
- [ ] Verify that no automated test ever opens the real user database for writing.
- [ ] Establish target generation times for typical daily, weekly, and 8,000-article monthly digests.
- [ ] Establish acceptable database size and growth targets.
- [ ] Document the final schema, period semantics, rebuild policy, backup procedure, and recovery procedure in `docs/`.
