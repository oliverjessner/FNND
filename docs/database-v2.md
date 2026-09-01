# Database v2 and materialized digests

## Decisions

- Canonical digest timezone: `Europe/Vienna`.
- Days use local midnight boundaries and therefore contain 23 or 25 hours across daylight-saving changes.
- Weeks are ISO-8601 Monday-through-Sunday weeks. Month boundaries use the same timezone.
- Empty periods are persisted. This allows the application to distinguish an empty ready digest from a missing or interrupted digest.
- Generated membership is immutable inside a generation. Rebuilds create a new generation and atomically switch `activeGenerationId` only after every cluster and member was written.
- Rebuild window: the latest seven local days, current and previous ISO week, and current and previous month. Older late arrivals are stored and assigned to their historical periods, but a closed period is not automatically reopened.
- Feed dismissal is global article state. Digest read/dismissed/completed state is keyed by digest period and stable cluster key. Completing a Daily cluster does not complete its Weekly or Monthly occurrence.
- Excluded-feed, blocked-word, topic-rule, or materially changed article data marks only rebuildable periods dirty. Generated membership is independent from UI state.
- SQLite is the source of truth for topic definitions. `topics.rules.json` is used only to bootstrap an empty database and as an import/export representation.
- Topic explanations persist only confidence and matched term names. Full scoring detail remains available from classification preview and is not duplicated for every assignment.
- Article history has no automatic deletion policy. Only the active and one previous successful digest generation are retained per period; failed generations remain available for diagnosis.

## Schema

The ordered v2 schema starts at [migrations/0001_schema.sql](../migrations/0001_schema.sql). The application applies pending migrations in filename order inside transactions before it starts accepting requests, and records each applied filename in `schema_migrations`.

- `sources` owns source name, website, and logo. `feeds` owns unique feed URLs and references a source.
- `articles` uses `UNIQUE(feedId, externalId)`, a canonical URL, content hash, UTC timestamps, classification version/status, and a versioned persisted digest fingerprint.
- `article_state` owns mutable global read/dismissed state. `lists` and `list_items` retain saved-item state.
- `topics` has stable numeric IDs, editable unique slugs, deterministic hashes, and monotonically increasing per-topic versions. `article_topics` references `topicId` and records the complete ruleset version used.
- `articles_fts` is an FTS5 external-content index synchronized by insert/update/delete triggers.
- `digest_periods` owns timezone-aware boundaries, state, dirty marker, versions, and the active generation pointer.
- `digest_period_articles` idempotently assigns every article to Daily, Weekly, and Monthly periods.
- `digest_generations`, `digest_clusters`, and `digest_cluster_articles` store immutable generated output and guarantee that an article appears at most once per generation.
- `digest_cluster_state` stores per-period user state separately from generated membership.

All persisted timestamps are UTC ISO-8601 strings. A missing or invalid publication date falls back to the fetch timestamp, so every article has a deterministic period assignment.

## Runtime and pipeline

Every writable application connection enables foreign keys, WAL, `synchronous=NORMAL`, and a 5,000 ms busy timeout. Busy/locked operations use bounded exponential retry. Network requests and clustering happen outside write transactions.

RSS and webhook input both use `article-ingest.js`:

1. Normalize the complete fetched batch before persistence.
2. Explicitly insert or update by `(feedId, externalId)`.
3. Reprocess only when the content hash or active classification version changed.
4. Persist the fingerprint and assign Daily, Weekly, and Monthly membership in a short transaction.
5. Classify the committed article; failed classification remains retryable.
6. Generate dirty periods after the article writes are complete.
7. Publish SSE updates only after persistence and generation complete.

Startup marks interrupted `building` generations failed, restores the previous active generation, and retries the dirty period. In-process generation locks prevent concurrent builds for the same period.

Digest clustering consumes persisted fingerprints. Exact hashes are resolved directly; remaining strict candidates use positional SimHash buckets, and relaxed candidates use title-anchor pairs. This preserves the existing similarity rules while avoiding an all-articles-by-all-clusters monthly scan.

The API and read-only CLI both read `activeGenerationId`. If a rebuild is in progress, the previous ready generation remains visible and the API exposes `status`, `stale`, `generatedAt`, `algorithmVersion`, `rulesVersion`, and period counts.

## Search and pagination

- Feed text search uses FTS5 over title, teaser, and content.
- Feed ordering is deterministic: `publishedAt DESC, id DESC`.
- Browser pagination uses a `(publishedAt, id)` cursor. The legacy offset parameter remains accepted for CLI compatibility, but the UI does not use it.
- Article and Digest payloads return explicit projections. Logos are loaded once through `/api/feeds/:id/logo` with immutable browser caching instead of base64 duplication per article.

## Performance budgets

Development budgets are intentionally broad enough for normal hardware while still catching regressions:

| Operation | Budget |
| --- | ---: |
| Latest 100 feed rows | 50 ms |
| FTS text search, first 100 | 100 ms |
| Topic-filter first page | 100 ms |
| Stored Daily API response | 100 ms |
| Stored Weekly API response | 200 ms |
| Stored Monthly API response | 500 ms |
| Cluster 8,000 normalized articles | 2,000 ms |

Observed on the 2026-08-29 cutover machine after the first clean fetch:

- 279 articles fetched, classified, assigned, and materialized in 5.65 seconds.
- Database query timings: latest 100 2.79 ms, FTS `OpenAI` 0.70 ms, Daily cluster query 0.22 ms, Weekly 0.25 ms, Monthly 0.08 ms.
- Complete local API payloads: Daily 3.3 ms, Weekly 7.4 ms, Monthly 2.8 ms.
- Synthetic 8,000-article clustering: approximately 440 ms.

## Cutover record

The 2026-08-29 legacy production baseline was:

- 73,175,040 bytes, DELETE journal mode.
- 46,580 articles spanning 2025-10-17 through 2026-08-29.
- 16 feeds and 26,983 topic assignments.
- Current month: 8,261 articles.
- `integrity_check=ok`, zero foreign-key violations.

The requested clean-start cutover did not import legacy history. Both production and repository development databases started with zero articles. The first production fetch then populated 279 currently available feed items.

The old production database is preserved in:

```text
~/Library/Application Support/NO-BULLSHIT-RSS/backups/data-legacy-pre-v2-2026-08-29.db
~/Library/Application Support/NO-BULLSHIT-RSS/backups/data-legacy-original-2026-08-29.db
```

The first file was produced with SQLite's live backup API and passed `integrity_check`. The second is the stopped original file. Development equivalents are under the repository's ignored `backups/` directory.

## Backup and recovery

Create and verify a live backup:

```bash
npm run db:backup -- --source "/path/to/data-v2.db" --destination "/safe/path/data-backup.db"
```

Generate a read-only database report:

```bash
npm run db:report -- "/path/to/data-v2.db"
```

Recovery procedure:

1. Quit Electron and confirm its backend process has stopped.
2. Back up the damaged/current `data-v2.db` if it is readable.
3. Run `npm run db:report -- "/path/to/backup.db"` and require `integrity=ok` and zero foreign-key violations.
4. Move the current `data-v2.db`, `data-v2.db-wal`, and `data-v2.db-shm` aside as one recovery set; do not copy only the main file from a running WAL database.
5. Restore the verified backup as `data-v2.db` while the application is stopped.
6. Start Electron. Startup recovery handles interrupted digest generations and rebuilds dirty periods.

Run `VACUUM` only after material deletion. `ANALYZE` runs after database initialization and `PRAGMA optimize` runs after migrations/initialization.
