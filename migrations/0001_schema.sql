PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_migrations (
  version TEXT PRIMARY KEY,
  appliedAt TEXT NOT NULL
);

CREATE TABLE app_metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updatedAt TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  websiteUrl TEXT NOT NULL,
  canonicalWebsiteUrl TEXT NOT NULL UNIQUE,
  logo BLOB,
  logoMime TEXT,
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL
);

CREATE TABLE feeds (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sourceId INTEGER NOT NULL,
  feedUrl TEXT NOT NULL UNIQUE,
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL,
  FOREIGN KEY (sourceId) REFERENCES sources(id) ON DELETE CASCADE
);

CREATE TABLE articles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  feedId INTEGER NOT NULL,
  externalId TEXT NOT NULL CHECK (length(trim(externalId)) > 0),
  title TEXT,
  teaser TEXT,
  content TEXT,
  url TEXT,
  canonicalUrl TEXT,
  contentHash TEXT NOT NULL,
  publishedAt TEXT NOT NULL,
  fetchedAt TEXT NOT NULL,
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL,
  classificationVersion TEXT,
  classificationStatus TEXT NOT NULL DEFAULT 'pending'
    CHECK (classificationStatus IN ('pending', 'ready', 'failed')),
  fingerprintVersion TEXT,
  digestFingerprintJson TEXT,
  UNIQUE (feedId, externalId),
  FOREIGN KEY (feedId) REFERENCES feeds(id) ON DELETE CASCADE
);

CREATE INDEX idx_articles_published_id ON articles (publishedAt DESC, id DESC);
CREATE INDEX idx_articles_feed_published_id ON articles (feedId, publishedAt DESC, id DESC);
CREATE INDEX idx_articles_canonical_url ON articles (canonicalUrl) WHERE canonicalUrl IS NOT NULL;
CREATE INDEX idx_articles_classification_stale ON articles (classificationStatus, classificationVersion, id);

CREATE TABLE article_state (
  articleId INTEGER PRIMARY KEY,
  readAt TEXT,
  dismissedAt TEXT,
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL,
  FOREIGN KEY (articleId) REFERENCES articles(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX idx_article_state_dismissed ON article_state (dismissedAt, articleId);

CREATE TABLE lists (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT,
  color TEXT NOT NULL DEFAULT '#1d1d1f',
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL
);

CREATE TABLE list_items (
  listId INTEGER NOT NULL,
  articleId INTEGER NOT NULL,
  createdAt TEXT NOT NULL,
  PRIMARY KEY (listId, articleId),
  FOREIGN KEY (listId) REFERENCES lists(id) ON DELETE CASCADE,
  FOREIGN KEY (articleId) REFERENCES articles(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX idx_list_items_article ON list_items (articleId, listId);

CREATE TABLE topics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  slug TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL,
  configJson TEXT NOT NULL,
  ruleHash TEXT NOT NULL,
  ruleVersion INTEGER NOT NULL CHECK (ruleVersion > 0),
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL
);

CREATE TABLE article_topics (
  articleId INTEGER NOT NULL,
  topicId INTEGER NOT NULL,
  score REAL NOT NULL,
  matchedTermsJson TEXT,
  classificationVersion TEXT NOT NULL,
  createdAt TEXT NOT NULL,
  PRIMARY KEY (articleId, topicId),
  FOREIGN KEY (articleId) REFERENCES articles(id) ON DELETE CASCADE,
  FOREIGN KEY (topicId) REFERENCES topics(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX idx_article_topics_topic ON article_topics (topicId, articleId);

CREATE TABLE digest_excluded_feeds (
  feedId INTEGER PRIMARY KEY,
  createdAt TEXT NOT NULL,
  FOREIGN KEY (feedId) REFERENCES feeds(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE TABLE digest_blocked_words (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  word TEXT NOT NULL UNIQUE COLLATE NOCASE,
  createdAt TEXT NOT NULL
);

CREATE TABLE digest_periods (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL CHECK (type IN ('day', 'week', 'month')),
  periodKey TEXT NOT NULL,
  startsAt TEXT NOT NULL,
  endsAt TEXT NOT NULL,
  timezone TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open'
    CHECK (status IN ('open', 'building', 'ready', 'closed')),
  activeGenerationId INTEGER,
  dirtyAt TEXT,
  generatedAt TEXT,
  algorithmVersion TEXT NOT NULL,
  rulesVersion TEXT NOT NULL,
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL,
  UNIQUE (type, startsAt),
  UNIQUE (type, periodKey)
);

CREATE INDEX idx_digest_periods_dirty ON digest_periods (dirtyAt, startsAt) WHERE dirtyAt IS NOT NULL;
CREATE INDEX idx_digest_periods_type_start ON digest_periods (type, startsAt DESC);

CREATE TABLE digest_period_articles (
  digestPeriodId INTEGER NOT NULL,
  articleId INTEGER NOT NULL,
  assignedAt TEXT NOT NULL,
  PRIMARY KEY (digestPeriodId, articleId),
  FOREIGN KEY (digestPeriodId) REFERENCES digest_periods(id) ON DELETE CASCADE,
  FOREIGN KEY (articleId) REFERENCES articles(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX idx_digest_period_articles_article ON digest_period_articles (articleId, digestPeriodId);

CREATE TABLE digest_generations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  digestPeriodId INTEGER NOT NULL,
  generationNumber INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('building', 'ready', 'failed', 'superseded')),
  sourceArticleCount INTEGER NOT NULL DEFAULT 0,
  clusterCount INTEGER NOT NULL DEFAULT 0,
  algorithmVersion TEXT NOT NULL,
  rulesVersion TEXT NOT NULL,
  startedAt TEXT NOT NULL,
  generatedAt TEXT,
  failedAt TEXT,
  error TEXT,
  UNIQUE (digestPeriodId, generationNumber),
  FOREIGN KEY (digestPeriodId) REFERENCES digest_periods(id) ON DELETE CASCADE
);

CREATE INDEX idx_digest_generations_period_status ON digest_generations (digestPeriodId, status, generationNumber DESC);

CREATE TABLE digest_clusters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  digestGenerationId INTEGER NOT NULL,
  clusterKey TEXT NOT NULL,
  title TEXT NOT NULL,
  representativeArticleId INTEGER NOT NULL,
  articleCount INTEGER NOT NULL CHECK (articleCount > 0),
  firstPublishedAt TEXT NOT NULL,
  lastPublishedAt TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  displayPosition INTEGER NOT NULL CHECK (displayPosition >= 0),
  UNIQUE (digestGenerationId, clusterKey),
  UNIQUE (digestGenerationId, displayPosition),
  FOREIGN KEY (digestGenerationId) REFERENCES digest_generations(id) ON DELETE CASCADE,
  FOREIGN KEY (representativeArticleId) REFERENCES articles(id) ON DELETE RESTRICT
);

CREATE TABLE digest_cluster_articles (
  digestClusterId INTEGER NOT NULL,
  digestGenerationId INTEGER NOT NULL,
  articleId INTEGER NOT NULL,
  position INTEGER NOT NULL CHECK (position >= 0),
  similarity REAL,
  isRepresentative INTEGER NOT NULL CHECK (isRepresentative IN (0, 1)),
  PRIMARY KEY (digestClusterId, articleId),
  UNIQUE (digestClusterId, position),
  FOREIGN KEY (digestClusterId) REFERENCES digest_clusters(id) ON DELETE CASCADE,
  FOREIGN KEY (digestGenerationId) REFERENCES digest_generations(id) ON DELETE CASCADE,
  FOREIGN KEY (articleId) REFERENCES articles(id) ON DELETE RESTRICT
) WITHOUT ROWID;

CREATE UNIQUE INDEX idx_digest_generation_article_once
  ON digest_cluster_articles (digestGenerationId, articleId);

CREATE TABLE digest_cluster_state (
  digestPeriodId INTEGER NOT NULL,
  clusterKey TEXT NOT NULL,
  readAt TEXT,
  dismissedAt TEXT,
  completedAt TEXT,
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL,
  PRIMARY KEY (digestPeriodId, clusterKey),
  FOREIGN KEY (digestPeriodId) REFERENCES digest_periods(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE VIRTUAL TABLE articles_fts USING fts5(
  title,
  teaser,
  content,
  content='articles',
  content_rowid='id',
  tokenize='unicode61 remove_diacritics 2'
);

CREATE TRIGGER articles_fts_insert AFTER INSERT ON articles BEGIN
  INSERT INTO articles_fts(rowid, title, teaser, content)
  VALUES (new.id, new.title, new.teaser, new.content);
END;

CREATE TRIGGER articles_fts_delete AFTER DELETE ON articles BEGIN
  INSERT INTO articles_fts(articles_fts, rowid, title, teaser, content)
  VALUES ('delete', old.id, old.title, old.teaser, old.content);
END;

CREATE TRIGGER articles_fts_update AFTER UPDATE OF title, teaser, content ON articles BEGIN
  INSERT INTO articles_fts(articles_fts, rowid, title, teaser, content)
  VALUES ('delete', old.id, old.title, old.teaser, old.content);
  INSERT INTO articles_fts(rowid, title, teaser, content)
  VALUES (new.id, new.title, new.teaser, new.content);
END;
