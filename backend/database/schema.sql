PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS feeds (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  websiteUrl TEXT NOT NULL,
  feedUrl TEXT NOT NULL,
  logo BLOB,
  logoMime TEXT,
  createdAt TEXT NOT NULL DEFAULT (datetime('now')),
  updatedAt TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS articles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  feedId INTEGER NOT NULL,
  title TEXT,
  teaser TEXT,
  content TEXT,
  url TEXT UNIQUE,
  publishedAt TEXT,
  guidOrHash TEXT NOT NULL UNIQUE CHECK (length(trim(guidOrHash)) > 0),
  dailyDigested INTEGER NOT NULL DEFAULT 0,
  dismissedAt TEXT,
  createdAt TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (feedId) REFERENCES feeds(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_articles_publishedAt_id ON articles (publishedAt DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_articles_feedId_publishedAt_id ON articles (feedId, publishedAt DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_articles_undigested_published_id
  ON articles (publishedAt DESC, id DESC)
  WHERE dailyDigested = 0;
CREATE INDEX IF NOT EXISTS idx_articles_active_published_id
  ON articles (publishedAt DESC, id DESC)
  WHERE dismissedAt IS NULL;

CREATE TABLE IF NOT EXISTS lists (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT,
  color TEXT NOT NULL DEFAULT '#1d1d1f',
  createdAt TEXT NOT NULL DEFAULT (datetime('now')),
  updatedAt TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS list_items (
  listId INTEGER NOT NULL,
  articleId INTEGER NOT NULL,
  createdAt TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (listId, articleId),
  FOREIGN KEY (listId) REFERENCES lists(id) ON DELETE CASCADE,
  FOREIGN KEY (articleId) REFERENCES articles(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_list_items_articleId ON list_items (articleId);

CREATE TABLE IF NOT EXISTS digest_excluded_feeds (
  feedId INTEGER PRIMARY KEY,
  createdAt TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (feedId) REFERENCES feeds(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS digest_blocked_words (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  word TEXT NOT NULL UNIQUE COLLATE NOCASE,
  createdAt TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS topics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  slug TEXT NOT NULL UNIQUE,
  label TEXT NOT NULL,
  configJson TEXT NOT NULL,
  createdAt TEXT NOT NULL DEFAULT (datetime('now')),
  updatedAt TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS article_topics (
  articleId INTEGER NOT NULL,
  topicSlug TEXT NOT NULL,
  score REAL NOT NULL,
  matchedTermsJson TEXT NOT NULL,
  createdAt TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (articleId, topicSlug),
  FOREIGN KEY (articleId) REFERENCES articles(id) ON DELETE CASCADE,
  FOREIGN KEY (topicSlug) REFERENCES topics(slug) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_article_topics_topicSlug ON article_topics (topicSlug);
