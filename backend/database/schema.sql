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
  guidOrHash TEXT UNIQUE,
  dailyDigested INTEGER NOT NULL DEFAULT 0,
  createdAt TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (feedId) REFERENCES feeds(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_articles_publishedAt ON articles (publishedAt);
CREATE INDEX IF NOT EXISTS idx_articles_feedId ON articles (feedId);

CREATE TABLE IF NOT EXISTS lists (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT,
  color TEXT NOT NULL DEFAULT '#1d1d1f',
  createdAt TEXT NOT NULL DEFAULT (datetime('now')),
  updatedAt TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS list_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  listId INTEGER NOT NULL,
  articleId INTEGER NOT NULL,
  createdAt TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (listId, articleId),
  FOREIGN KEY (listId) REFERENCES lists(id) ON DELETE CASCADE,
  FOREIGN KEY (articleId) REFERENCES articles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_list_items_listId ON list_items (listId);
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
  config_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS article_topics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  article_id INTEGER NOT NULL,
  topic_slug TEXT NOT NULL,
  score REAL NOT NULL,
  matched_terms_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (article_id, topic_slug),
  FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
  FOREIGN KEY (topic_slug) REFERENCES topics(slug) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_article_topics_article_id ON article_topics (article_id);
CREATE INDEX IF NOT EXISTS idx_article_topics_topic_slug ON article_topics (topic_slug);
