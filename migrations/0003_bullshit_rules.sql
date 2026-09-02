CREATE TABLE bullshit_rules (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL CHECK (length(trim(name)) > 0),
  enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
  field TEXT NOT NULL CHECK (field IN ('title', 'teaser', 'url', 'source')),
  operator TEXT NOT NULL CHECK (operator IN ('contains', 'not_contains', 'equals', 'regex')),
  value TEXT NOT NULL CHECK (length(trim(value)) > 0),
  createdAt TEXT NOT NULL,
  updatedAt TEXT NOT NULL
);

CREATE INDEX idx_bullshit_rules_enabled ON bullshit_rules (enabled, id);

CREATE TABLE article_bullshit_matches (
  articleId INTEGER NOT NULL,
  ruleId INTEGER NOT NULL,
  matchedAt TEXT NOT NULL,
  PRIMARY KEY (articleId, ruleId),
  FOREIGN KEY (articleId) REFERENCES articles(id) ON DELETE CASCADE,
  FOREIGN KEY (ruleId) REFERENCES bullshit_rules(id) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE INDEX idx_article_bullshit_matches_rule ON article_bullshit_matches (ruleId, articleId);

