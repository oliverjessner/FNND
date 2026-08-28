ALTER TABLE articles ADD COLUMN dismissedAt TEXT;

CREATE INDEX IF NOT EXISTS idx_articles_active_published_id
  ON articles (publishedAt DESC, id DESC)
  WHERE dismissedAt IS NULL;
