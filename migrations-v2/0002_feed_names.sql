ALTER TABLE feeds ADD COLUMN name TEXT NOT NULL DEFAULT '';

UPDATE feeds
SET name = COALESCE(
  (SELECT sources.name FROM sources WHERE sources.id = feeds.sourceId),
  ''
)
WHERE name = '';
