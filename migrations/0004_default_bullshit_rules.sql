WITH default_rules (name, enabled, field, operator, value) AS (
  VALUES
    ('Black Friday', 0, 'title', 'contains', 'Black Friday'),
    ('Sponsored', 0, 'title', 'contains', 'Sponsored'),
    ('Deals', 0, 'url', 'contains', '/deals/'),
    ('SEO List', 0, 'title', 'regex', '^Die \d+')
)
INSERT INTO bullshit_rules (name, enabled, field, operator, value, createdAt, updatedAt)
SELECT default_rules.name,
       default_rules.enabled,
       default_rules.field,
       default_rules.operator,
       default_rules.value,
       datetime('now'),
       datetime('now')
FROM default_rules
WHERE NOT EXISTS (
  SELECT 1
  FROM bullshit_rules AS existing
  WHERE lower(existing.name) = lower(default_rules.name)
);
