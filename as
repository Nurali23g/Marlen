SELECT *
FROM table_a a
JOIN table_b b
  ON REGEXP_REPLACE(REGEXP_REPLACE(a.phone, '\D', '', 'g'), '^(7|8)', '') = b.phone
