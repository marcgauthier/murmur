-- murmurd example schema.sql, referenced by config.toml [schema] file.
--
-- The file holds CREATE TABLE statements in the strict subset only:
-- no indexes, views, foreign keys, defaults, or table options, and no
-- data statements (INSERT/UPDATE/DELETE belong to clients, not to
-- boot). Every table needs exactly one PRIMARY KEY on an `id` blob
-- column; other columns are integer, real, text, or blob spellings
-- (SQLite, PostgreSQL, and common MySQL names all map). Columns are
-- nullable unless declared NOT NULL. Comments (-- and /* */) allowed.
--
-- Editing this file and restarting migrates additive changes in;
-- redefining a stored column's type fails the boot loudly.

CREATE TABLE docs (
  id BLOB PRIMARY KEY,
  title TEXT,
  body TEXT
);

CREATE TABLE notes (
  id BLOB PRIMARY KEY,
  title TEXT NOT NULL,
  n INTEGER
);
