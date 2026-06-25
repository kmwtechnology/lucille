-- Demo schema for the PostgresIndexer example.
-- Requires pgvector: https://github.com/pgvector/pgvector

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS documents (
  id         TEXT PRIMARY KEY,
  title      TEXT,
  body       TEXT,
  -- tsvector is derived from body server-side; the indexer does NOT write this column.
  body_tsv   TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', coalesce(body, ''))) STORED,
  tags       TEXT[],
  embedding  VECTOR(1536),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS documents_tsv_idx ON documents USING GIN (body_tsv);
CREATE INDEX IF NOT EXISTS documents_vec_idx ON documents USING hnsw (embedding vector_cosine_ops);

-- Example hybrid query (FTS + vector ANN):
--   SELECT id, title, ts_rank(body_tsv, plainto_tsquery('english', :q)) AS fts_score,
--          1 - (embedding <=> :query_vec) AS vec_score
--     FROM documents
--    WHERE body_tsv @@ plainto_tsquery('english', :q)
--    ORDER BY vec_score DESC
--    LIMIT 20;
