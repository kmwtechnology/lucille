-- Schema for the PostgreSQL + pgvector example. Requires the pgvector extension:
-- https://github.com/pgvector/pgvector

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS documents (
  id         TEXT PRIMARY KEY,
  title      TEXT,
  body       TEXT,
  -- derived from body by Postgres; the indexer doesn't write it
  body_tsv   TSVECTOR GENERATED ALWAYS AS (to_tsvector('english', coalesce(body, ''))) STORED,
  meta       JSONB,
  tags       TEXT[],
  -- the sample data has 4-dimensional vectors; use the embedding model's dimension in a real pipeline
  embedding  VECTOR(4),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS documents_tsv_idx ON documents USING GIN (body_tsv);
CREATE INDEX IF NOT EXISTS documents_vec_idx ON documents USING hnsw (embedding vector_cosine_ops);
