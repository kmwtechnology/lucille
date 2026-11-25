CREATE TABLE docs (
  id VARCHAR PRIMARY KEY,
  title VARCHAR,
  n INT CHECK (n IS NULL OR n > 0),
  meta JSON,
  tags VARCHAR ARRAY,
  ts TIMESTAMP
);

INSERT INTO docs (id, title) VALUES ('existing', 'already there');
