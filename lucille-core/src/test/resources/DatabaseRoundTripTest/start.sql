CREATE TABLE source (
  id VARCHAR PRIMARY KEY,
  title VARCHAR,
  quantity INT,
  big BIGINT,
  price DECIMAL(10, 2),
  ratio DOUBLE,
  score REAL,
  active BOOLEAN,
  created TIMESTAMP,
  birthday DATE,
  alarm TIME,
  payload VARBINARY,
  notes VARCHAR
);

-- the same schema under a different name, written by the DatabaseIndexer
CREATE TABLE target AS SELECT * FROM source WITH NO DATA;

-- the transaction that wrote each row of target, so the test can count the batches
CREATE TABLE writes (id VARCHAR, tx BIGINT);
CREATE TRIGGER record_transaction AFTER INSERT ON target FOR EACH ROW
  CALL 'com.kmwllc.lucille.indexer.DatabaseRoundTripTest$RecordTransaction';

INSERT INTO source VALUES
  ('a', 'First', 1, 9007199254740993, 12.34, 0.5, 1.25, TRUE, TIMESTAMP '2024-03-10 08:30:15.123456',
   DATE '1990-07-04', TIME '10:15:30', X'00FF10', 'has notes'),
  ('b', 'Second', -7, -42, 0.01, -1.0E10, -0.75, FALSE, TIMESTAMP '1970-01-01 00:00:00',
   DATE '2000-02-29', TIME '23:59:59', X'', NULL),
  ('c', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- 97 more rows, for 100 in all, with values derived from the row number and some NULLs
INSERT INTO source
SELECT
  'r' || LPAD(CAST(X AS VARCHAR), 3, '0'),
  CASE WHEN MOD(X, 7) = 0 THEN NULL ELSE 'Row ' || X END,
  CASE WHEN MOD(X, 2) = 0 THEN CAST(X AS INT) ELSE -CAST(X AS INT) END,
  9007199254740993 + X,
  CAST(X * 1.37 AS DECIMAL(10, 2)),
  CAST(X AS DOUBLE) / 3,
  CAST(X AS REAL) / 3,
  MOD(X, 2) = 0,
  DATEADD('MICROSECOND', X * 1234567891, TIMESTAMP '2024-01-01 00:00:00'),
  DATEADD('DAY', X * 37, DATE '1950-01-01'),
  DATEADD('SECOND', X * 611, TIME '00:00:00'),
  HASH('SHA-256', CAST(X AS VARCHAR)),
  CASE WHEN MOD(X, 5) = 0 THEN NULL ELSE REPEAT('n', CAST(X AS INT)) END
FROM SYSTEM_RANGE(1, 97);
