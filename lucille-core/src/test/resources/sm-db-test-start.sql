-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE FILE(name VARCHAR(200) PRIMARY KEY, last_published BIGINT, encountered BOOLEAN, runs_not_encountered INTEGER);

-- last_published is epoch microseconds: 2025-04-11 14:00:00, 2025-04-10 08:00:00 and 2025-04-12 20:00:00 UTC
INSERT INTO FILE VALUES ('/hello.txt', 1744380000000000, FALSE, 0);
INSERT INTO FILE VALUES ('/files/info.txt', 1744272000000000, FALSE, 0);
INSERT INTO FILE VALUES ('/files/subdir/secrets.txt', 1744488000000000, FALSE, 0);
