-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE FILE(name VARCHAR(200) PRIMARY KEY, last_published TIMESTAMP WITH TIME ZONE, encountered BOOLEAN, runs_not_encountered INTEGER);

INSERT INTO FILE VALUES ('/hello.txt', '2025-04-11 14:00:00', FALSE, 0);
INSERT INTO FILE VALUES ('/files/info.txt', '2025-04-10 08:00:00', FALSE, 0);
INSERT INTO FILE VALUES ('/files/subdir/secrets.txt', '2025-04-12 20:00:00', FALSE, 0);