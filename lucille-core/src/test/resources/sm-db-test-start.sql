-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE FILE(name VARCHAR(200) PRIMARY KEY, last_published TIMESTAMP, encountered BOOLEAN);

INSERT INTO FILE VALUES ('/hello.txt', '2025-04-11 14:00:00', FALSE);
INSERT INTO FILE VALUES ('/files/info.txt', '2025-04-10 08:00:00', FALSE);
INSERT INTO FILE VALUES ('/files/subdir/secrets.txt', '2025-04-12 20:00:00', FALSE);