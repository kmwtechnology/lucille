-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE FILE(name VARCHAR(200) PRIMARY KEY, last_published TIMESTAMP, is_directory BOOLEAN, parent VARCHAR(200));

INSERT INTO FILE VALUES ('/', NULL, TRUE, NULL);
INSERT INTO FILE VALUES ('/files/', NULL, TRUE, '/');
INSERT INTO FILE VALUES ('/files/subdir/', NULL, TRUE, '/files/');

INSERT INTO FILE VALUES ('/hello.txt', '2025-04-11 14:00:00', FALSE, '/');
INSERT INTO FILE VALUES ('/files/info.txt', '2025-04-10 08:00:00', FALSE, '/files/');
INSERT INTO FILE VALUES ('/files/subdir/secrets.txt', '2025-04-12 20:00:00', FALSE, '/files/subdir/');

CREATE INDEX FILEparent ON FILE(parent);