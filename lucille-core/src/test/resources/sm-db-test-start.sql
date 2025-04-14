-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE file(name VARCHAR(200) PRIMARY KEY, last_published TIMESTAMP, is_directory BIT, parent VARCHAR(200));

INSERT INTO file VALUES ('/', NULL, 1, NULL);
INSERT INTO file VALUES ('/files/', NULL, 1, '/');
INSERT INTO file VALUES ('/files/subdir/', NULL, 1, '/files/');

INSERT INTO file VALUES ('/hello.txt', '2025-04-11 14:00:00', 0, '/');
INSERT INTO file VALUES ('/files/info.txt', '2025-04-10 08:00:00', 0, '/files/');
INSERT INTO file VALUES ('/files/subdir/secrets.txt', '2025-04-12 20:00:00', 0, '/files/subdir/');

CREATE INDEX parent ON file(parent);