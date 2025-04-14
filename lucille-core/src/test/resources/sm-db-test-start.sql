-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE file(name VARCHAR(200), last_modified TIMESTAMP, is_directory BIT);

INSERT INTO file VALUES ('/files/', NULL, 1);
INSERT INTO file VALUES ('/files/subdir/', NULL, 1);

INSERT INTO file VALUES ('/hello.txt', '2025-04-11 14:00:00', 0);
INSERT INTO file VALUES ('/files/info.txt', '2025-04-10 08:00:00', 0);
INSERT INTO file VALUES ('/files/subdir/secrets.txt', '2025-04-12 20:00:00', 0);