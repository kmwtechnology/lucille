-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE fileroot(name VARCHAR(200), last_modified TIMESTAMP, is_directory BIT)

INSERT INTO fileroot VALUES ("/files/", NULL, TRUE)
INSERT INTO fileroot VALUES ("/files/subdir/", NULL, TRUE)

INSERT INTO fileroot VALUES ("/hello.txt", '2025-04-11 14:00:00', FALSE)
INSERT INTO fileroot VALUES ("/files/info.txt", '2025-04-10 08:00:00', FALSE)
INSERT INTO fileroot VALUES ("/files/subdir/secrets.txt", '2025-04-12 20:00:00', FALSE)