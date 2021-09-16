-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE animal(id INT, name VARCHAR(10), type VARCHAR(10));
INSERT INTO animal VALUES (1, 'Matt', 'Human');
INSERT INTO animal VALUES (2, 'Sonny', 'Cat');
INSERT INTO animal VALUES (3, 'Blaze', 'Cat');

CREATE TABLE meal(id INT, animal_id INT, name VARCHAR(20));
INSERT INTO meal VALUES (1, 1, 'breakfast');
INSERT INTO meal VALUES (2, 1, 'lunch');
INSERT INTO meal VALUES (3, 1, 'dinner');
INSERT INTO meal VALUES (4, 2, 'breakfast');
INSERT INTO meal VALUES (5, 2, 'dinner');
INSERT INTO meal VALUES (6, 3, 'breakfast');
INSERT INTO meal VALUES (7, 3, 'dinner');

CREATE TABLE attribute(id INT, name VARCHAR(15));
INSERT INTO attribute VALUES (1, 'weight');
INSERT INTO attribute VALUES (2, 'hair_color');

CREATE TABLE data(id INT, animal_id INT, attr_id INT, val VARCHAR(10));
INSERT INTO data VALUES (1, 1, 1, '165');
INSERT INTO data VALUES (2, 1, 2, 'brown');
INSERT INTO data VALUES (3, 2, 1, '12');
INSERT INTO data VALUES (4, 2, 2, 'tiger');
INSERT INTO data VALUES (5, 3, 1, '12');
INSERT INTO data VALUES (6, 3, 2, 'white');