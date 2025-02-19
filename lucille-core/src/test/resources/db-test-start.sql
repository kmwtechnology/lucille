-- noinspection SqlNoDataSourceInspectionForFile

CREATE TABLE animal(id INT, name VARCHAR(10), type VARCHAR(10), birthday DATE);
INSERT INTO animal VALUES (1, 'Matt', 'Human', '2001-07-30');
INSERT INTO animal VALUES (2, 'Sonny', 'Cat', '2024-07-30');
INSERT INTO animal VALUES (3, 'Blaze', 'Cat', '2024-05-30');

CREATE TABLE network(id INT, name VARCHAR(10), friends_with VARCHAR(10));
INSERT INTO network VALUES (1, 'Sonny', 'Matt');
INSERT INTO network VALUES (2, 'Sonny', 'Blaze');
INSERT INTO network VALUES (3, 'Matt', 'Bob');
INSERT INTO network VALUES (4, 'Matt', 'Sonny');
INSERT INTO network VALUES (5, 'Matt', 'Blaze');
INSERT INTO network VALUES (6, 'Blaze', 'Matt');

CREATE TABLE nonComparable (id INT, name VARCHAR(10), metadata JSON);
INSERT INTO nonComparable VALUES (1, 'Sonny', '{"age": 25, "city": "NY"}');
INSERT INTO nonComparable VALUES (2, 'Blaze', '{"age": 30, "city": "LA"}');

CREATE TABLE adopted(id INT, name VARCHAR(10), adopted_on DATE);
INSERT INTO adopted VALUES (1, 'Sonny', '2024-07-30');
INSERT INTO adopted VALUES (2, 'Blaze', '2024-07-30');

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

CREATE TABLE companies(company_id VARCHAR, name VARCHAR);
INSERT INTO companies VALUES ('1-1', 'Acme');
INSERT INTO companies VALUES ('1-2', NULL);

CREATE TABLE mixed(id VARCHAR, int_field INT, bool_field BIT);
INSERT INTO mixed VALUES ('1', 3, 1);
INSERT INTO mixed VALUES ('2', 4, 0);

create table table_with_id_column(id int, `value` int, other_id varchar(10));
insert into table_with_id_column values (1, 1, 'id1');
insert into table_with_id_column values (2, 2, 'id2');

CREATE TABLE test_data_types (
    id INT PRIMARY KEY AUTO_INCREMENT,
    varchar_col VARCHAR(255),
    char_col CHAR(10),
    longvarchar_col LONGVARCHAR,
    nchar_col NCHAR(10),
    nvarchar_col NVARCHAR(255),
    longnvarchar_col LONGNVARCHAR,
    clob_col CLOB,
    nclob_col NCLOB,
    tinyint_col TINYINT,
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    double_col DOUBLE,
    decimal_col DECIMAL,
    numeric_col NUMERIC,
    float_col FLOAT,
    real_col REAL,
    boolean_col BOOLEAN,
    bit_col BIT,
    date_col DATE,
    timestamp_col TIMESTAMP,
    blob_col BLOB,
    binary_col BINARY(100),
    varbinary_col VARBINARY(255),
    longvarbinary_col LONGVARBINARY,
    nullable_int INTEGER,
    nullable_varchar VARCHAR(255),
    nullable_date DATE
);

INSERT INTO test_data_types (
    varchar_col, char_col, longvarchar_col, nchar_col, nvarchar_col, longnvarchar_col, clob_col, nclob_col,
    tinyint_col, smallint_col, integer_col, bigint_col, double_col, decimal_col, numeric_col, float_col, real_col,
    boolean_col, bit_col, date_col, timestamp_col, blob_col, binary_col, varbinary_col, longvarbinary_col,
    nullable_int, nullable_varchar, nullable_date
) VALUES (
    'Test VARCHAR', 'CHAR Test', 'Long VARCHAR test data', '😀', 'こんにちは、世界！', 'こんにちは、世界！長いテキストのテストです。', 'test clob', 'test nclob',
    127, 32767, 2147483647, 9223372036854775807, 3.14159265359, 9876.54, 1.0000, 2.71828, 1.414214,
    TRUE, 1, '2024-07-30', '1970-01-01 00:00:01', 'This is a test blob.', X'BEEFBEEF', X'0123456789ABCDEF', X'0011223344556677889900',
    NULL, NULL, NULL
);


INSERT INTO test_data_types (
    varchar_col, char_col, longvarchar_col, nchar_col, nvarchar_col, longnvarchar_col, clob_col, nclob_col,
    tinyint_col, smallint_col, integer_col, bigint_col, double_col, decimal_col, numeric_col, float_col, real_col,
    boolean_col, bit_col, date_col, timestamp_col, blob_col, binary_col, varbinary_col, longvarbinary_col,
    nullable_int, nullable_varchar, nullable_date
) VALUES (
    'Another', 'Test', 'More long text here', '😀', '안녕하세요, 세계!', '안녕하세요, 세계! 긴 텍스트 테스트입니다.', 'test clob', 'test nclob',
    -128, -32768, -2147483648, -9223372036854775808, 1.41421356237, 500.00, 100000, 1.61803, 3.141592,
    FALSE, 0, '2023-01-01', '2038-01-19 03:14:07', 'This is a test blob2.', X'BEEFBEEF', X'FEDCBA9876543210', X'AABBCCDDEEFF00112233',
    NULL, NULL, NULL
);


CREATE TABLE test_time_type (
    id INT PRIMARY KEY AUTO_INCREMENT,
    time_col TIME
);

INSERT INTO test_time_type (
    time_col
) VALUES (
    '00:00:00.0000000'
);

CREATE TABLE time_with_timezone_type (
    id INT PRIMARY KEY AUTO_INCREMENT,
    time_w_timezone_col TIME WITH TIME ZONE
);

INSERT INTO time_with_timezone_type (
    time_w_timezone_col
) VALUES (
    '10:15:30.334-03:30'
);