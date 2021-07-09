DROP DATABASE IF EXISTS testdb;
CREATE DATABASE testdb;
USE testdb;

CREATE TABLE t (i BIGINT(20), i2 BIGINT(20), d DOUBLE, d2 DOUBLE, t TEXT, t2 TEXT);

INSERT INTO t VALUES (1, -22, 3.4, 5.6, 'abc', 'de');
INSERT INTO t VALUES (2, -33, 4.5, 6.7, 'a', 'b');
INSERT INTO t VALUES (3, -44, 5.6, 7.8, '3x', 'c');
INSERT INTO t VALUES (4, -55, 6.7, 8.9, '4x', 'cc');
INSERT INTO t VALUES (5, -66, -7.8, 0.01, 'xxxxx', 'ccc');
INSERT INTO t VALUES (6, -77, -8.9, 0.001, 'xxxxx', 'cccc');
INSERT INTO t VALUES (7, -88, 5.1, 0.001, 'xxxxx', 'ccccc');
INSERT INTO t VALUES (8, -99, 5.11, 0.0001, 'xxxxx', 'cccccc');
INSERT INTO t VALUES (9, 101, 5.111, 9.8, 'xxxxx', 'cccccccc');
INSERT INTO t VALUES (10, 202, 5.1111, 10.8, 'xxxxx', 'cccccccc');
INSERT INTO t VALUES (11, 303, 5.11111, 11.8, 'xxxxx', 'ccccccccc');
INSERT INTO t VALUES (12, 404, 5.111111, 12.8, 'xxxxx', 'cccccccccc');
INSERT INTO t VALUES (13, 505, 5.1111111, 12.8, 'xxxxx', 'ccccccccc');

SELECT *, partition_id() FROM t;
