DROP DATABASE IF EXISTS testdb;
CREATE DATABASE testdb;
USE testdb;

CREATE TABLE t (i BIGINT(20), rowId BIGINT AUTO_INCREMENT, d DOUBLE, d2 DOUBLE, t TEXT, t2 TEXT, SORT KEY (rowId));

INSERT INTO t (i, d, d2, t, t2) VALUES (1,  3.4, 5.6, 'abc', 'de');
INSERT INTO t (i, d, d2, t, t2) VALUES (2,  4.5, 6.7, 'a', 'b');
INSERT INTO t (i, d, d2, t, t2) VALUES (3,  5.6, 7.8, '3x', 'c');
INSERT INTO t (i, d, d2, t, t2) VALUES (4,  6.7, 8.9, '4x', 'cc');
INSERT INTO t (i, d, d2, t, t2) VALUES (5, -7.8, 0.01, 'xxxxx', 'ccc');
INSERT INTO t (i, d, d2, t, t2) VALUES (6, -8.9, 0.001, 'xxxxx', 'cccc');
INSERT INTO t (i, d, d2, t, t2) VALUES (7,  5.1, 0.001, 'xxxxx', 'ccccc');
INSERT INTO t (i, d, d2, t, t2) VALUES (8,  5.11, 0.0001, 'xxxxx', 'cccccc');
INSERT INTO t (i, d, d2, t, t2) VALUES (9,  5.111, 9.8, 'xxxxx', 'cccccccc');
INSERT INTO t (i, d, d2, t, t2) VALUES (10, 5.1111, 10.8, 'xxxxx', 'cccccccc');
INSERT INTO t (i, d, d2, t, t2) VALUES (11, 5.11111, 11.8, 'xxxxx', 'ccccccccc');
INSERT INTO t (i, d, d2, t, t2) VALUES (12, 5.111111, 12.8, 'xxxxx', 'cccccccccc');
INSERT INTO t (i, d, d2, t, t2) VALUES (13, 5.1111111, 12.8, 'xxxxx', 'ccccccccc');
INSERT INTO t (i, d, d2, t, t2) VALUES (NULL, NULL, NULL, NULL, NULL, NULL);

SELECT *, partition_id() FROM t;
