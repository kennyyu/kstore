CREATE DATABASE IF NOT EXISTS p4challenge;
USE p4challenge;

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS u;

CREATE TABLE IF NOT EXISTS t (
a INTEGER,
b INTEGER,
c INTEGER,
d INTEGER);

CREATE TABLE IF NOT EXISTS u (
a INTEGER,
b INTEGER,
c INTEGER,
d INTEGER);

LOAD DATA LOCAL INFILE 't.csv' INTO TABLE t
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(a,b,c,d);

LOAD DATA LOCAL INFILE 'u.csv' INTO TABLE t
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(a,b,c,d);

SELECT avg(t.d), avg(u.c)
FROM t,u
WHERE t.a = u.a
        AND t.b = 1000
        AND u.d BETWEEN 9000 AND 9000000;

-- (575250,498194)

INSERT INTO t(a,b,c,d) VALUES (412234,123231,54323,764543),
(1312312,5321234,3453241,14232342),
(39485394,123124,23429,1293),
(234231,1000,23425,21234),
(30293,123901,23491,12303);

SELECT avg(t.d), avg(u.c)
FROM t,u
WHERE t.a = u.a
        AND t.b = 1000
        AND u.d BETWEEN 9000 AND 9000000;

-- (536077,507767)

DELETE FROM t WHERE d < 100000;

SELECT avg(t.d), avg(u.c)
FROM t,u
WHERE t.a = u.a
        AND t.b = 1000
        AND u.d BETWEEN 9000 AND 9000000;

-- (618527,499023)

UPDATE u SET c = 10000 WHERE b > 500000;

SELECT avg(t.d), avg(u.c)
FROM t,u
WHERE t.a = u.a
        AND t.b = 1000
        AND u.d BETWEEN 9000 AND 9000000;

-- (618527,238469)

DELETE FROM u WHERE a BETWEEN 100000 AND 200000;

SELECT avg(t.d), avg(u.c)
FROM t,u
WHERE t.a = u.a
        AND t.b = 1000
        AND u.d BETWEEN 9000 AND 9000000;

-- (618527,238469)

UPDATE t SET a = 54321 WHERE c BETWEEN 10000 AND 200000;

SELECT avg(t.d), avg(u.c)
FROM t,u
WHERE t.a = u.a
        AND t.b = 1000
        AND u.d BETWEEN 9000 AND 9000000;

-- (593609,244244)