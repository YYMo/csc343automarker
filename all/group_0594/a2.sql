-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

DROP TABLE IF EXISTS Query1;
DROP TABLE IF EXISTS Query2;
DROP TABLE IF EXISTS Query3;
DROP TABLE IF EXISTS Query4;
DROP TABLE IF EXISTS Query5;
DROP TABLE IF EXISTS Query6;
DROP TABLE IF EXISTS Query7;
DROP TABLE IF EXISTS Query8;
DROP TABLE IF EXISTS Query9;
DROP TABLE IF EXISTS Query10;

CREATE TABLE Query1(
        c1id    INTEGER,
    c1name      VARCHAR(20),
        c2id    INTEGER,
    c2name      VARCHAR(20)
);

CREATE TABLE Query2(
        cid             INTEGER,
    cname       VARCHAR(20)
);

CREATE TABLE Query3(
        c1id    INTEGER,
    c1name      VARCHAR(20),
        c2id    INTEGER,
    c2name      VARCHAR(20)
);

CREATE TABLE Query4(
        cname   VARCHAR(20),
    oname       VARCHAR(20)
);

CREATE TABLE Query5(
        cid             INTEGER,
    cname       VARCHAR(20),
        avghdi  REAL
);

CREATE TABLE Query6(
        cid             INTEGER,
    cname       VARCHAR(20)
);

CREATE TABLE Query7(
        rid                     INTEGER,
    rname               VARCHAR(20),
        followers       INTEGER
);

CREATE TABLE Query8(
        c1name  VARCHAR(20),
    c2name      VARCHAR(20),
        lname   VARCHAR(20)
);

CREATE TABLE Query9(
    cname               VARCHAR(20),
        totalspan       INTEGER
);

CREATE TABLE Query10(
    cname                       VARCHAR(20),
        borderslength   INTEGER
);

-- Assume a2.ddl file has been read and excuted in psql before this file is executed.
-- Query 1 statements

CREATE VIEW heights AS 
SELECT neighbour.country AS c1id, country.cname AS c1name, neighbour.neighbor AS c2id, country.height AS height
FROM neighbour, country
WHERE neighbour.neighbor = country.cid; 

CREATE VIEW max_heights AS
SELECT c1id, max(height) AS height
FROM heights
GROUP BY c1id;

INSERT INTO Query1 (
SELECT heights.c1id AS c1id, heights.c1name AS c1name, heights.c2id AS c2id, country.cname AS c2name
FROM heights, max_heights, country
WHERE heights.c1id = max_heights.c1id AND 
heights.height = max_heights.height AND 
heights.c1id = country.cid
ORDER BY c1name ASC
);

-- Query 2 statements

INSERT INTO Query2 (
SELECT cid, cname
FROM country
WHERE country.cid NOT IN (SELECT cid FROM oceanAccess)
ORDER BY cname ASC
);

-- Query 3 statements

CREATE VIEW landlocked_neighbours AS
SELECT Query2.cid AS c1id, Query2.cname AS c1name
FROM Query2, neighbour
WHERE Query2.cid = neighbour.country
GROUP BY Query2.cid, Query2.cname
HAVING count(neighbour.neighbor) = 1;

INSERT INTO Query3 (
SELECT landlocked_neighbours.c1id AS c1id, landlocked_neighbours.c1name AS c1name, neighbour.neighbor AS c2id, country.cname AS c2name
FROM landlocked_neighbours, neighbour, country
WHERE neighbour.country = landlocked_neighbours.c1id
AND neighbour.neighbor = country.cid
ORDER BY c1name ASC
);

-- Query 4 statements

CREATE VIEW directOcean AS
SELECT country.cname AS cname, ocean.oname AS oname
FROM country, oceanAccess, ocean
WHERE country.cid = oceanAccess.cid
AND oceanAccess.oid = ocean.oid;

CREATE VIEW indirectOcean AS
SELECT country.cname AS cname, ocean.oname AS oname
FROM country, neighbour, ocean, oceanAccess  
WHERE country.cid = neighbour.country
AND neighbour.neighbor = oceanAccess.cid
AND oceanAccess.oid = ocean.oid;

INSERT INTO Query4(
SELECT * FROM directOcean UNION SELECT * FROM indirectOcean
ORDER BY cname ASC, oname DESC
);


-- Query 5 statements

CREATE VIEW range AS
SELECT hdi.cid, country.cname ,avg(hdi_score) AS avghdi
FROM hdi, country
WHERE year <= 2013 AND year >= 2009 AND country.cid = hdi.cid
GROUP BY hdi.cid, country.cname;

CREATE VIEW ranked AS 
SELECT rank, cid, cname, avghdi
FROM (SELECT T1.avghdi, T1.cid, T1.cname, 
	(SELECT COUNT(T2.avghdi) FROM range T2 
		WHERE T1.avghdi <= T2.avghdi) AS rank
	FROM range T1) AS TopTen
	WHERE rank < 11
	ORDER BY rank;

INSERT INTO Query5(
SELECT cid, cname, avghdi 
FROM ranked
);
-- Query 6 statements
CREATE VIEW countryhdi AS 
SELECT country.cid, country.cname, hdi.hdi_score, year
FROM country, hdi
WHERE country.cid = hdi.cid
AND year >= 2009 AND year <=2013;

CREATE VIEW notincreasing AS
SELECT T1.cid
FROM countryhdi T1, countryhdi T2
WHERE T2.hdi_score <= T1.hdi_score AND T2.year > T1.year AND T1.cid = T2.cid;

CREATE VIEW notchanging AS
SELECT cid
FROM hdi
WHERE year >=2009 AND year <=2013
GROUP BY cid
HAVING count(hdi_score) = 1;

INSERT INTO Query6(
SELECT DISTINCT countryhdi.cid, countryhdi.cname
FROM countryhdi
WHERE countryhdi.cid NOT IN (SELECT * FROM notincreasing)
AND countryhdi.cid NOT IN (SELECT * FROM notchanging)
ORDER BY cname ASC
);
-- Query 7 statements

CREATE VIEW precise AS
SELECT rid, rname,(population * (rpercentage / 100)) AS followers 
FROM country, religion
WHERE country.cid = religion.cid;

INSERT INTO Query7(
SELECT rid, rname, SUM(followers) 
FROM precise 
GROUP BY rname, rid 
ORDER BY SUM(followers) DESC
);


-- Query 8 statements
CREATE VIEW notpoplanguage AS
SELECT T1.cid, T1.lname
FROM language T1, language T2
WHERE T1.lpercentage < T2.lpercentage
AND T1.cid = T2.cid;

CREATE VIEW poplanguage AS
(SELECT cid, lname FROM language) EXCEPT (SELECT * FROM notpoplanguage);

CREATE VIEW almostfinal AS
SELECT T1.cid AS c1cid, T2.cid AS c2cid, T2.lname
FROM poplanguage T1, poplanguage T2, neighbour, country
WHERE T1.cid = neighbour.country AND T2.cid = neighbour.neighbor 
AND T1.lname = T2.lname;

INSERT INTO Query8(
SELECT DISTINCT T1.cname AS c1name, T2.cname AS c2name, lname
FROM almostfinal, country T1, country T2
WHERE c1cid = T1.cid AND c2cid = T2.cid
ORDER BY c1name DESC
);

-- Query 9 statements
CREATE VIEW hasDirectAccess AS
SELECT depth, cid FROM ocean, oceanAccess
WHERE ocean.oid = oceanAccess.oid;

CREATE VIEW noDirectAccess AS
(SELECT cid FROM country) EXCEPT (SELECT cid FROM hasDirectAccess);

CREATE VIEW finalAnswer AS
(SELECT cname, max(height + depth) AS totalspan 
FROM hasDirectAccess, country
WHERE hasDirectAccess.cid = country.cid
GROUP BY cname)
UNION
(SELECT cname, max(height) AS totalspan
FROM noDirectAccess, country
WHERE noDirectAccess.cid = country.cid
GROUP BY cname);

INSERT INTO Query9(
SELECT cname, totalspan
FROM finalAnswer WHERE totalspan = (select  max(totalspan) from finalAnswer)
);

-- Query 10 statements
INSERT INTO Query10(
select cname, borderslength 
FROM country, 
(select country AS cid, SUM(length) AS borderslength 
FROM neighbour GROUP BY country 
ORDER BY borderslength DESC LIMIT 1) cidBased
WHERE country.cid = cidBased.cid
);

DROP VIEW finalAnswer;
DROP VIEW noDirectAccess;
DROP VIEW hasDirectAccess;
DROP VIEW almostfinal;
DROP VIEW poplanguage;
DROP VIEW notpoplanguage;
DROP VIEW precise;
DROP VIEW notchanging;
DROP VIEW notincreasing;
DROP VIEW countryhdi;
DROP VIEW ranked;
DROP VIEW range;
DROP VIEW directOcean;
DROP VIEW indirectOcean;
DROP VIEW max_heights;
DROP VIEW heights;
DROP VIEW landlocked_neighbours;
