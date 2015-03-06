-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW maxHeight AS
SELECT neighbour.country AS c1id, max(height) AS height
FROM country, neighbour
WHERE country.cid = neighbour.neighbor
GROUP BY neighbour.country;

CREATE VIEW pairs AS
SELECT maxHeight.c1id AS c1id, n.cid AS c2id, n.cname AS c2name  
FROM maxHeight, (neighbour JOIN country ON neighbour.neighbor = country.cid) n
WHERE maxHeight.c1id = n.country AND maxHeight.height = n.height;

CREATE VIEW answer AS
SELECT p.c1id, c.cname AS c1name, p.c2id AS c2id, p.c2name AS c2name 
FROM country c, pairs p
WHERE c.cid = p.c1id;
 
INSERT INTO Query1(
	SELECT *
	FROM answer
	ORDER BY c1name ASC);

DROP VIEW answer;
DROP VIEW pairs;
DROP VIEW maxHeight;

-- Query 2 statements
CREATE VIEW answer AS
SELECT country.cid, country.cname
FROM country
WHERE country.cid NOT IN (
	SELECT oceanAccess.cid
	FROM oceanAccess)
ORDER BY country.cname ASC;

INSERT INTO Query2(
	SELECT *
	FROM answer);

DROP VIEW answer;

-- Query 3 statements
CREATE VIEW landLocked AS
SELECT country.cid AS cid, country.cname AS cname
FROM country
WHERE country.cid NOT IN (
	SELECT oceanAccess.cid
	FROM oceanAccess)
ORDER BY country.cname ASC;

CREATE VIEW oneNeighbour AS
(SELECT country, neighbor
FROM neighbour)
EXCEPT
(SELECT n1.country, n1.neighbor
FROM neighbour n1, neighbour n2
WHERE n1.country = n2.country AND n1.neighbor <> n2.neighbor);

CREATE VIEW lockedone AS
SELECT n.country AS c1id, n.neighbor AS c2id, l.cname AS c2name
FROM oneNeighbour n, landLocked l
WHERE n.country = l.cid;

CREATE VIEW answer AS
SELECT l.c1id AS c1id, country.cname AS c1name, l.c2id AS c2id, l.c2name AS c2name
FROM lockedone l, country
WHERE l.c1id = country.cid;

INSERT INTO Query3(
	SELECT *
	FROM answer
	ORDER BY c1name ASC);

DROP VIEW answer;
DROP VIEW lockedone;
DROP VIEW oneNeighbour;
DROP VIEW landLocked;

-- Query 4 statements
CREATE VIEW indirect AS
SELECT neighbour.country AS cid, oid
FROM oceanAccess, neighbour
WHERE neighbour.neighbor = oceanAccess.cid;

CREATE VIEW access AS
	(SELECT *
	FROM oceanAccess)
UNION (
	SELECT *
	FROM indirect);

CREATE VIEW answer AS
SELECT country.cname AS cname, ocean.oname AS oname
FROM access, country, ocean
WHERE access.cid = country.cid AND access.oid = ocean.oid;

INSERT INTO Query4(
	SELECT *
	FROM answer
	ORDER BY cname ASC, oname DESC);

DROP VIEW answer;
DROP VIEW access;
DROP VIEW indirect;

-- Query 5 statements

CREATE VIEW averageHDI AS
SELECT cid, AVG(hdi_score) AS avghdi
FROM hdi
WHERE year >= 2009 AND year <= 2013
GROUP BY cid;

CREATE VIEW answer AS
SELECT country.cid, country.cname, averageHDI.avghdi
FROM averageHDI, country
WHERE averageHDI.cid = country.cid
ORDER BY avghdi DESC;

INSERT INTO Query5(
	SELECT *
	FROM answer
	ORDER BY avghdi DESC 
	LIMIT 10);

DROP VIEW answer;
DROP VIEW averageHDI;

-- Query 6 statements

CREATE VIEW hdi2009 AS
SELECT cid, year, hdi_score 
FROM hdi
WHERE year = 2009;

CREATE VIEW hdi2010 AS
SELECT cid, year, hdi_score 
FROM hdi
WHERE year = 2010;

CREATE VIEW hdi2011 AS
SELECT cid, year, hdi_score 
FROM hdi
WHERE year = 2011;

CREATE VIEW hdi2012 AS
SELECT cid, year, hdi_score 
FROM hdi
WHERE year = 2012;

CREATE VIEW hdi2013 AS
SELECT cid, year, hdi_score 
FROM hdi
WHERE year = 2013;

CREATE VIEW answer AS
SELECT increasingHDI.cid AS cid, country.cname AS cname 
FROM country, (
	SELECT hdi2009.cid
	FROM hdi2009, hdi2010, hdi2011, hdi2012, hdi2013 
	WHERE hdi2009.cid = hdi2010.cid AND hdi2009.hdi_score < hdi2010.hdi_score AND 
		hdi2010.cid = hdi2011.cid AND hdi2010.hdi_score < hdi2011.hdi_score AND 
		hdi2011.cid = hdi2012.cid AND hdi2011.hdi_score < hdi2012.hdi_score AND 
		hdi2012.cid = hdi2013.cid AND hdi2012.hdi_score < hdi2013.hdi_score 
	GROUP BY hdi2009.cid) AS increasingHDI 
WHERE country.cid = increasingHDI.cid;

INSERT INTO Query6(
	SELECT *
	FROM answer
	ORDER by cname ASC);

DROP VIEW answer;
DROP VIEW hdi2013;
DROP VIEW hdi2012;
DROP VIEW hdi2011;
DROP VIEW hdi2010;
DROP VIEW hdi2009;

-- Query 7 statements
CREATE VIEW answer AS
SELECT religion.rid AS rid, religion.rname AS rname, 
	SUM(religion.rpercentage * country.population) AS followers 
FROM religion JOIN country ON religion.cid = country.cid
GROUP BY religion.rid, religion.rname;

INSERT INTO Query7(
	SELECT *
	FROM answer
	ORDER BY followers DESC);

DROP VIEW answer;

-- Query 8 statements

CREATE VIEW mostPopular AS
SELECT country.cid AS cid, country.cname AS cname, language.lname, MAX(
	lpercentage * population) AS junk
FROM country JOIN language ON country.cid = language.cid
GROUP BY country.cid, country.cname, language.lname;

CREATE VIEW answer AS
SELECT p1.cname AS c1name, p2.cname AS c2name, p1.lname AS lname
FROM mostPopular p1, mostPopular p2, neighbour n1
WHERE p1.lname = p2.lname AND p1.cid = n1.country AND p2.cid = n1.neighbor;

INSERT INTO Query8(
	SELECT *
	FROM answer
	ORDER BY lname ASC, c1name DESC);

DROP VIEW answer;
DROP VIEW mostPopular;

-- Query 9 statements
CREATE VIEW countryAccess AS
SELECT oceanAccess.cid AS cid, MAX(country.height + ocean.depth) AS totalSpan 
FROM country, ocean, oceanAccess
WHERE oceanAccess.cid = country.cid AND oceanAccess.oid = ocean.oid
GROUP BY oceanAccess.cid;

CREATE VIEW countryNoAccess AS
SELECT country.cname AS cname, country.height AS totalSpan
FROM country
WHERE country.cid NOT IN (
	SELECT oceanAccess.cid AS cid
	FROM oceanAccess);

CREATE VIEW answer AS
(SELECT * FROM countryAccess)
UNION 
(SELECT * FROM countryNoAccess);

INSERT INTO Query9(
	SELECT *
	FROM answer
	LIMIT 1);

DROP VIEW answer;
DROP VIEW countryNoAccess;
DROP VIEW countryAccess;

-- Query 10 statements
CREATE VIEW answer AS
SELECT country.cname AS cname, SUM(neighbour.length) AS borderslength
FROM country, neighbour
WHERE country.cid = neighbour.country
GROUP BY country.cid
ORDER BY borderslength DESC;


INSERT INTO Query10(
	SELECT *
	FROM answer
	LIMIT 1);

DROP VIEW answer;
