-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE OR REPLACE VIEW NeighbourNamesHeight AS
(SELECT A.country AS c1id, B.cname As c1name, A.neighbor AS c2id, C.cname AS c2name, C.height AS height
FROM neighbour AS A, country AS B, country AS C 
WHERE A.country = B.cid AND A.neighbor = C.cid);

CREATE OR REPLACE VIEW Q1a AS (	
SELECT c1id, max(height) AS maxHeight
FROM NeighbourNamesHeight
GROUP BY c1id);

INSERT INTO Query1(
SELECT A.c1id AS c1id, A.c1name As c1name, A.c2id AS c2id, A.c2name AS c2name
FROM NeighbourNamesHeight As A, Q1a AS B
WHERE A.c1id = B.c1id AND A.height = B.maxHeight
ORDER BY A.c1name ASC
);

DROP VIEW Q1a;
DROP VIEW NeighbourNamesHeight;
-- Query 2 statements 
INSERT INTO Query2(
SELECT cid, cname
FROM country
WHERE cid NOT IN 
(SELECT cid FROM oceanAccess)
ORDER BY cname ASC
);

-- Query 3 statements
CREATE OR REPLACE VIEW LandlockedCountry AS (
SELECT cid, cname
FROM country
WHERE cid NOT IN 
(SELECT cid FROM oceanAccess)
ORDER BY cname ASC
);

CREATE OR REPLACE VIEW LandlockedCountryNeighbour AS
(SELECT A.country AS c1id, B.cname As c1name, A.neighbor AS c2id, C.cname AS c2name
FROM neighbour AS A, LandlockedCountry AS B, country AS C 
WHERE A.country = B.cid AND A.neighbor = C.cid);

CREATE OR REPLACE VIEW LandlockedCountryNeighbourCount AS
( SELECT c1id, count(c2id) AS ncount
FROM LandlockedCountryNeighbour
GROUP BY c1id
);

INSERT INTO Query3
( SELECT A.c1id AS c1id, c1name, c2id, c2name
FROM LandlockedCountryNeighbourCount AS A, LandlockedCountryNeighbour AS B
WHERE A.ncount = 1 AND A.c1id = B.c1id
ORDER BY c1name ASC
);

DROP VIEW LandlockedCountryNeighbourCount;
DROP VIEW LandlockedCountryNeighbour;
DROP VIEW LandlockedCountry;

-- Query 4 statements
CREATE OR REPLACE VIEW NeighbourNamesOcean AS
(SELECT A.country AS c1id, B.cname AS c1name, D.oid AS oid, E.oname AS oname, A.neighbor AS c2id, C.cname AS c2name
FROM neighbour AS A, country AS B, country AS C, oceanAccess AS D, ocean AS E
WHERE A.country = B.cid AND A.country = D.cid AND D.oid = E.oid AND A.neighbor = C.cid);

CREATE OR REPLACE VIEW IndirectOceanAccessCountry AS
(SELECT c2name AS cname, oname
FROM NeighbourNamesOcean);

CREATE OR REPLACE VIEW DirectOceanAccessCountry AS
(SELECT c1name AS cname, oname
FROM NeighbourNamesOcean);

-- UNION (IndirectOceanAccessCountry, DirectOceanAccessCountry)
INSERT INTO Query4
(
(SELECT c2name AS cname, oname
FROM NeighbourNamesOcean) 
UNION
(SELECT c1name AS cname, oname
FROM NeighbourNamesOcean)
ORDER BY cname ASC, oname DESC);

DROP VIEW IndirectOceanAccessCountry;
DROP VIEW DirectOceanAccessCountry;
DROP VIEW NeighbourNamesOcean;

-- Query 5 statements
CREATE OR REPLACE VIEW AverageHDICountry AS
(SELECT A.cid AS cid, cname, AVG(hdi_score) AS avghdi
FROM country AS A, hdi AS B
WHERE A.cid = B.cid and B.year >= 2009 AND B.year <= 2013
GROUP BY A.cid
ORDER BY avghdi DESC
);

INSERT INTO Query5
(SELECT *
FROM AverageHDICountry
LIMIT 10
);

DROP VIEW AverageHDICountry;

-- Query 6 statements
CREATE OR REPLACE VIEW HDICountry AS
(SELECT A.cid AS cid, A.hdi_score AS hdi2009, B.hdi_score AS hdi2010, C.hdi_score AS hdi2011, D.hdi_score AS hdi2012, E.hdi_score AS hdi2013
FROM hdi AS A, hdi AS B, hdi AS C, hdi AS D, hdi AS E
WHERE A.cid = B.cid AND B.cid = C.cid AND C.cid = D.cid AND D.cid = E.cid 
	AND A.year = 2009 AND B.year = 2010 AND C.year = 2011 AND D.year = 2012 AND E.year = 2013);

INSERT INTO Query6
(SELECT A.cid AS cid, cname
FROM HDICountry AS A, country AS B
WHERE A.cid = B.cid AND hdi2013 > hdi2012 AND hdi2012 > hdi2011 AND hdi2011 > hdi2010 AND hdi2010 > hdi2009
ORDER BY cname ASC);

DROP VIEW HDICountry;

-- Query 7 statements
CREATE OR REPLACE VIEW ReligionCountryPop AS
(SELECT rid, rname, rpercentage, A.cid AS cid, population, rpercentage * population AS rpopulation
FROM religion AS A, country AS B
WHERE A.cid = B.cid);

INSERT INTO Query7
(SELECT rid, rname, sum(rpopulation) AS followers
FROM ReligionCountryPop
GROUP BY rid, rname
ORDER BY followers DESC);

DROP VIEW ReligionCountryPop;

-- Query 8 statements
CREATE OR REPLACE VIEW PopularLanguagePercentage AS
(SELECT cid, max(lpercentage) AS maxLPercentage
FROM language
GROUP BY cid);

CREATE OR REPLACE VIEW PopularLanguage AS
(SELECT A.cid AS cid, lid, lname
FROM PopularLanguagePercentage AS A, language AS B
WHERE A.cid = B.cid AND A.maxLPercentage = B.lpercentage);

CREATE OR REPLACE VIEW PopularLanguageNeighbour AS
(SELECT C.country AS c1id, A.lid AS l1id, A.lname AS l1name, C.neighbor AS c2id, B.lid AS l2id, B.lname AS l2name
FROM PopularLanguage AS A, PopularLanguage AS B, neighbour AS C
WHERE A.cid = C.country AND B.cid = C.neighbor);

INSERT INTO Query8
(SELECT B.cname AS c1name, C.cname AS c2name, l1name AS lname
FROM PopularLanguageNeighbour AS A, country AS B, country AS C
WHERE l1name = l2name AND B.cid = A.c1id AND C.cid = A.c2id
ORDER BY lname ASC, c1name DESC);

DROP VIEW PopularLanguageNeighbour;
DROP VIEW PopularLanguage;
DROP VIEW PopularLanguagePercentage;

-- Query 9 statements
CREATE OR REPLACE VIEW CountryMaxSpan AS
(
(SELECT cid, cname, height AS span
FROM country)
UNION
(SELECT cid, cname, max(span) AS span
FROM 
(SELECT A.cid AS cid, cname, height+depth AS span
FROM country AS A, oceanAccess AS B, ocean AS C
WHERE A.cid = B.cid AND B.oid = C.oid) AS D
GROUP BY cid, cname));

CREATE OR REPLACE VIEW MaxSpan AS
(SELECT max(span) AS totalspan
FROM CountryMaxSpan);

INSERT INTO Query9
(SELECT cname, totalspan
FROM CountryMaxSpan AS A, MaxSpan AS B
WHERE A.span = B.totalspan);

DROP VIEW MaxSpan;
DROP VIEW CountryMaxSpan;

-- Query 10 statements
CREATE OR REPLACE VIEW LongestBorderCountry AS
(SELECT country, sum(length) AS borderslength
FROM neighbour
GROUP BY country
ORDER BY borderslength DESC
LIMIT 1);

INSERT INTO Query10
(SELECT B.cname, borderslength
FROM LongestBorderCountry AS A, country AS B
WHERE A.country = B.cid);

DROP VIEW LongestBorderCountry;

