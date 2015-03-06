-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW NeighbourDetails AS 
	SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name, c2.height AS c2height
	FROM country c1, country c2, neighbour 
	WHERE c1.cid = country AND c2.cid = neighbor;

CREATE VIEW HighestNeighbour AS 
	SELECT c1id, max(c2height) AS maxheight
	FROM NeighbourDetails 
	GROUP BY c1id;

INSERT INTO Query1 (
	SELECT n.c1id AS c1id, n.c1name AS c1name, n.c2id AS c2id, n.c2name AS c2name 
	FROM NeighbourDetails n, HighestNeighbour h
	WHERE n.c1id = h.c1id AND n.c2height = h.maxheight
	ORDER BY c1name ASC);

DROP VIEW HighestNeighbour;
DROP VIEW NeighbourDetails;

-- Query 2 statements
INSERT INTO Query2 ( 
	SELECT cid, cname 
	FROM country 
	WHERE cid NOT IN ( 
		SELECT cid FROM oceanAccess) 
	GROUP BY cid, cname 
	ORDER BY cname ASC);

-- Query 3 statements
INSERT INTO Query3 (
	SELECT Query2.cid AS c1id, Query2.cname AS c1name, C1.cid AS c2id, C2.cname AS c2name 
	FROM Query2, neighbour N1, country C1 , country C2
	WHERE N1.country = Query2.cid AND C1.cid = N1.neighbor 
	GROUP BY Query2.cid, Query2.cname, C1.cid, C2.cname 
	HAVING count(N1.neighbor) = 1 
	ORDER BY c1name ASC);

-- Query 4 statements
CREATE VIEW AccessibleOcean AS 
	SELECT country AS cid, oid 
	FROM neighbour, oceanAccess 
	WHERE country = cid OR neighbor = cid; 

INSERT INTO Query4 (
	SELECT cname, oname 
	FROM country, ocean, AccessibleOcean 
	WHERE country.cid = AccessibleOcean.cid AND ocean.oid = AccessibleOcean.oid 
	GROUP BY cname, oname 
	ORDER BY cname ASC, oname DESC); 

DROP VIEW AccessibleOcean;

-- Query 5 statements
CREATE VIEW AverageHDI AS
	SELECT cid, avg(hdi_score) AS avghdi 
	FROM hdi 
	WHERE year >= 2009 AND year <= 2013 
	GROUP BY cid;

INSERT INTO Query5 (
	SELECT country.cid AS cid, cname, avghdi 
	FROM AverageHDI, country 
	WHERE country.cid = AverageHDI.cid
	GROUP BY country.cid, cname, avghdi 
	ORDER BY avghdi DESC 
	LIMIT 10);

DROP VIEW AverageHDI;

-- Query 6 statements
CREATE VIEW IncreasingHDI AS 
	SELECT h1.cid AS cid 
	FROM hdi h1, hdi h2, hdi h3, hdi h4, hdi h5 
	WHERE h1.cid = h2.cid 
		AND h1.cid = h3.cid 
		AND h1.cid = h4.cid 
		AND h1.cid = h5.cid 
		AND h1.year = 2009 
		AND h2.year = 2010 
		AND h3.year = 2011 
		AND h4.year = 2012 
		AND h5.year = 2013 
		AND h2.hdi_score > h1.hdi_score 
		AND h3.hdi_score > h2.hdi_score 
		AND h4.hdi_score > h3.hdi_score 
		AND h5.hdi_score > h4.hdi_score;

INSERT INTO Query6 (
	SELECT country.cid AS cid, cname 
	FROM country, IncreasingHDI
	WHERE country.cid = IncreasingHDI.cid
	GROUP BY country.cid, cname 
	ORDER BY cname ASC);

DROP VIEW IncreasingHDI;

-- Query 7 statements
CREATE VIEW CountryReligion AS
	SELECT rid, rname, rpercentage, c1.population AS cpop
	FROM country c1, religion r1 
	WHERE c1.cid = r1.cid;

INSERT INTO Query7 (
	SElECT rid, rname, 
		sum(rpercentage*cpop) AS followers 
	FROM CountryReligion 
	GROUP BY rid, rname 
	ORDER BY followers DESC);

DROP VIEW CountryReligion;

-- Query 8 statements
CREATE VIEW MostPopularLanguage AS
	SELECT language.cid AS cid, lid, lname
	FROM language, (
		SELECT cid, max(lpercentage) AS max
		FROM language
		GROUP BY cid) AS maxlang
	WHERE lpercentage = max;

CREATE VIEW CountryPopularLanguage AS
	SELECT C1.cid AS cid, cname, lname, lid
	FROM country C1, MostPopularLanguage M1 
	WHERE C1.cid = M1.cid;

INSERT INTO Query8 (
	SELECT CP1.cname AS c1name, CP2.cname AS c2name, CP1.lname AS lname 
	FROM CountryPopularLanguage CP1, CountryPopularLanguage CP2, neighbour N1 
	WHERE CP1.cid < CP2.cid 
		AND CP1.cid = N1.country 
		AND CP2.cid = N1.neighbor 
		AND CP1.lid = CP2.lid 
	GROUP BY CP1.cname, CP2.cname, CP1.lname 
	ORDER BY lname ASC, c1name DESC);

DROP VIEW CountryPopularLanguage;
DROP VIEW MostPopularLanguage;

-- Query 9 statements
CREATE VIEW OceanCalculations AS
	SELECT C1.cname AS cname, (c1.height + O1.depth) AS totalspan 
	FROM country C1, ocean O1, oceanAccess OA1 
	WHERE C1.cid = OA1.cid AND O1.oid = OA1.oid;

CREATE VIEW NoOceanCalculations AS
	SELECT cname, height AS totalspan
	FROM country
	WHERE cname NOT IN (SELECT cname FROM OceanCalculations);

CREATE VIEW CalculatedData AS
	(SELECT * FROM OceanCalculations)
	UNION
	(SELECT * FROM NoOceanCalculations);

CREATE VIEW MaxSpan AS
	SELECT max(totalspan) AS MAX
	FROM CalculatedData;

INSERT INTO Query9 (
	SELECT cname, totalspan
	FROM CalculatedData, MaxSpan
	WHERE totalspan = max);

DROP VIEW MaxSpan;
DROP VIEW CalculatedData;
DROP VIEW NoOceanCalculations;
DROP VIEW OceanCalculations;

-- Query 10 statements
CREATE VIEW BorderLength AS
	SELECT cname, sum(length) as borderslength 
	FROM neighbour, country 
	WHERE cid = country 
	GROUP BY cname;

CREATE VIEW MaxBorderLength AS
	SELECT max(borderslength) AS max
	FROM BorderLength;

INSERT INTO Query10 (
	SELECT cname, borderslength
	FROM BorderLength, MaxBorderLength
	WHERE borderslength = max);

DROP VIEW MaxBorderLength;
DROP VIEW BorderLength;
