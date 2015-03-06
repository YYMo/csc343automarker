-- Add below your SQL statements. 
-- You can CREATE intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW CountriesAndNeighbors AS 
	(SELECT * 
	FROM country C JOIN neighbour N ON C.cid=N.country
	); 

CREATE VIEW CountriesAndNeighborsDetailed AS
	(SELECT C1.cid AS c1id, C1.cname AS c1name, neighbor AS c2id, C2.cname AS c2name, C2.height 
	FROM CountriesAndNeighbors C1 JOIN country C2 ON C1.neighbor = C2.cid
	); 

CREATE VIEW Highest AS 
	(SELECT c1id, c1name, MAX(height) 
	FROM CountriesAndNeighborsDetailed
	GROUP BY c1id, c1name 
	ORDER BY c1id
	);

INSERT INTO Query1
	(SELECT H.c1id, H.c1name, C.c2id, C.c2name 
	FROM CountriesAndNeighborsDetailed C JOIN Highest H ON MAX=height AND C.c1id = H.c1id 
	ORDER BY c1name ASC
	);

DROP VIEW Highest;
DROP VIEW CountriesAndNeighborsDetailed;
DROP VIEW CountriesAndNeighbors;

-- Query 2 statements

CREATE VIEW NoOceanAccess AS 
	(SELECT cid 
	FROM country 
	WHERE cid NOT IN (SELECT DISTINCT cid 
						FROM oceanaccess)
	);
	
INSERT INTO Query2
	(SELECT N.cid, cname 
	FROM NoOceanAccess N JOIN country C ON N.cid = C.cid 
	ORDER BY C.cname ASC
	);

DROP VIEW NoOceanAccess;

-- Query 3 statements

--CREATE VIEW op1 AS 
--	(SELECT cid 
--	FROM country 
--	WHERE cid NOT IN (SELECT DISTINCT cid 
--						FROM oceanaccess)
--	);
	
CREATE VIEW OnlyOneNeighbor AS 
	(SELECT country FROM 
	neighbour 
	GROUP BY country 
	HAVING COUNT(neighbor)=1
	);
	
CREATE VIEW OnlyOneNeighborPairs AS 
	(SELECT country, neighbor 
	FROM OnlyOneNeighbor NATURAL JOIN neighbour
	);
	
CREATE VIEW OneNeighborNoOceanAccess AS 
	(SELECT * 
	FROM OnlyOneNeighborPairs WHERE country NOT IN (SELECT DISTINCT cid 
									FROM oceanaccess)
	);

CREATE VIEW LandLocked AS 
	(SELECT O.country AS c1id, country.cname AS c1name, O.neighbor 
	FROM OneNeighborNoOceanAccess O JOIN country ON O.country = country.cid
	);

INSERT INTO Query3
	(SELECT c1id, c1name, neighbor AS c2id, cname AS c2name 
	FROM LandLocked JOIN country ON neighbor = country.cid 
	ORDER BY c1name ASC
	);

DROP VIEW LandLocked;
DROP VIEW OneNeighborNoOceanAccess;
DROP VIEW OnlyOneNeighborPairs;
DROP VIEW OnlyOneNeighbor;

-- Query 4 statements

CREATE VIEW CountriesWithDirectOceanAccess AS 
	(SELECT cid, neighbor, oid 
	FROM oceanaccess JOIN neighbour ON cid=country
	);
	
CREATE VIEW CountriesWithAnyOceanAccess AS 
	((SELECT cid, oid FROM CountriesWithDirectOceanAccess) 
	UNION 
	(SELECT neighbor, oid FROM CountriesWithDirectOceanAccess)
	);

INSERT INTO Query4
	(SELECT cname, oname 
	FROM CountriesWithAnyOceanAccess C, country, ocean WHERE C.cid=country.cid AND C.oid=ocean.oid 
	ORDER BY cname ASC, oname DESC
	);

DROP VIEW CountriesWithAnyOceanAccess;
DROP VIEW CountriesWithDirectOceanAccess;

-- Query 5 statements

CREATE VIEW CountryHDI2009to2013 AS 
	(SELECT cid, hdi_score 
	FROM hdi WHERE year >= 2009 AND year <= 2013
	);

CREATE VIEW avgHDIs AS 
	(SELECT cid, avg(hdi_score) AS avghdi 
	FROM CountryHDI2009to2013
	GROUP BY cid 
	ORDER BY avghdi DESC 
	limit 10
	);

INSERT INTO Query5
	(SELECT A.cid, cname, avghdi 
	FROM avgHDIs A JOIN country ON A.cid=country.cid
	ORDER BY avghdi DESC
	);
	
DROP VIEW avgHDIs;
DROP VIEW CountryHDI2009to2013;

-- Query 6 statements

CREATE VIEW CountryHDI2009to2013 AS 
	(SELECT cid, year, hdi_score 
	FROM hdi WHERE year >= 2009 AND year <= 2013
	);

CREATE VIEW HDIDifferences AS 
	(SELECT cid, year, hdi_score, hdi_score - lag(hdi_score) over (partition BY cid) AS diff_to_prev
	FROM CountryHDI2009to2013 
	ORDER BY cid
	);

CREATE VIEW IncHDI AS 
	(SELECT * 
	FROM HDIDifferences WHERE diff_to_prev > 0
	);

CREATE VIEW ValidCountries AS 
	(SELECT cid, COUNT(diff_to_prev) 
	FROM IncHDI 
	GROUP BY cid 
	HAVING COUNT(diff_to_prev)=4
	);

INSERT INto Query6
	(SELECT V.cid, country.cname 
	FROM ValidCountries V JOIN country ON V.cid=country.cid
	ORDER BY cname ASC
	);
	
DROP VIEW ValidCountries;
DROP VIEW IncHDI;
DROP VIEW HDIDifferences;
DROP VIEW CountryHDI2009to2013;

-- Query 7 statements

INSERT INTO Query7
	(SELECT rid, rname, SUM(rpercentage*population) AS followers 
	FROM country JOIN religion ON country.cid = religion.cid 
	GROUP BY rid, rname 
	ORDER BY followers DESC
	);

-- Query 8 statements

CREATE VIEW CountriesAndMostPopLang1 AS 
	(SELECT cid, MAX(lpercentage) 
	FROM language 
	GROUP BY cid
	);

CREATE VIEW CountriesAndMostPopLang2 AS 
	(SELECT L.cid, L.lid, L.lname 
	FROM language L JOIN CountriesAndMostPopLang1 C ON L.cid=C.cid AND C.MAX=L.lpercentage
	);

CREATE VIEW CountriesAndMostPopLang3 AS 
	(SELECT C.cid, cname, lid, lname 
	FROM CountriesAndMostPopLang2 C JOIN country ON C.cid=country.cid
	);
	
CREATE VIEW CountryPairsSamePopLang AS 
	(SELECT A.cid AS c1id, A.cname AS c1name, B.cid AS c2id, B.cname AS c2name, A.lid, A.lname 
	FROM CountriesAndMostPopLang3 A JOIN CountriesAndMostPopLang3 B ON A.cid!=B.cid AND A.lid=B.lid
	);

INSERT INTO Query8
	(SELECT c1name, c2name, lname 
	FROM CountryPairsSamePopLang JOIN neighbour ON c1id=country AND c2id=neighbor
	ORDER BY c1name DESC);
	
DROP VIEW CountryPairsSamePopLang;
DROP VIEW CountriesAndMostPopLang3;
DROP VIEW CountriesAndMostPopLang2;
DROP VIEW CountriesAndMostPopLang1;

-- Query 9 statements

CREATE VIEW CountriesAndOceans AS 
	(SELECT cid, ocean.oid, oname, depth 
	FROM ocean JOIN oceanaccess ON ocean.oid=oceanaccess.oid
	);

CREATE VIEW CountriesWithDeepestDepth AS 
	(SELECT cid, MAX(depth) 
	FROM CountriesAndOceans 
	GROUP BY cid
	);

CREATE VIEW CountriesAndDeepestOcean AS 
	(SELECT C1.cid, oid, oname, depth, max 
	FROM CountriesAndOceans C1 JOIN CountriesWithDeepestDepth C2 ON 
	C1.cid=C2.cid AND depth=max);

CREATE VIEW CountriesTotalSpan AS 
	(SELECT country.cid, cname, height+depth AS totalspan 
	FROM country JOIN CountriesAndDeepestOcean C ON C.cid=country.cid
	);

CREATE VIEW CountriesTotalSpanWithOceanAccess AS 
	(SELECT cid, cname, height AS totalspan 
	FROM country WHERE cid NOT IN (SELECT cid 
									FROM oceanaccess)
	);

CREATE VIEW CountriesAndSpans AS 
	((SELECT * FROM CountriesTotalSpan) 
	UNION 
	(SELECT * FROM CountriesTotalSpanWithOceanAccess)
	);

INSERT INto Query9
	(SELECT cname, totalspan 
	FROM CountriesAndSpans WHERE totalspan=(SELECT MAX(totalspan) 
								FROM CountriesAndSpans)
	);
	
DROP VIEW CountriesAndSpans;
DROP VIEW CountriesTotalSpanWithOceanAccess;
DROP VIEW CountriesTotalSpan;
DROP VIEW CountriesAndDeepestOcean;
DROP VIEW CountriesWithDeepestDepth;
DROP VIEW CountriesAndOceans;

-- Query 10 statements

CREATE VIEW summedBorderLengths AS 
	(SELECT country, SUM(length) AS borderslength 
	FROM neighbour 
	GROUP BY country
	);

CREATE VIEW countryWithBordersLength AS 
	(SELECT cname, borderslength 
	FROM summedBorderLengths S JOIN country ON S.country = country.cid
	);

INSERT INTO Query10
	(SELECT cname, borderslength 
	FROM countryWithBordersLength C WHERE borderslength=(SELECT MAX(borderslength) 
									FROM countryWithBordersLength)
	);

DROP VIEW countryWithBordersLength;
DROP VIEW summedBorderLengths;
