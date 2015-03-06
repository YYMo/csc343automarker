-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW neighborHeights AS
	SELECT N.country AS c1id, N.neighbor AS c2id, C.height AS height, C.cname AS c2name
	FROM neighbour N JOIN country C ON C.cid = N.neighbor;

CREATE VIEW maxNeighborHeight AS
	SELECT NH1.*
	from neighborHeights NH1 LEFT JOIN neighborHeights NH2
	ON (NH1.c1id = NH2.c1id AND NH1.height < NH2.height)
	WHERE NH2.height IS NULL;

INSERT INTO Query1(
	SELECT mNH.c1id, C.cname as c1name, mNH.c2id, mNH.c2name
	FROM maxNeighborHeight mNH INNER JOIN country C ON mNH.c1id = C.cid
	ORDER BY c1name ASC
);

DROP VIEW maxNeighborHeight;
DROP VIEW neighborHeights;


-- Query 2 statements
CREATE VIEW noOceanAccess AS
	SELECT cid FROM country
	EXCEPT
	SELECT cid FROM oceanAccess;

INSERT INTO Query2 (
	SELECT noOA.cid, C.cname
	FROM noOceanAccess noOA, country C
	WHERE C.cid = noOA.cid	
	ORDER BY C.cname ASC
);

DROP VIEW noOceanAccess;

-- Query 3 statements
CREATE VIEW noOceanAccess AS
	SELECT cid FROM country
	EXCEPT
	SELECT cid FROM oceanAccess;


CREATE VIEW landlocked AS
	SELECT noOA.cid, C.cname
	FROM noOceanAccess noOA, country C
	WHERE C.cid = noOA.cid	
	ORDER BY C.cname ASC;

--Find countries that are surrounded by exactly one neighbour
CREATE VIEW only1Neighbor AS
	SELECT LL.cid as c1id, COUNT(N.neighbor)
	FROM landlocked LL, neighbour N
	WHERE LL.cid = N.country
	GROUP BY LL.cid
	HAVING COUNT(N.neighbor)=1;

INSERT INTO Query3(
	SELECT o1N.c1id as c1id, C1.cname as c1name, N.neighbor as c2id, C2.cname as c2name
	FROM only1Neighbor o1N INNER JOIN neighbour N ON o1N.c1id = N.country
				INNER JOIN country C1 ON o1N.c1id = C1.cid
				INNER JOIN country C2 ON N.neighbor = C2.cid
	ORDER BY c1name ASC
);

DROP VIEW only1Neighbor;
DROP VIEW landlocked;
DROP VIEW noOceanAccess;

-- Query 4 statements

--Create a view that has a list of countries that are neighbours and do not have any ocean access
CREATE VIEW noOceanNeighbors AS
	SELECT country as cid FROM neighbour
	EXCEPT
	SELECT cid FROM oceanAccess;

CREATE VIEW indirectOcean AS
	SELECT nON.cid as cid, OA.oid as oid
	FROM noOceanNeighbors nON, neighbour N, oceanAccess OA
	WHERE nON.cid=N.neighbor AND N.country = OA.cid;

CREATE VIEW allOceanAccess AS
	SELECT * from indirectOcean
	UNION
	SELECT * from oceanAccess;

INSERT INTO Query4(
	SELECT C.cname as cname, O.oname as oname
	FROM allOceanAccess AOA, country C, ocean O
	WHERE AOA.cid=C.cid AND AOA.oid=O.oid
	ORDER BY cname ASC, oname DESC
);

DROP VIEW allOceanAccess;
DROP VIEW indirectOcean;
DROP VIEW noOceanNeighbors;

-- Query 5 statements
CREATE VIEW fiveyears AS
	SELECT *
	FROM hdi
	WHERE year >= 2009 AND year<=2013;

CREATE VIEW averageHDI AS
	SELECT cid, AVG(hdi_score) as avghdi
	FROM fiveyears
	GROUP BY cid;

INSERT INTO Query5(
	SELECT aHDI.cid as cid, C.cname as cname, aHDI.avghdi as avghdi
	FROM averageHDI aHDI, country C
	WHERE aHDI.cid = C.cid
	ORDER BY avghdi DESC
	LIMIT 10
);

DROP VIEW averageHDI;
DROP VIEW fiveyears;

-- Query 6 statements
CREATE VIEW y9 AS
	SELECT *
	FROM hdi
	WHERE year = 2009
	ORDER BY cid ASC;

CREATE VIEW y10 AS
	SELECT *
	FROM hdi
	WHERE year = 2010
	ORDER BY cid ASC;

CREATE VIEW y11 AS
	SELECT *
	FROM hdi
	WHERE year = 2011
	ORDER BY cid ASC;

CREATE VIEW y12 AS
	SELECT *
	FROM hdi
	WHERE year = 2012
	ORDER BY cid ASC;

CREATE VIEW y13 AS
	SELECT *
	FROM hdi
	WHERE year = 2013
	ORDER BY cid ASC;

INSERT INTO Query6(
	SELECT y9.cid as cid, C.cname as cname
	FROM y9 JOIN y10 ON y9.cid=y10.cid
		JOIN y11 ON y9.cid=y11.cid
		JOIN y12 ON y9.cid=y12.cid
		JOIN y13 ON y9.cid=y13.cid
		JOIN country C ON y9.cid=C.cid
	WHERE y9.hdi_score < y10.hdi_score AND y10.hdi_score < y11.hdi_score
		AND y11.hdi_score < y12.hdi_score AND y12.hdi_score < y13.hdi_score
	ORDER BY cname ASC
);

DROP VIEW y9;
DROP VIEW y10;
DROP VIEW y11;
DROP VIEW y12;
DROP VIEW y13;

-- Query 7 statements
CREATE VIEW religionPopulation AS
	SELECT R.cid, R.rid, R.rname, R.rpercentage * C.population as cfollowers
	FROM religion R, country C
	WHERE R.cid = C.cid;

INSERT INTO Query7(
	SELECT RP.rid as rid, RP.rname as rname, SUM(RP.cfollowers) as followers
	FROM religionPopulation RP
	GROUP BY RP.rid, RP.rname
	ORDER BY followers DESC
);

DROP VIEW religionPopulation;

-- Query 8 statements
CREATE VIEW mostPopularLang AS
	SELECT L1.*
	from language L1 LEFT JOIN language L2
	ON (L1.cid = L2.cid AND L1.lpercentage < L2.lpercentage)
	WHERE L2.lpercentage IS NULL;

CREATE VIEW neighbourLanguage AS
	SELECT N.country as c1id, N.neighbor as c2id, MPL1.lname as lname
	from neighbour N JOIN mostPopularLang MPL1 ON N.country = MPL1.cid
			  JOIN mostPopularLang MPL2 ON N.neighbor = MPL2.cid
	WHERE MPL1.lid = MPL2.lid;

INSERT INTO Query8(
SELECT C1.cname as c1name, C2.cname as c2name, NL.lname as lname
FROM neighbourLanguage NL JOIN country C1 ON NL.c1id=C1.cid
			   JOIN country C2 ON NL.c2id=C2.cid
ORDER BY lname ASC, c1name DESC
);

DROP VIEW neighbourLanguage;
DROP VIEW mostPopularLang;

-- Query 9 statements

-- Associate country's surrounding oceans to the ocean's depth
CREATE VIEW oceanAccessDepth AS
	SELECT OA.cid, OA.oid, O.depth
	FROM oceanAccess OA, ocean O
	WHERE OA.oid = O.oid;

CREATE VIEW MaxOceanAccessDepth AS
	SELECT OA1.*
	FROM oceanAccessDepth OA1 LEFT JOIN oceanAccessDepth OA2
	ON (OA1.cid = OA2.cid AND OA1.depth < OA2.depth)
	WHERE OA2.depth IS NULL;

CREATE VIEW oceanCountries AS
	SELECT C.cid as cid, C.cname as cname, C.height-OAD.depth as totalspan
	FROM country C JOIN MaxOceanAccessDepth OAD ON C.cid=OAD.cid;

-- Create a view of all countries that have no ocean access and set totalspan to their height
CREATE VIEW noOceanCountries AS
	SELECT C.cid as cid, C.cname as cname, C.height as totalspan
	FROM country C
	WHERE C.cid NOT IN (SELECT cid FROM oceanCountries);

INSERT INTO Query9(
	SELECT cname, totalspan FROM oceanCountries
	UNION
	SELECT cname, totalspan FROM noOceanCountries
);

DROP VIEW noOceanCountries;
DROP VIEW oceanCountries;
DROP VIEW MaxOceanAccessDepth;
DROP VIEW oceanAccessDepth;

-- Query 10 statements
CREATE VIEW sumBorder AS
	SELECT country, SUM(length) as border
	FROM neighbour N
	GROUP BY country;

CREATE VIEW maxBorder AS
	SELECT country, border as borderslength
	FROM sumBorder
	WHERE border = (SELECT MAX(border) FROM sumBorder) ;

INSERT INTO Query10(
	SELECT C.cname as cname, MB.borderslength as borderslength
	FROM maxBorder MB JOIN country C ON MB.country = C.cid
);

DROP VIEW maxBorder;
DROP VIEW sumBorder;

