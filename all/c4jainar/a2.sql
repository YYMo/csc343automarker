-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW NeighborHeights AS
SELECT country AS c1id, neighbor AS c2id, cname AS c2name, height AS c2height 
FROM country C JOIN neighbour N ON C.cid=N.neighbor;

CREATE VIEW HighestNeighbourElevation AS
SELECT NH.c1id, cname AS c1name, max(c2height) AS c2height
FROM NeighborHeights NH JOIN country C ON NH.c1id=C.cid
GROUP BY NH.c1id, cname;


INSERT INTO Query1 (
	SELECT NH.c1id, c1name, c2id, c2name
	FROM NeighborHeights NH JOIN HighestNeighbourElevation HNE ON NH.c1id = HNE.c1id AND NH.c2height = HNE.c2height
	ORDER BY c1name
	);

DROP VIEW HighestNeighbourElevation;
DROP VIEW NeighborHeights;

-- Query 2 statements

INSERT INTO Query2 (
	SELECT cid, cname
	FROM country C
	WHERE C.cid NOT IN (SELECT cid FROM oceanAccess)
	ORDER BY cname
	);

-- Query 3 statements

CREATE VIEW LandLockedCountries AS	
SELECT cid AS c1id, cname AS c1name
FROM country C JOIN neighbour N ON C.cid=N.country
WHERE cid NOT IN (SELECT cid FROM oceanAccess)
GROUP BY cid, cname HAVING count(*)=1;

CREATE VIEW LockingNeighbours AS
SELECT c1id, c1name, neighbor AS c2id
FROM LandLockedCountries LLC JOIN neighbour N ON LLC.c1id=N.country;

INSERT INTO Query3 (
	SELECT c1id, c1name, c2id, cname AS c2name
	FROM LockingNeighbours LN JOIN country C ON LN.c2id=C.cid
	ORDER BY c1name
	);

DROP VIEW LockingNeighbours;
DROP VIEW LandLockedCountries;


-- Query 4 statements

CREATE VIEW DirectAccess AS
SELECT cname, oname
FROM oceanAccess OA, country C, ocean O
WHERE OA.cid=C.cid AND OA.oid=O.oid;

CREATE VIEW IndirectAccess AS
SELECT cname, oname
FROM neighbour N, oceanAccess OA, country C, ocean O
WHERE C.cid=N.country AND OA.cid=N.neighbor AND OA.oid=O.oid;

INSERT INTO Query4 (
	(SELECT * FROM DirectAccess) UNION (SELECT * FROM IndirectAccess)
	ORDER BY cname ASC, oname DESC
	);

DROP VIEW DirectAccess;
DROP VIEW IndirectAccess;


-- Query 5 statements

INSERT INTO Query5 (
	SELECT H.cid, cname, AVG(hdi_score) AS avghdi
	FROM hdi H JOIN country C ON H.cid=C.cid
	WHERE year BETWEEN 2009 AND 2013
	GROUP BY H.cid, cname 
	ORDER BY AVG(hdi_score) DESC
	LIMIT 10
	);

-- Query 6 statements


-- TODO: Make the following query (hack) not take a million years.
-- Alternative: Use PL/pgSQL stored procedure


CREATE VIEW IncreasingHDI AS
SELECT H1.cid
FROM hdi H1, hdi H2, hdi H3, hdi H4, hdi H5
WHERE H1.cid = H2.cid AND H2.cid = H3.cid AND H3.cid = H4.cid AND H4.cid = H5.cid AND
	H1.year = 2009 AND H5.year = 2013 AND H1.year < H2.year AND H2.year < H3.year 
	AND H3.year < H4.year AND H4.year < H5.year
	AND H1.hdi_score < H2.hdi_score AND H2.hdi_score < H3.hdi_score 
	AND H3.hdi_score < H4.hdi_score AND H4.hdi_score < H5.hdi_score 
GROUP BY H1.cid;


INSERT INTO Query6 (
	SELECT IH.cid, cname
	FROM IncreasingHDI IH JOIN country C ON IH.cid=C.cid
	ORDER BY cname
	);

DROP VIEW IncreasingHDI;

-- Query 7 statements

INSERT INTO Query7 (
	SELECT rid, rname, SUM(rpercentage * population) as followers
	FROM religion R JOIN country C ON R.cid=C.cid
	GROUP BY rid, rname
	ORDER BY SUM(rpercentage * population) DESC
	);


-- Query 8 statements

CREATE VIEW TopLanguages AS
SELECT C.cid, cname, lname, lid
FROM language L1 JOIN country C ON L1.cid=C.cid
WHERE lpercentage = (SELECT MAX(lpercentage) FROM language L2 WHERE L1.cid = L2.cid);

INSERT INTO Query8 (
	SELECT TL1.cname AS c1name, TL2.cname AS c2name, TL1.lname
	FROM TopLanguages TL1 JOIN TopLanguages TL2 ON TL1.lid=TL2.lid AND TL1.cid <> TL2.cid
	WHERE ROW(TL1.cid, TL2.cid) IN (SELECT country, neighbor FROM neighbour)
	ORDER BY TL1.lname ASC, TL1.cname DESC
	);

DROP VIEW TopLanguages;

-- Query 9 statements

CREATE VIEW HeightsDepths AS
SELECT C.cid, cname, height, depth
FROM oceanAccess OA JOIN ocean O ON OA.oid = O.oid RIGHT JOIN country C ON OA.cid=C.cid;

INSERT INTO Query9 (
	(SELECT cname, height AS totalspan FROM HeightsDepths WHERE depth IS NULL GROUP BY cid, cname, height)
	UNION
	(SELECT cname, MAX(height+depth) AS totalspan FROM HeightsDepths WHERE depth IS NOT NULL GROUP BY cid, cname, height)
	);

DROP VIEW HeightsDepths;

-- Query 10 statements

INSERT INTO Query10 (
	SELECT cname, SUM(length) AS borderslength
	FROM neighbour N JOIN country C ON N.country=C.cid
	GROUP BY country, cname 
	ORDER BY SUM(length) DESC
	LIMIT 1
	);
