-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

INSERT INTO Query1 (SELECT C1.cid as c1id, C1.cname as c1name, C2.cid as c2id, C2.cname as c2name
FROM country C1, country C2, neighbour N1
WHERE C1.cid = N1.country AND C2.cid = N1.neighbor AND 
C2.height >= ALL
(SELECT C22.height 
FROM country C22, neighbour N11 
WHERE C1.cid = N11.country AND C22.cid = N11.neighbor)
ORDER BY c1name ASC);

INSERT INTO Query2 (SELECT DISTINCT country.cid as cid, country.cname as cname 
FROM country, oceanAccess
WHERE country.cid NOT IN (SELECT cid FROM oceanAccess)
ORDER BY country.cname ASC);

CREATE VIEW landLocked AS
SELECT DISTINCT cid 
FROM country
WHERE cid NOT IN (SELECT cid FROM oceanAccess);

INSERT INTO Query3(SELECT C1.cid as c1id, C1.cname as c1name, C2.cid as c2id, C2.cname as c2name
FROM country C1, country C2, neighbour N1
WHERE C1.cid = N1.country AND C2.cid = N1.neighbor
AND 1 = (SELECT count(*) FROM neighbour N2 WHERE N2.country = C1.cid)
AND C1.cid IN (SELECT cid FROM landLocked)
ORDER BY c1name ASC);

DROP VIEW landLocked;

INSERT INTO Query4(SELECT DISTINCT C.cname, O.oname 
FROM country C, ocean O, oceanAccess A, neighbour N
WHERE ((C.cid = A.cid) OR (N.neighbor = A.cid AND C.cid = N.country)) AND O.oid = A.oid
ORDER BY C.cname ASC, O.oname DESC);

INSERT INTO Query5(SELECT C.cid, C.cname, AVG(H.hdi_score) as avghid
FROM country C, hdi H
WHERE C.cid = H.cid AND (H.year = 2009 OR H.year=2010 OR H.year=2011 OR H.year=2012 OR H.year=2013)
GROUP BY C.cid, C.cname
ORDER BY avghid DESC LIMIT 10);

INSERT INTO Query6(SELECT DISTINCT C.cid, C.cname
FROM country C, hdi H
WHERE C.cid = H.cid AND 
(SELECT hdi_score FROM hdi WHERE hdi.cid = C.cid AND hdi.year = 2013) >
(SELECT hdi_score FROM hdi WHERE hdi.cid = C.cid AND hdi.year = 2012) AND
(SELECT hdi_score FROM hdi WHERE hdi.cid = C.cid AND hdi.year = 2012) >
(SELECT hdi_score FROM hdi WHERE hdi.cid = C.cid AND hdi.year = 2011) AND
(SELECT hdi_score FROM hdi WHERE hdi.cid = C.cid AND hdi.year = 2011) >
(SELECT hdi_score FROM hdi WHERE hdi.cid = C.cid AND hdi.year = 2010) AND
(SELECT hdi_score FROM hdi WHERE hdi.cid = C.cid AND hdi.year = 2010) >
(SELECT hdi_score FROM hdi WHERE hdi.cid = C.cid AND hdi.year = 2009)
ORDER BY C.cname ASC);

INSERT INTO Query7(SELECT rid, rname, SUM(C.population * R.rpercentage/100) as followers
FROM religion R, country C
WHERE R.cid = C.cid 
GROUP BY rid, rname
ORDER BY followers);

INSERT INTO Query8(SELECT C1.cname as c1name, C2.cname as c2name, L1.lname
FROM country C1, country C2, language L1, language L2, neighbour N1
WHERE C1.cid = N1.country AND C2.cid = N1.neighbor AND L1.cid = C1.cid AND L2.cid = C2.cid AND L2.lid = L1.lid AND
L1.lpercentage >= ALL
(SELECT lpercentage 
FROM language L3
WHERE L3.cid = C1.cid)
AND 
L2.lpercentage >= ALL
(SELECT lpercentage 
FROM language L4
WHERE L4.cid = C2.cid)
ORDER BY lname ASC, c1name DESC);

INSERT INTO Query9((SELECT C.cname, (C.height + O.depth) as totalspan
FROM country C, oceanAccess A, ocean O
WHERE C.cid = A.cid AND O.oid = A.oid AND (C.height + O.depth) >= ALL
(SELECT (C1.height + O1.depth)
FROM country C1, oceanAccess A1, ocean O1
WHERE C1.cid = A1.cid AND O1.oid = A1.oid) AND (C.height + O.depth) >= ALL
(SELECT C2.height
FROM country C2))
UNION
(SELECT C.cname, C.height as totalspan
FROM country C
WHERE C.height >= ALL
(SELECT C.height + O.depth
FROM country C, oceanAccess A, ocean O
WHERE C.cid = A.cid AND O.oid = A.oid)));

INSERT INTO Query10(SELECT C.cname, SUM(N.length) as borderslength
FROM country C, neighbour N
WHERE C.cid = N.country 
GROUP BY C.cname
HAVING SUM(N.length) >= ALL
(SELECT SUM(N1.length)
FROM country C1, neighbour N1
WHERE C1.cid = N1.country
GROUP BY C1.cname));













