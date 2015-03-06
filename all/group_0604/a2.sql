-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW maxheight (cid, maxheight) AS
SELECT N.country, max(C.height)
FROM neighbour N, country C
WHERE N.neighbor = C.cid
GROUP BY N.country;

INSERT INTO Query1 (
SELECT C1.cid AS c1id, C1.cname AS c1name, C2.cid AS C2id, C2.cname AS c2name
FROM maxheight M, country C1, country C2, neighbour N
WHERE M.cid = C1.cid AND M.maxheight = C2.height AND M.cid = N.country AND C2.cid = N.neighbor
ORDER BY c1name
);

DROP VIEW maxheight;

-- Query 2 statements

INSERT INTO Query2 (
SELECT cid, cname 
FROM country
WHERE cid NOT IN (SELECT cid FROM oceanAccess)
ORDER BY cname
);

-- Query 3 statements

CREATE VIEW atleast2 (cid) AS
SELECT N1.country
FROM neighbour N1, neighbour N2
WHERE N1.country = N2.country AND N1.neighbor != N2.neighbor;

INSERT INTO Query3 (
SELECT C1.cid AS c1id, C1.cname AS c1name, C2.cid AS c2id, C2.cname AS c2name
FROM neighbour N, country C1, country C2
WHERE N.country = C1.cid AND N.neighbor = C2.cid AND N.country NOT IN (SELECT cid FROM oceanAccess) AND N.country NOT IN (SELECT cid FROM atleast2)
ORDER BY c1name
);

DROP VIEW atleast2;

-- Query 4 statements

CREATE VIEW indirectaccess (cid, oid) AS
SELECT N.country, O.oid
FROM neighbour N, oceanAccess O
WHERE N.neighbor = O.cid;

INSERT INTO Query4 (
SELECT C.cname AS cname, O.oname AS oname
FROM (SELECT * FROM oceanAccess UNION SELECT * FROM indirectaccess) A, country C, ocean O
WHERE A.cid = C.cid AND A.oid = O.oid
ORDER BY cname, oname DESC
);

DROP VIEW indirectaccess;

-- Query 5 statements

CREATE VIEW avghdi (cid, avghdi) AS
SELECT cid, avg(hdi_score)
FROM hdi
WHERE year >= 2009 AND year <= 2013
GROUP BY cid;

INSERT INTO Query5 (
SELECT A.cid AS cid, C.cname AS cname, A.avghdi AS avghdi
FROM avghdi A, country C
WHERE A.cid = C.cid
ORDER BY avghdi DESC
LIMIT 10
);

DROP VIEW avghdi;

-- Query 6 statements

CREATE VIEW notincreasing (cid) AS
SELECT H1.cid
FROM hdi H1, hdi H2
WHERE H1.cid = H2.cid AND H1.year >= 2009 AND H1.year < H2.year AND H2.year <= 2013 AND H1.hdi_score >= H2.hdi_score;

INSERT INTO Query6 (
SELECT DISTINCT C.cid AS cid, C.cname AS cname
FROM country C, hdi H
WHERE C.cid = H.cid AND H.year >= 2009 AND H.year <= 2013 AND C.cid NOT IN (SELECT cid FROM notincreasing)
ORDER BY cname
);

DROP VIEW notincreasing;

-- Query 7 statements

INSERT INTO Query7 (
SELECT R.rid AS rid, R.rname AS rname, sum(C.population * R.rpercentage) AS followers
FROM religion R, country C
WHERE R.cid = C.cid
GROUP BY rid, rname
ORDER BY followers DESC
);

-- Query 8 statements

CREATE VIEW mostpopular (cid, lname) AS
SELECT L1.cid, L2.lname
FROM (SELECT cid, max(lpercentage) AS maxper FROM language GROUP BY cid) L1 INNER JOIN language L2 ON L1.cid = L2.cid AND L1.maxper = L2.lpercentage;

CREATE VIEW samepopular (c1id, c2id, lname) AS
SELECT P1.cid, P2.cid, P1.lname
FROM MostPopular P1, MostPopular P2
WHERE P1.lname = P2.lname AND P1.cid != P2.cid;

INSERT INTO Query8 (
SELECT C1.cname AS c1name, C2.cname AS c2name, P.lname AS lname
FROM samepopular P, country C1, country C2, neighbour N
WHERE P.c1id = C1.cid AND P.c2id = C2.cid AND P.c1id = N.country AND P.c2id = N.neighbor
ORDER BY lname, c1name DESC
);

DROP VIEW samepopular;
DROP VIEW mostpopular;

-- Query 9 statements

CREATE VIEW countryocean (cid, totalspan) AS
SELECT C.cid AS cid, C.height + max(O.depth) AS totalspan
FROM country C, oceanAccess A, ocean O
WHERE C.cid = A.cid AND A.oid = O.oid
GROUP BY C.cid;

CREATE VIEW countrynoocean (cid, totalspan) AS
SELECT cid, height AS totalspan
FROM country
WHERE cid NOT IN (SELECT cid FROM oceanAccess);

CREATE VIEW totalspan (cid, totalspan) AS
SELECT * FROM countryocean UNION SELECT * FROM countrynoocean;

INSERT INTO Query9 (
SELECT C.cname AS cname, T.totalspan AS totalspan
FROM totalspan T, country C
WHERE T.cid = C.cid AND T.totalspan = (SELECT max(totalspan) FROM TotalSpan)
);

DROP VIEW totalspan;
DROP VIEW countryocean;
DROP VIEW countrynoocean;

-- Query 10 statements

CREATE VIEW borderslength (cid, borderslength) AS
SELECT country, sum(length)
FROM neighbour
GROUP BY country;

INSERT INTO Query10 (
SELECT C.cname AS cname, B.borderslength AS borderslength
FROM borderslength B, country C
WHERE B.cid = C.cid AND B.borderslength = (SELECT max(borderslength) FROM BordersLength)
);

DROP VIEW borderslength;
