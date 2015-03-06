-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

INSERT INTO Query1 (
SELECT C1.cid AS c1id, C1.cname AS c1name, C2.cid AS c2id, C2.cname AS c2name
FROM country AS C2, neighbour AS N, 
	(SELECT C1.cid, C1.cname, max(C2.height) AS NeighbourMaxHeight
	FROM country AS C1, country AS C2, neighbour AS N
	WHERE C1.cid=N.country and C2.cid=N.neighbor
	GROUP BY C1.cid, C1.cname) AS C1
WHERE C1.cid=N.country and C2.cid=N.neighbor and C2.height=C1.NeighbourMaxHeight
ORDER BY C1.cname ASC
);

-- Query 2 statements

INSERT INTO Query2 (
SELECT C.cid AS cid, C.cname AS cname
FROM country AS C
WHERE C.cid NOT IN 
	(
	SELECT O.cid AS cid
	FROM oceanAccess AS O
	)
ORDER BY C.cname ASC
);

-- Query 3 statements

INSERT INTO Query3 (SELECT C1.cid AS c1id, C1.cname AS c1name, C2.cid AS c2id, C2.cname AS c2name
FROM country AS C2, neighbour AS N, (SELECT C1.cid, C1.cname
FROM country AS C2, neighbour AS N, (SELECT C.cid AS cid, C.cname AS cname
FROM country AS C
WHERE C.cid NOT IN (
SELECT O.cid AS cid
FROM oceanAccess AS O)
ORDER BY C.cname ASC) AS C1
WHERE C1.cid=N.country and C2.cid=N.neighbor
GROUP BY C1.cid, C1.cname
HAVING COUNT(C2.cid)=1
ORDER BY C1.cname ASC) AS C1
WHERE C1.cid=N.country and C2.cid=N.neighbor);

-- Query 4 statements

INSERT INTO Query4 (SELECT *
FROM (SELECT C2.cname AS cname, C1.oname AS oname
FROM country AS C2, neighbour AS N, (SELECT C.cid, C.cname AS cname, O.oname AS oname
FROM country AS C, oceanAccess, ocean as O
WHERE C.cid=oceanAccess.cid and oceanAccess.oid=O.oid) AS C1
WHERE C1.cid=N.country and C2.cid=N.neighbor
UNION
SELECT C.cname AS cname, O.oname AS oname
FROM country AS C, oceanAccess, ocean as O
WHERE C.cid=oceanAccess.cid and oceanAccess.oid=O.oid) AS AccessibleCountries
ORDER BY cname ASC, oname DESC);

-- Query 5 statements

INSERT INTO Query5 (SELECT C.cid AS cid, C.cname AS cname, AVG(H.hdi_score) AS avghdi
FROM country AS C, hdi AS H
WHERE C.cid=H.cid and (H.year=2009 or H.year=2010 or H.year=2011 or H.year=2012 or H.year=2013)
GROUP BY C.cid, C.cname
ORDER BY AVG(H.hdi_score) DESC
LIMIT 10);

-- Query 6 statements

INSERT INTO Query6 (SELECT C.cid AS cid, C.cname AS cname
FROM country AS C, (SELECT H.cid AS c1cid, H.hdi_score AS c1hdi_score, H.year AS c1year, H2.cid AS c2cid, H2.hdi_score AS c2hdi_score, H2.year AS c2year, H.hdi_score<H2.hdi_score AS result
FROM hdi AS H, hdi AS H2, (SELECT H.cid AS c1cid, H.hdi_score AS c1hdi_score, H.year AS c1year, H2.cid AS c2cid, H2.hdi_score AS c2hdi_score, H2.year AS c2year, H.hdi_score<H2.hdi_score AS result 
FROM hdi AS H, hdi AS H2, (SELECT H.cid AS c1cid, H.hdi_score AS c1hdi_score, H.year AS c1year, H2.cid AS c2cid, H2.hdi_score AS c2hdi_score, H2.year AS c2year, H.hdi_score<H2.hdi_score AS result 
FROM hdi AS H, hdi AS H2, (SELECT H.cid AS c1cid, H.hdi_score AS c1hdi_score, H.year AS c1year, H2.cid AS c2cid, H2.hdi_score AS c2hdi_socre, H2.year AS c2year, H.hdi_score<H2.hdi_score AS result 
FROM hdi AS H, hdi AS H2 
WHERE H.cid=H2.cid and H.year=2009 and H2.year=2010) AS Result1
WHERE H.cid=H2.cid and Result1.c1cid=H.cid and H.year=2010 and H2.year=2011 and Result1.result='t') AS Result2
WHERE H.cid=H2.cid and Result2.c1cid=H.cid and H.year=2011 and H2.year=2012 and Result2.result='t') AS Result3
WHERE H.cid=H2.cid and Result3.c1cid=H.cid and H.year=2012 and H2.year=2013 and Result3.result='t') AS Result4
WHERE C.cid=Result4.c1cid
ORDER BY C.cname ASC);

-- Query 7 statements

INSERT INTO Query7 (SELECT EachCountryReligionTotal.rid AS rid, EachCountryReligionTotal.rname AS rname, sum(EachCountryReligionTotal.totalfollowers) as followers
FROM (SELECT R.rid, R.rname, R.rpercentage*C.population AS totalfollowers
FROM religion as R, country as C
WHERE R.cid=C.cid) AS EachCountryReligionTotal
GROUP BY EachCountryReligionTotal.rid, EachCountryReligionTotal.rname
ORDER BY followers DESC);

-- Query 8 statements

INSERT INTO Query8 (SELECT Result.c1cname AS c1name, Result.c2cname AS c2name, Result.c1lname AS lname
FROM 
(SELECT * 
FROM neighbour AS N, 
(SELECT C.cid as c1cid, C.cname AS c1cname, L.lname AS c1lname
FROM country AS C, language AS L,
(SELECT C.cid AS c1cid, C.cname AS c1name, max(L.lpercentage) AS popular
FROM country AS C, language AS L
WHERE C.cid=L.cid
GROUP BY C.cid, C.cname) AS MostPopular
WHERE C.cid=L.cid and MostPopular.c1cid=L.cid and MostPopular.popular=L.lpercentage) AS CountryOne,
(SELECT C.cid as c2cid, C.cname AS c2cname, L.lname AS c2lname
FROM country AS C, language AS L,
(SELECT C.cid AS c1cid, C.cname AS c1name, max(L.lpercentage) AS popular
FROM country AS C, language AS L
WHERE C.cid=L.cid
GROUP BY C.cid, C.cname) AS MostPopular
WHERE C.cid=L.cid and MostPopular.c1cid=L.cid and MostPopular.popular=L.lpercentage) AS CountryTwo
WHERE CountryOne.c1lname=CountryTwo.c2lname and N.country=CountryOne.c1cid and N.neighbor=CountryTwo.c2cid) AS Result
ORDER BY Result.c1lname ASC, Result.c1cname DESC);

-- Query 9 statements

INSERT INTO Query9 (SELECT *
FROM (SELECT C.cname AS cname, CountryTotalSpan.totalspan AS totalspan
FROM country AS C, (SELECT OceanAndElevation.cid, SUM(height) AS totalspan
FROM (SELECT OA.cid, MAX(O.depth) AS height
FROM oceanAccess AS OA, ocean AS O
WHERE OA.oid=O.oid
GROUP BY OA.cid
UNION
SELECT C.cid, C.height AS height
FROM country AS C) AS OceanAndElevation
GROUP BY OceanAndElevation.cid) AS CountryTotalSpan
WHERE C.cid=CountryTotalSpan.cid
ORDER BY totalspan DESC) AS Result
WHERE Result.totalspan IN (
SELECT CountryTotalSpan.totalspan AS totalspan
FROM country AS C, (SELECT OceanAndElevation.cid, SUM(height) AS totalspan
FROM (SELECT OA.cid, MAX(O.depth) AS height
FROM oceanAccess AS OA, ocean AS O
WHERE OA.oid=O.oid
GROUP BY OA.cid
UNION
SELECT C.cid, C.height AS height
FROM country AS C) AS OceanAndElevation
GROUP BY OceanAndElevation.cid) AS CountryTotalSpan
WHERE C.cid=CountryTotalSpan.cid
ORDER BY totalspan DESC
LIMIT 1
));

-- Query 10 statements

INSERT INTO Query10 (SELECT *
FROM (SELECT C.cname AS cname, CountryBorderLengthWithNeighbours.borderlength AS borderslength
FROM country AS C, (SELECT N.country, SUM(N.length) AS borderlength
FROM neighbour AS N
GROUP BY N.country) AS CountryBorderLengthWithNeighbours
WHERE C.cid=CountryBorderLengthWithNeighbours.country
ORDER BY CountryBorderLengthWithNeighbours.borderlength DESC
LIMIT 1) AS Result
WHERE Result.borderslength IN (
(SELECT CountryBorderLengthWithNeighbours.borderlength AS borderslength
FROM country AS C, (SELECT N.country, SUM(N.length) AS borderlength
FROM neighbour AS N
GROUP BY N.country) AS CountryBorderLengthWithNeighbours
WHERE C.cid=CountryBorderLengthWithNeighbours.country
ORDER BY CountryBorderLengthWithNeighbours.borderlength DESC
LIMIT 1)
));
