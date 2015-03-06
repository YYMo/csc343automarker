--INSERT INTO country(cid, cname, height, population)
--VALUES(1, 'A', 1, 100),(2, 'B', 2, 200),(3, 'C', 3, 300),(4, 'D', 4, 400),(5, 'E', 5, 500);

--INSERT INTO religion(cid, rid, rname, rpercentage)
--VALUES(1, 1, 'religion1', 10), (2, 2, 'religion2', 20), (3, 3, 'religion3', 30), (4, 4, 'religion4', 40), (5, 5, 'religion5', 50), (5, 1, 'religion1', 50), (5, 2, 'religion2', 50), (5, 3, 'religion3', 50), (5, 4, 'religion4', 50);

--INSERT INTO hdi(cid, year, hdi_score)
--VALUES(1, 2008, 100), (1, 2009, 110), (1, 2010, 120), (1, 2011, 130), (1, 2012, 140), (1, 2013, 150),
	  --(2, 2008, 200), (2, 2009, 210), (2, 2010, 220), (2, 2011, 230), (2, 2012, 240), (2, 2013, 250),
	  --(3, 2008, 300), (3, 2009, 310), (3, 2010, 320), (3, 2011, 330), (3, 2012, 340), (3, 2013, 350),
	  --(4, 2008, 400), (4, 2009, 410), (4, 2010, 420), (4, 2011, 430), (4, 2012, 440), (4, 2013, 450),
	  --(5, 2008, 500), (5, 2009, 510), (5, 2010, 520), (5, 2011, 530), (5, 2012, 540), (5, 2013, 550);

--INSERT INTO ocean(oid, oname, depth)
--VALUES(1, 'ocean1', 5), (2, 'ocean2', 2);

--INSERT INTO neighbour(country, neighbor, length)
--VALUES(4, 1, 9), (4, 2, 9), (4, 3, 9), (4, 5, 9), (1, 4, 9), (1, 2, 9), (1, 3, 9), (2, 1, 9), (2, 3, 9), (2, 4, 9),
	  --(3, 1, 9), (3, 2, 9), (3, 4, 9), (5, 4, 9);

--INSERT INTO oceanAccess(cid, oid)
--VALUES (1, 1), (2, 2);

--INSERT INTO language(cid, lid, lname, lpercentage)

-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW Query1View AS
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name, c2.height AS height
FROM (neighbour JOIN country c1 ON c1.cid=neighbour.country) JOIN country c2 ON c2.cid=neighbour.neighbor;

INSERT INTO Query1(
SELECT c1id, c1name, c2id, c2name
FROM Query1View q1
WHERE height >= ALL(SELECT height FROM Query1View q2 WHERE q1.c1id = q2.c1id)
ORDER BY c1name ASC
);

DROP VIEW Query1View;
-- Query 2 statements
INSERT INTO Query2(
SELECT cid, cname
FROM (
SELECT cid, cname
FROM country
EXCEPT
SELECT country.cid AS cid, country.cname AS cname
FROM country, oceanAccess
WHERE country.cid = oceanAccess.cid) AS unsortedList
ORDER BY CNAME ASC
);


-- Query 3 statements
CREATE VIEW Query3View1 AS
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name
FROM (neighbour JOIN country c1 ON c1.cid=neighbour.country) JOIN country c2 ON c2.cid=neighbour.neighbor;

CREATE VIEW Query3View2 AS
SELECT c1id
FROM Query2 JOIN Query3View1 ON c1id = cid
GROUP BY c1id
HAVING count(*) = 1;

INSERT INTO Query3(
SELECT Query3View2.c1id AS c1id, c1name, c2id, c2name
FROM Query3View2 JOIN Query3View1 ON Query3View2.c1id = Query3View1.c1id
ORDER BY c1name ASC
);

DROP VIEW Query3View2;
DROP VIEW Query3View1;
-- Query 4 statements

INSERT INTO Query4(
SELECT *
FROM (
SELECT country.cname AS cname, ocean.oname AS oname
FROM country JOIN oceanAccess AS cO
ON country.cid = cO.cid 
JOIN ocean 
ON cO.oid = ocean.oid
UNION
SELECT country.cname AS cname, ocean.oname AS oname
FROM country JOIN neighbour 
ON country.cid = neighbour.country
JOIN oceanAccess AS nO
ON neighbour.neighbor = nO.cid
JOIN ocean 
ON nO.oid = ocean.oid
) sub
ORDER BY cname ASC, oname DESC
);

-- Query 5 statements
CREATE VIEW Query5View AS  
SELECT cid, AVG(hdi_score) AS avghdi
FROM hdi
WHERE year > 2008 AND year < 2014
GROUP BY cid
ORDER BY AVG(hdi_score) DESC
LIMIT 10;

INSERT INTO Query5(
SELECT country.cid AS cid, cname, avghdi
FROM Query5View JOIN country ON Query5View.cid = country.cid
ORDER BY avghdi DESC
);

DROP VIEW Query5View;
-- Query 6 statements

CREATE VIEW hdiyears AS(
SELECT hdiNine.cid AS cid
FROM hdi AS hdiNine 
JOIN hdi AS hdiTen
ON hdiNine.cid = hdiTen.cid
JOIN hdi AS hdiEleven
ON hdiTen.cid = hdiEleven.cid
JOIN hdi AS hdiTwelve
ON hdiEleven.cid = hdiTwelve.cid
JOIN hdi AS hdiThirteen
ON hdiTwelve.cid = hdiThirteen.cid
WHERE hdiNine.year = 2009 AND hdiTen.year = 2010 AND hdiNine.hdi_score < hdiTen.hdi_score 
AND hdiEleven.year = 2011 AND hdiTen.hdi_score < hdiEleven.hdi_score
AND hdiTwelve.year = 2012 AND hdiEleven.hdi_score < hdiTwelve.hdi_score
AND hdiThirteen.year = 2013 AND hdiTwelve.hdi_score < hdiThirteen.hdi_score
);

INSERT INTO Query6(
SELECT hdiyears.cid AS cid, country.cname AS cname
FROM hdiyears JOIN country
ON hdiyears.cid = country.cid
ORDER BY cname ASC
);

DROP VIEW hdiyears;

-- Query 7 statements

INSERT INTO Query7(
SELECT rid, rname, SUM(rpercentage*population) AS followers
FROM religion JOIN country ON religion.cid = country.cid
GROUP BY rid, rname
ORDER BY followers DESC
);


-- Query 8 statements

CREATE VIEW cMaxLang AS(
SELECT cMperc.cid AS cid, allLang.lname AS lname
FROM 
(
SELECT cid, max(lpercentage) AS lperc
FROM language
GROUP BY cid
) AS cMperc
JOIN language AS allLang
ON cMperc.cid = allLang.cid
WHERE cMperc.lperc = allLang.lpercentage
);

INSERT INTO Query8 (
SELECT cccOne.cname AS c1name, cccTwo.cname AS c2name, lMaxOne.lname AS lname
FROM neighbour AS cOne JOIN cMaxLang AS lMaxOne
ON cOne.country = lMaxOne.cid
JOIN cMaxLang AS lMaxTwo
ON cOne.neighbor = lMaxTwo.cid
JOIN country AS cccOne
ON cOne.country = cccOne.cid
JOIN country AS cccTwo
ON cOne.neighbor = cccTwo.cid
WHERE 
lMaxOne.lname = lMaxTwo.lname
GROUP BY cccOne.cname, cccTwo.cname, lMaxOne.lname
ORDER BY
lname ASC, c1name DESC
);

DROP VIEW cMaxLang;

-- Query 9 statements
CREATE VIEW Query9View AS
SELECT cid, max(depth) as deepest
FROM ocean JOIN oceanAccess ON ocean.oid = oceanAccess.oid
GROUP BY cid;

INSERT INTO Query9(
SELECT cname, height + COALESCE(deepest, 0) as totalspan
FROM country LEFT JOIN Query9View ON country.cid = Query9View.cid
ORDER BY height + COALESCE(deepest, 0) DESC
LIMIT 1
);

DROP VIEW Query9View;
-- Query 10 statements

CREATE VIEW totBord AS(
SELECT country, sum(length) AS totLength
FROM neighbour
GROUP BY country

);

INSERT INTO Query10(
SELECT country.cname AS cname, max(totBord.totLength) AS borderslength
FROM totBord JOIN country
ON totBord.country = country.cid
GROUP BY country.cname
);

DROP VIEW totBord;
