-- Add below your SQL statements.
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW HighestElevation AS
SELECT n.country as cid, max(c.height) as elevation
FROM country c JOIN neighbour n ON c.cid=n.neighbor
GROUP BY n.country;

INSERT INTO Query1
(SELECT C1.cid as c1id, C1.cname as c1name, C2.cid as c2id, C2.cname as c2name
	FROM country C1 JOIN neighbour ON C1.cid=neighbour.country
	JOIN country C2 ON C2.cid=neighbour.neighbor
	WHERE C2.height=(SELECT elevation FROM HighestElevation WHERE cid=C1.cid)
	ORDER BY C1.name ASC);

DROP VIEW HighestElevation;

-- Query 2 statements

CREATE VIEW EnclosedCountries AS
SELECT cid
FROM country c
WHERE c.cid NOT IN (SELECT cid FROM oceanAccess);

INSERT INTO Query2
(SELECT c.cid AS cid, c.cname AS cname
	FROM country c JOIN EnclosedCountries e
	ON c.cid=e.cid
	ORDER BY c.cname ASC);


-- Query 3 statements

CREATE VIEW NeighborCount AS
SELECT country as cid, count(n.neighbor) as count
FROM neighbour n
WHERE n.country IN (SELECT cid FROM EnclosedCountries)
GROUP BY country;

CREATE VIEW ExactlyOne AS
SELECT n.cid AS cid
FROM NeighborCount n
WHERE n.count=1;

INSERT INTO Query3
(SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name
	FROM ExactlyOne JOIN neighbour n ON ExactlyOne.cid=n.country
	JOIN country c1 ON c1.cid=n.country
	JOIN country c2 ON c2.cid=n.neighbor
	ORDER BY c1.cname ASC);


DROP VIEW ExactlyOne;
DROP VIEW NeighborCount;
DROP VIEW EnclosedCountries;

-- Query 4 statements

CREATE VIEW IndirectAccess AS
SELECT n.country AS cid, o.oid AS oid
FROM oceanAccess o JOIN neighbour n ON o.cid=n.neighbor;

CREATE VIEW AllAccesses AS
SELECT cid, oid FROM oceanAccess
UNION -- UNION ALL keeps duplicates.
SELECT cid, oid FROM IndirectAccess;

INSERT INTO Query4
(SELECT c.cname AS cname, o.oname AS oname
	FROM AllAccesses a JOIN ocean o ON a.oid=o.oid
	JOIN country c ON c.cid=a.cid
	ORDER BY c.cname ASC, o.oname DESC);

DROP VIEW AllAccesses;
DROP VIEW IndirectAccess;

-- Query 5 statements

CREATE VIEW FiveYearAvg AS
SELECT cid, avg(hdi_score) AS avghdi
FROM hdi
WHERE year IN (2009, 2010, 2011, 2012, 2013)
GROUP BY cid
ORDER BY avg(hdi_score) DESC LIMIT 10;

INSERT INTO Query5
(SELECT f.cid AS cid, c.cname AS cname, f.avghdi AS avghdi
	FROM FiveYearAvg f JOIN country c ON f.cid=c.cid
	ORDER BY f.avghdi DESC);

DROP VIEW FiveYearAvg;

-- Query 6 statements

CREATE VIEW NineToTen AS
SELECT h1.cid
FROM hdi h1 JOIN hdi h2 ON h1.cid=h2.cid
WHERE h1.year=2009 AND h2.year=2010 AND h2.hdi_score > h1.hdi_score;

CREATE VIEW TenToEle AS
SELECT h1.cid
FROM hdi h1 JOIN hdi h2 ON h1.cid=h2.cid
WHERE h1.year=2010 AND h2.year=2011 AND h2.hdi_score > h1.hdi_score;

CREATE VIEW EleToTwel AS
SELECT h1.cid
FROM hdi h1 JOIN hdi h2 ON h1.cid=h2.cid
WHERE h1.year=2011 AND h2.year=2012 AND h2.hdi_score > h1.hdi_score;

CREATE VIEW TwelToThir AS
SELECT h1.cid
FROM hdi h1 JOIN hdi h2 ON h1.cid=h2.cid
WHERE h1.year=2012 AND h2.year=2013 AND h2.hdi_score > h1.hdi_score;

CREATE VIEW PositiveChange AS
SELECT a.cid
FROM NineToTen as a, TenToEle as b, EleToTwel as c, TwelToThir as d
WHERE a.cid=b.cid AND b.cid=c.cid AND c.cid=d.cid AND a.cid=c.cid AND a.cid=d.cid AND b.cid=d.cid;

INSERT INTO Query6
(SELECT c.cid AS cid, c.cname AS cname
	FROM PositiveChange p JOIN country c ON c.cid=p.cid
	ORDER BY c.cname ASC);

DROP VIEW PositiveChange;
DROP VIEW NineToTen;
DROP VIEW TenToEle;
DROP VIEW EleToTwel;
DROP VIEW TwelToThir;

-- Query 7 statements

CREATE VIEW ReligionStats AS
SELECT r.rid AS rid, sum(r.rpercentage * c.population) AS followers
FROM country c JOIN religion r ON c.cid=r.cid
GROUP BY r.rid
ORDER BY sum(r.rpercentage * c.population) DESC;

INSERT INTO Query7
(SELECT rs.rid AS rid, r.rname AS rname, rs.followers AS followers
	FROM ReligionStats rs JOIN religion r ON rs.rid=r.rid
	ORDER BY rs.followers DESC);

DROP VIEW ReligionStats;

-- Query 8 statements

CREATE VIEW MostPopular AS
SELECT l2.cid AS cid, l2.lid AS lid
FROM language l1 JOIN language l2 ON l1.cid=l2.cid AND l2.lpercentage>l1.lpercentage;

CREATE VIEW SameLanguage AS
SELECT n.country AS c1name, n.neighbor AS c2name, m1.lid AS lid
FROM MostPopular m1 JOIN neighbour n ON m1.cid=n.country
JOIN MostPopular m2 ON m2.cid=n.neighbor
WHERE m1.lid=m2.lid
ORDER BY m1.lid;

INSERT INTO Query8
(SELECT s.c1name AS c1name, s.c2name AS c2name, l.lname AS lname
	FROM SameLanguage s JOIN language l on s.lid=l.lid
	ORDER BY l.lname ASC, s.c1name DESC);

DROP VIEW SameLanguage;
DROP VIEW MostPopular;

-- Query 9 statements

-- (Country ID, Deepest Ocean Depth)

CREATE VIEW DeepestOcean AS
SELECT c.cid AS cid, max(o.depth) AS depth
FROM oceanAccess oa JOIN ocean o on oa.oid=o.oid
JOIN country c ON c.cid=oa.cid
GROUP BY c.cid;

INSERT INTO Query9
(SELECT c.cname AS cname, d.depth+c.height AS totalspan
	FROM country c JOIN DeepestOcean d ON c.cid=d.cid
	ORDER BY d.depth+c.height DESC LIMIT 1);

DROP VIEW DeepestOcean;

-- Query 10 statements

INSERT INTO Query10
(SELECT neighbour.country AS cname, sum(length) AS borderslength
	FROM neighbour
	GROUP BY neighbour.country
	ORDER BY sum(length) DESC LIMIT 1);