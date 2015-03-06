-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW neighboursWithHeights AS
SELECT t1.cid AS cid1, t1.cname AS cname1, t2.neighbor, t3.cname AS cname2, t3.height
FROM country t1 FULL JOIN neighbour t2 ON t1.cid = t2.country
LEFT JOIN country t3 ON t3.cid = t2.neighbor;

INSERT INTO Query1
SELECT cid1, cname1, neighbor, cname2
FROM neighboursWithHeights
WHERE height = 
	(select max(height) FROM neighboursWithHeights AS nwh
		where nwh.cid1 = neighboursWithHeights.cid1)
ORDER BY cname1 ASC;

DROP VIEW IF EXISTS neighboursWithHeights CASCADE;


-- Query 2 statements
CREATE VIEW landlockedCIDs AS
SELECT cid
FROM country
WHERE cid NOT IN 
	(SELECT cid FROM oceanAccess);

INSERT INTO Query2
SELECT country.cid, country.cname 
FROM landlockedCIDs JOIN country ON country.cid = landlockedCIDs.cid
ORDER BY cname ASC;

DROP VIEW IF EXISTS landlockedCIDs CASCADE;


-- Query 3 statements
CREATE VIEW landlockedCIDs AS
SELECT cid
FROM country
WHERE cid NOT IN 
	(SELECT cid FROM oceanAccess);

CREATE VIEW landlockedCIDs2 AS
SELECT country.cid, country.cname 
FROM landlockedCIDs JOIN country ON country.cid = landlockedCIDs.cid;

CREATE VIEW atLeastOne AS
SELECT cid, cname, neighbor
FROM landlockedCIDs2 JOIN neighbour ON landlockedCIDs2.cid = neighbour.country;

CREATE VIEW atLeastTwo AS
SELECT cid, cname, atLeastOne.neighbor AS neighbour1, neighbour.neighbor AS neighbour2
FROM atLeastOne JOIN neighbour ON atLeastOne.cid = neighbour.country
WHERE atLeastOne.neighbor != neighbour.neighbor;

CREATE VIEW exactlyOne AS
(SELECT DISTINCT cid FROM atLeastOne) EXCEPT (SELECT DISTINCT cid FROM atLeastTwo);

CREATE VIEW exactlyOneWithCids AS
SELECT exactlyOne.cid, cname, neighbor
FROM exactlyOne JOIN neighbour ON exactlyOne.cid = country
	JOIN country ON exactlyOne.cid = country.cid;

INSERT INTO Query3
SELECT t1.cid, t1.cname, t2.cid, t2.cname
FROM exactlyOneWithCids t1 JOIN country t2 on t1.neighbor = t2.cid
ORDER BY t1.cname ASC;

DROP VIEW IF EXISTS landlockedCIDs CASCADE;

-- Query 4 statements
CREATE VIEW directAccess AS
SELECT t1.cname, t2.oname
FROM oceanaccess o JOIN country t1
ON o.cid = t1.cid
JOIN ocean t2
ON t2.oid = o.oid;

CREATE VIEW directAccessWithCIDs AS
SELECT directAccess.cname, oname, cid
FROM directAccess JOIN country on directAccess.cname = country.cname;

CREATE VIEW DAWithNeighbourCIDs AS
select cname, oname, cid, neighbor
FROM directAccessWithCIDs JOIN neighbour ON cid = country;

CREATE VIEW indirectAccess AS
SELECT country.cname, oname
FROM DAWithNeighbourCIDs JOIN country ON neighbor = country.cid;

INSERT INTO Query4
(SELECT * FROM directAccess) UNION (SELECT * FROM indirectAccess)
ORDER BY cname ASC, oname DESC;

DROP VIEW IF EXISTS directAccess CASCADE;


-- Query 5 statements
CREATE VIEW relevantYears AS
SELECT *
FROM hdi
WHERE year >= 2009 AND year <= 2013;

CREATE VIEW averageHDI AS
SELECT cid, avg(hdi_score) AS average
FROM relevantYears
GROUP BY cid
ORDER BY average desc
LIMIT 10;

INSERT INTO Query5
SELECT averageHDI.cid, cname, average
FROM averageHDI JOIN country
ON averageHDI.cid = country.cid
ORDER BY average DESC;

DROP VIEW IF EXISTS relevantYears CASCADE;

-- Query 6 statements
CREATE VIEW relevantYearsQ6 AS
SELECT *
FROM hdi
WHERE year >= 2009 AND year <= 2013;

CREATE VIEW fiveYearsIncreasing AS
SELECT h1.cid
FROM relevantYearsQ6 h1, relevantYearsQ6 h2, relevantYearsQ6 h3, relevantYearsQ6 h4, relevantYearsQ6 h5
WHERE h1.cid = h2.cid AND h2.cid = h3.cid AND h3.cid = h4.cid AND h4.cid = h5.cid AND
	h1.year < h2.year AND h2.year < h3.year AND h3.year < h4.year AND h4.year < h5.year AND
	h1.hdi_score < h2.hdi_score AND h2.hdi_score < h3.hdi_score AND h3.hdi_score < h4.hdi_score AND h4.hdi_score < h5.hdi_score;

INSERT INTO Query6
SELECT country.cid, cname
FROM fiveYearsIncreasing JOIN country ON fiveYearsIncreasing.cid = country.cid
ORDER BY cname ASC;

DROP VIEW IF EXISTS relevantYearsQ6 CASCADE;

-- Query 7 statements
CREATE VIEW followersByCountry AS
SELECT rid, (rpercentage * population) AS followers
FROM religion JOIN country ON country.cid = religion.cid;

CREATE VIEW groups AS
SELECT rid, sum(followers)
FROM followersByCountry
GROUP BY rid;

INSERT INTO Query7
SELECT DISTINCT groups.rid, rname, sum
FROM groups JOIN religion ON groups.rid = religion.rid
ORDER BY sum DESC;

DROP VIEW IF EXISTS followersByCountry CASCADE;

-- Query 8 statements
CREATE VIEW mostPopularLanguage AS
SELECT cid, lid
FROM language
WHERE lpercentage = 
	(SELECT max(lpercentage)
	FROM language AS l1
	WHERE l1.cid = language.cid);

CREATE VIEW neighbourPairs AS
SELECT cid, lid, neighbor
FROM mostPopularLanguage JOIN neighbour ON cid = country;

CREATE VIEW neighbourPairs2 AS
SELECT t1.cid, t1.lid as lid1, t1.neighbor, t2.lid AS lid2 
FROM neighbourPairs t1 join mostPopularLanguage t2 
	on t1.neighbor = t2.cid
WHERE t1.lid = t2.lid;

CREATE VIEW distinctLanguages AS
SELECT DISTINCT lid, lname
FROM language;

INSERT INTO Query8
SELECT t1.cname, t2.cname, lname
FROM neighbourPairs2 JOIN country t1 ON
	neighbourPairs2.cid = t1.cid
	JOIN country t2 
	ON t2.cid = neighbor
	JOIN distinctLanguages 
	ON neighbourPairs2.lid1 = distinctLanguages.lid
ORDER BY lname ASC, t1.cname DESC;

DROP VIEW IF EXISTS mostPopularLanguage CASCADE;
DROP VIEW IF EXISTS distinctLanguages CASCADE;

-- Query 9 statements
CREATE VIEW landlockedDepths AS
SELECT cid, cname, cast('0' AS integer) AS depth
FROM Query2;

CREATE VIEW allDepths AS
(SELECT t1.cid, cname, depth
FROM oceanAccess t1 JOIN ocean
	ON ocean.oid = t1.oid
	JOIN country
	ON t1.cid = country.cid) UNION
(SELECT * FROM landlockedDepths);

CREATE VIEW spans AS
SELECT t1.cname, (depth+height) AS span
FROM allDepths t1
	JOIN country
	ON t1.cid = country.cid; 

INSERT INTO Query9
SELECT cname, span
FROM spans
WHERE span >= ALL
	(SELECT span FROM spans);

DROP VIEW IF EXISTS landlockedDepths CASCADE;

-- Query 10 statements
CREATE VIEW borderLengths AS
SELECT country, sum(length)
FROM neighbour
GROUP BY country;

CREATE VIEW borderLengthsWithNames AS
SELECT cname, sum
FROM borderLengths JOIN country ON
	cid = country;

INSERT INTO Query10
SELECT cname, sum
FROM borderLengthsWithNames
WHERE sum >= ALL
	(SELECT sum
	 FROM borderLengthsWithNames);

DROP VIEW IF EXISTS borderLengths CASCADE;