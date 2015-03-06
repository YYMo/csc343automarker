-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW neighbourElevation AS
(SELECT country AS c1id, c.cid AS c2id, cname AS c2name
FROM neighbour n JOIN country c ON n.neighbor = c.cid
WHERE (country, height) IN 
(SELECT country AS c1id, max(height) AS height 
FROM neighbour n JOIN country c ON n.neighbor = c.cid
GROUP BY c1id)); 

INSERT INTO Query1
(SELECT c1id, cname AS c1name, c2id, c2name
FROM neighbourElevation ne JOIN country c ON ne.c1id = c.cid
ORDER BY c1name);

DROP VIEW IF EXISTS neighbourElevation CASCADE;

-- Query 2 statements
INSERT INTO Query2 (((SELECT cid, cname
FROM country)
EXCEPT
(SELECT c.cid, cname
FROM country c JOIN oceanAccess oa ON c.cid = oa.cid))
ORDER BY cname);

-- Query 3 statements
-- All landlocked countries (countries that do not have ocean access). 
CREATE VIEW landlocked as
((SELECT cid AS c1id, cname AS c1name
FROM country)
EXCEPT
(SELECT c.cid AS c1id, cname AS c1name
FROM country c JOIN oceanAccess oa ON c.cid = oa.cid));

INSERT INTO Query3 
(SELECT c1id, c1name, neighbor AS c2id, cname as c2name 
FROM (landlocked l JOIN neighbour n ON l.c1id = n.country)
JOIN country c ON n.neighbor = c.cid
GROUP BY c1id, c1name, c2id, c2name HAVING count(c1id) = 1
ORDER BY c1name);

DROP VIEW IF EXISTS landlocked CASCADE;

-- Query 4 statements
-- A table of countries who have neighbours that have access to an ocean.
CREATE VIEW neighbourAccess AS
(SELECT c.cid AS cid, cname, oid
FROM (neighbour n JOIN oceanAccess oa ON n.neighbor = oa.cid) noa 
JOIN country c ON c.cid = noa.country);

INSERT INTO Query4
(SELECT cname, oname
FROM ((SELECT c.cid AS cid, cname, oid 
FROM country c JOIN oceanAccess oa ON c.cid = oa.cid) 
UNION (SELECT * FROM neighbourAccess)) oac
JOIN ocean o ON oac.oid = o.oid
ORDER BY cname ASC, oname DESC);

DROP VIEW IF EXISTS neighbourAccess CASCADE;

-- Query 5 statements
-- A table of average HDI scores for each country from the year 2009 to 2013. 
CREATE VIEW averageHDI AS
(SELECT cid, avg(hdi_score) AS avghdi
FROM (SELECT cid, hdi_score
FROM hdi
WHERE year >= 2009 AND year <= 2013) cah
GROUP BY cid);

INSERT INTO Query5 (SELECT c.cid AS cid, cname, avghdi
FROM country c JOIN averageHDI ah ON c.cid = ah.cid
ORDER BY avghdi DESC LIMIT 10);

DROP VIEW IF EXISTS averageHDI CASCADE;

-- Query 6 statements
-- A table displaying HDI scores from the years 2009 to 2013.
CREATE VIEW fiveYears AS
(SELECT *
FROM hdi
WHERE year >= 2009 AND year <= 2013);

-- The below tables are labelled "xToY" and keep track of the countries where
-- from year x to Y where there was a positive increase.  
CREATE VIEW nineToTen AS
(SELECT fy1.cid AS cid, fy2.year AS year, fy2.hdi_score AS hdi_score
FROM fiveYears fy1 JOIN fiveYears fy2 ON ((fy1.year = 2009 AND fy2.year = 2010) 
AND (fy1.cid = fy2.cid))
WHERE fy2.hdi_score - fy1.hdi_score > 0);

CREATE VIEW tenToEleven AS
(SELECT ntt.cid AS cid, fy3.year AS year, fy3.hdi_score AS hdi_score
FROM nineToTen ntt JOIN fiveYears fy3 ON ((ntt.cid = fy3.cid)
AND (ntt.year = 2010 AND fy3.year = 2011))
WHERE fy3.hdi_score - ntt.hdi_score > 0);

CREATE VIEW elevenToTwelve AS
(SELECT tte.cid AS cid, fy4.year AS year, fy4.hdi_score AS hdi_score
FROM tenToEleven tte JOIN fiveYears fy4 ON ((tte.cid = fy4.cid)
AND (tte.year = 2011 AND fy4.year = 2012))
WHERE fy4.hdi_score - tte.hdi_score > 0);

CREATE VIEW twelveToThirteen AS
(SELECT ett.cid AS cid
FROM elevenToTwelve ett JOIN fiveYears fy5 ON ((ett.cid = fy5.cid)
AND (ett.year = 2012 AND fy5.year = 2013))
WHERE fy5.hdi_score - ett.hdi_score > 0);

INSERT INTO Query6
(SELECT c.cid AS cid, cname
FROM country c JOIN twelveToThirteen ttt ON c.cid = ttt.cid);

DROP VIEW IF EXISTS fiveYears CASCADE;
DROP VIEW IF EXISTS nineToTen CASCADE;
DROP VIEW IF EXISTS tenToEleven CASCADE;
DROP VIEW IF EXISTS elevenToTwelve CASCADE;
DROP VIEW IF EXISTS twelveToThirteen CASCADE; 

-- Query 7 statements
-- A table of the number of followers that follow each religion. 
CREATE VIEW numOfFollowers AS
(SELECT rid, sum(cfollowers) AS followers
FROM (SELECT rid, population * (rpercentage/100) AS cfollowers
FROM country c 
JOIN religion r ON c.cid = r.cid
GROUP BY rid, cfollowers) crf 
GROUP BY rid);

INSERT INTO Query7
(SELECT DISTINCT nof.rid, rname, followers
FROM numOfFollowers nof JOIN religion r ON nof.rid = r.rid
ORDER BY followers DESC);

DROP VIEW IF EXISTS numOfFollowers CASCADE;
 
-- Query 8 statements
-- A table of popular languages for each country.
CREATE VIEW popularLanguages AS
(SELECT c.cid AS cid, cname, lid, lname, max(lpercentage)  
FROM country c JOIN language l ON c.cid = l.cid
GROUP BY c.cid, cname, lid, lname);

-- A table of countries where two countries have the same most popular language.
CREATE VIEW samePopular AS
(SELECT pl1.cid AS c1id, pl1.cname AS c1name, 
pl2.cid AS c2id, pl2.cname AS c2name, pl1.lname AS lname
FROM popularLanguages pl1 JOIN popularLanguages pl2 
ON (pl1.cid <> pl2.cid AND pl1.lid = pl2.lid));

INSERT INTO Query8
(SELECT c1name, c2name, lname
FROM samePopular
WHERE (c1id, c2id) IN (SELECT country AS c1id, neighbor AS c2id FROM neighbour))
ORDER BY lname ASC, c1name DESC;

DROP VIEW IF EXISTS popularLanguages CASCADE;
DROP VIEW IF EXISTS samePopular CASCADE;
 
-- Query 9 statements
-- A table of countries that have no access to the ocean and setting their depth
-- to 0.
CREATE VIEW noOceanAccess AS
(SELECT c.cid AS cid, cname, height, 0 AS depth 
FROM ((SELECT cid FROM country)
EXCEPT
(SELECT cid FROM oceanAccess)) noa JOIN country c ON noa.cid = c.cid);

-- A table of countries that do have access to the ocean.
CREATE VIEW oceanAccInfo AS
(SELECT oac.cid AS cid, cname, height, depth
FROM (SELECT cid, depth
FROM oceanAccess oa JOIN ocean o ON oa.oid = o.oid) oac 
JOIN country c ON oac.cid = c.cid);

INSERT INTO Query9
(SELECT cname, height + depth AS totalspan
FROM ((SELECT * FROM noOceanAccess) UNION (SELECT * FROM oceanAccInfo)) si 
ORDER BY totalspan DESC LIMIT 1);

DROP VIEW IF EXISTS noOceanAccess CASCADE;
DROP VIEW IF EXISTS oceanAccInfo CASCADE;

-- Query 10 statements
INSERT INTO Query10
(SELECT cname, borderslength
FROM (SELECT country, sum(length) AS borderslength
FROM neighbour
GROUP BY country
ORDER BY borderslength DESC LIMIT 1) lbl JOIN country c ON lbl.country = c.cid);
