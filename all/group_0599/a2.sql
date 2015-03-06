-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
DELETE FROM Query1;

CREATE VIEW neighbour_elev AS
SELECT n.country, c.cid, c.height
FROM neighbour n, country c
WHERE n.neighbor=c.cid;

CREATE VIEW neighbour_max_elev AS
SELECT n.country AS country, n.cid AS neighbour
FROM neighbour_elev n
WHERE height = (
SELECT MAX(height)
FROM neighbour_elev n2
WHERE n2.country=n.country
GROUP BY n2.country
);

INSERT INTO Query1
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name
FROM country c1, country c2, neighbour_max_elev n
WHERE c1.cid=n.country AND c2.cid=n.neighbour
ORDER BY c1name ASC;

DROP VIEW neighbour_max_elev;
DROP VIEW neighbour_elev;

-- Query 2 statements
DELETE FROM Query2;

INSERT INTO Query2
SELECT cid, cname 
FROM country
WHERE cid NOT IN(
SELECT cid
FROM oceanAccess)
ORDER BY cname ASC;

-- Query 3 statements
DELETE FROM Query3;

CREATE VIEW landlocked_countries AS
SELECT *
FROM country
WHERE cid NOT IN(
SELECT cid
FROM oceanAccess);

CREATE VIEW single_landlocked_cid AS
SELECT llc.cid 
FROM neighbour n, landlocked_countries llc
WHERE n.country = llc.cid
GROUP BY llc.cid
HAVING COUNT(n.neighbor) = 1;

INSERT INTO Query3
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name
FROM single_landlocked_cid sllc, country c1, neighbour n, country c2
WHERE sllc.cid = c1.cid AND sllc.cid = n.country AND n.neighbor = c2.cid
ORDER BY c1name ASC;

DROP VIEW single_landlocked_cid;
DROP VIEW landlocked_countries;

-- Query 4 statements
DELETE FROM Query4;

CREATE VIEW direct_access AS
SELECT c.cname, o.oname
FROM country c, ocean o, oceanAccess oa 
WHERE c.cid = oa.cid AND o.oid = oa.oid;

CREATE VIEW indirect_access AS
SELECT c1.cname AS cname, o.oname
FROM country c1, neighbour n, country c2, oceanAccess oa, ocean o
WHERE c1.cid = n.country AND n.neighbor = c2.cid
AND n.neighbor IN (SELECT cid FROM oceanAccess)
AND n.neighbor = oa.cid AND oa.oid = o.oid;

INSERT INTO Query4
(SELECT * FROM direct_access 
UNION 
SELECT * FROM indirect_access)
ORDER BY cname ASC, oname DESC;

DROP VIEW indirect_access;
DROP VIEW direct_access;

-- Query 5 statements
DELETE FROM Query5;

INSERT INTO Query5
SELECT h.cid AS cid, c.cname AS cname, AVG(hdi_score) AS avghdi
FROM hdi h, country c
WHERE h.cid = c.cid AND h.year >= 2009 AND h.year <= 2013
GROUP BY h.cid, c.cname
ORDER BY avghdi DESC
LIMIT 10;

-- Query 6 statements
DELETE FROM Query6;

CREATE VIEW thirteen AS
SELECT cid, hdi_score FROM hdi WHERE year = 2013;
CREATE VIEW twelve AS
SELECT cid, hdi_score FROM hdi WHERE year = 2012;
CREATE VIEW eleven AS
SELECT cid, hdi_score FROM hdi WHERE year = 2011;
CREATE VIEW ten AS
SELECT cid, hdi_score FROM hdi WHERE year = 2010;
CREATE VIEW nine AS
SELECT cid, hdi_score FROM hdi WHERE year = 2009;

CREATE VIEW increasing AS
SELECT thirteen.cid FROM thirteen, twelve, eleven, ten, nine 
WHERE thirteen.cid = twelve.cid AND
twelve.cid = eleven.cid AND
eleven.cid = ten.cid AND
ten.cid = nine.cid AND
thirteen.hdi_score > twelve.hdi_score AND
twelve.hdi_score > eleven.hdi_score AND
eleven.hdi_score > ten.hdi_score AND
ten.hdi_score > nine.hdi_score;

INSERT INTO Query6 
SELECT c.cid AS cid, c.cname AS cname
FROM country c, increasing i 
WHERE c.cid = i.cid 
GROUP BY c.cid, c.cname 
ORDER BY c.cname ASC;

DROP VIEW increasing;
DROP VIEW nine;
DROP VIEW ten;
DROP VIEW eleven;
DROP VIEW twelve;
DROP VIEW thirteen;

-- Query 7 statements
DELETE FROM Query7;

INSERT INTO Query7
SELECT r.rid AS rid, r.rname AS rname, SUM(r.rpercentage*c.population) AS followers
FROM religion r, country c
WHERE r.cid = c.cid 
GROUP BY r.rid, r.rname
ORDER BY followers DESC;

-- Query 8 statements
DELETE FROM Query8;

CREATE VIEW most_popular_language AS
SELECT l.cid AS cid, l.lid AS lid
FROM language l
WHERE l.lpercentage IN (
SELECT MAX(l2.lpercentage)
FROM language l2
GROUP BY l2.cid);

INSERT INTO Query8
SELECT DISTINCT c1.cname as c1name, c2.cname as c2name, l.lname as lname
FROM country c1, most_popular_language mpl1, 
neighbour n, language l,
country c2, most_popular_language mpl2
WHERE mpl1.cid = n.country AND c1.cid = mpl1.cid 
AND mpl2.cid = n.neighbor AND c2.cid = mpl2.cid
AND mpl1.lid = mpl2.lid and mpl1.lid = l.lid
ORDER BY lname ASC, c1name DESC;

DROP VIEW most_popular_language;

-- Query 9 statements
DELETE FROM Query9;

CREATE VIEW all_countries AS
SELECT c.cname AS cname, c.height AS height, 
CASE WHEN o.depth IS NULL THEN 0 ELSE o.depth END AS depth 
FROM country c LEFT JOIN oceanAccess oa ON c.cid = oa.cid
LEFT JOIN ocean o ON oa.oid = o.oid;

INSERT INTO Query9
SELECT ac.cname AS cname, ac.height+ac.depth AS span
FROM all_countries ac
WHERE ac.height+ac.depth IN (
SELECT MAX(ac2.height+ac2.depth) 
FROM all_countries ac2
);

DROP VIEW all_countries;

-- Query 10 statements
DELETE FROM Query10;

CREATE VIEW country_border_length AS 
SELECT c1.cname AS cname, SUM(n.length) AS borderslength
FROM country c1, neighbour n, country c2
WHERE c1.cid = n.country AND n.neighbor = c2.cid
GROUP BY c1.cid;

INSERT INTO Query10
SELECT * 
FROM country_border_length cbl
WHERE cbl.borderslength IN (
SELECT MAX(borderslength)
FROM country_border_length);

DROP VIEW country_border_length;

