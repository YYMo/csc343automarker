-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.
set search_path to a2;

-- Query 1 statements
CREATE VIEW CNE as
SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name, c2.height as elevation
FROM country c1, neighbour n, country c2
WHERE c1.cid = n.country AND n.neighbor = c2.cid;

CREATE VIEW CHE as 
SELECT c1id, MAX(elevation) as elevation
FROM CNE
Group by c1id;

INSERT INTO Query1(
SELECT CNE.c1id as c1id, CNE.c1name as C1name, CNE.c2id as c2id, CNE.c2name as c2name
FROM CNE,CHE
WHERE CNE.c1id = CHE.c1id and CNE.elevation = CHE.elevation
ORDER by c1name);

DROP VIEW CHE;
DROP VIEW CNE;



-- Query 2 statements
CREATE VIEW RCID as
(SELECT cid FROM country) EXCEPT (SELECT cid FROM oceanaccess Group by cid);

INSERT INTO Query2
(SELECT c1.cid as cid, c1.cname as cname
FROM country as c1, RCID as c2
WHERE c1.cid = c2.cid
ORDER by cname);

DROP VIEW RCID;



-- Query 3 statements
CREATE VIEW RCID as
(SELECT cid FROM country) EXCEPT (SELECT cid FROM oceanaccess Group by cid);

CREATE VIEW LLC as 
SELECT c1.cid as cid, c1.cname as cname
FROM country as c1, RCID as c2
WHERE c1.cid = c2.cid
Order by cname;

CREATE VIEW LLCN as
SELECT c1.cid as c1id, count(c2.neighbor) as nn
FROM llc as c1, neighbour as c2 
WHERE c1.cid = c2.country 
GROUP by c1.cid;

INSERT INTO Query3
(SELECT c1.c1id as c1id, c2.cname as c1name, c3.cid as c2id, c3.cname as c2name
FROM llcn as c1, country as c2, neighbour as n, country as c3
WHERE c1.nn = 1 and c1.c1id = c2.cid and c1.c1id = n.country and c3.cid = n.neighbor
ORDER by c2.cname);

DROP VIEW LLCN;
DROP VIEW LLC;
DROP VIEW RCID;

-- Query 4 statements
CREATE VIEW DA as
SELECT cid, oid 
FROM oceanaccess;

CREATE VIEW IDA as 
SELECT o1.cid as cid, o2.oid as oid
FROM oceanaccess o1, neighbour n, oceanaccess o2
WHERE o1.cid = n.country AND n.neighbor = o2.cid;

CREATE VIEW OA as 
SELECT *
FROM DA UNION (SELECT * FROM IDA);

INSERT INTO Query4(
SELECT c.cname as cname, o.oname as oname
FROM OA, country c, ocean o
WHERE OA.cid = c.cid AND OA.oid = o.oid
ORDER by c.cname, o.oname DESC);

DROP VIEW OA;
DROP VIEW IDA; 
DROP VIEW DA;


-- Query 5 statements

CREATE VIEW result as
SELECT cid, AVG(hdi_score) as avghdi
FROM hdi
WHERE year>2008 AND year<2014
Group by cid
ORDER by avghdi DESC
LIMIT 10;

INSERT INTO Query5(
SELECT r.cid as cid, c.cname as cname, r.avghdi as avghdi
FROM result r, country c
WHERE r.cid = c.cid
ORDER by avghdi DESC);

DROP VIEW result;



-- Query 6 statements

CREATE VIEW result as
SELECT h1.cid from hdi h1, hdi h2, hdi h3, hdi h4, hdi h5 
where h1.cid = h2.cid and h2.cid = h3.cid and h3.cid  = h4.cid and h4.cid = h5.cid 
and h1.hdi_score <h2.hdi_score and h2.hdi_score < h3.hdi_score and h3.hdi_score < h4.hdi_score and h4.hdi_score < h5.hdi_score 
and h1.year = 2009 and h2.year = 2010 and h3.year = 2011 and h4.year = 2012 and h5.year = 2013;

INSERT INTO Query6(
SELECT r.cid as cid, c.cname as cname
FROM result r, country c
WHERE r.cid = c.cid 
ORDER by cname);

DROP VIEW result;

-- Query 7 statements

INSERT INTO Query7(
SELECT r.rid as rid, r.rname as rname, SUM(r.rpercentage * c.population) as followers
FROM religion r, country c
WHERE r.cid = c.cid
GROUP by r.rid, r.rname
ORDER by followers DESC);


-- Query 8 statements
CREATE VIEW NMPL as 
SELECT l1.cid, l1.lid
FROM language l1, language l2
WHERE l1.cid = l2.cid AND l1.lid <> l2.lid AND l1.lpercentage < l2.lpercentage
GROUP by l1.cid,l1.lid;

CREATE VIEW MPL as
(SELECT cid, lid
FROM language) EXCEPT
(SELECT * FROM NMPL);

INSERT INTO Query8(
SELECT c1.cname as c1name, c2.cname as c2name, lan.lname as lname
FROM MPL as l1, MPL as l2, language as lan, country as c1, country as c2
WHERE l1.lid = l2.lid AND l1.cid <> l2.cid 
AND l1.lid = lan.lid
AND c1.cid = l1.cid AND c2.cid = l2.cid
GROUP by c1.cname, c2.cname, lan.lname
ORDER BY lan.lname, C1.cname DESC);

DROP VIEW MPL;
DROP VIEW NMPL;


-- Query 9 statements

CREATE VIEW DPO as 
SELECT c.cid, MAX(o.depth) as mdepth
FROM country c, oceanaccess oa, ocean o
WHERE c.cid = oa.cid AND oa.oid = o.oid
GROUP by c.cid;

CREATE VIEW LLCID as
(SELECT cid FROM country) EXCEPT (SELECT cid FROM oceanaccess Group by cid);

CREATE VIEW NOA as
SELECT cid, 0 as mdepth
FROM LLCID;

CREATE VIEW AO as 
(SELECT * FROM NOA) UNION (SELECT * FROM DPO);

INSERT INTO Query9(
SELECT c.cname as cname, c.height + o.mdepth as totalspan
FROM country as c, AO as o
WHERE c.cid = o.cid);

DROP VIEW AO;
DROP VIEW NOA;
DROP VIEW LLCID;
DROP VIEW DPO;

-- Query 10 statements

CREATE VIEW TB as 
SELECT c.cid, SUM(n.length)
FROM country c, neighbour n
WHERE c.cid = n.country
GROUP by c.cid;

CREATE VIEW LTB as
SELECT MAX(sum) as LTB
FROM TB;

INSERT INTO Query10(
SELECT c.cname as cname, b.sum as borderslength
FROM country c, TB b, LTB
WHERE c.cid = b.cid AND b.sum = LTB.LTB);

DROP VIEW LTB;
DROP VIEW TB;