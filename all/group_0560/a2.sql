-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW neighbourHeight (c1id,c2id,c2name,c2height) AS
SELECT n.country AS c1id, n.neighbor AS c2id, c.cname AS c2name, c.height AS c2height
FROM country c, neighbour n
WHERE n.neighbor = c.cid;

CREATE VIEW maxHeight (c1id,c2id,c2name) AS
SELECT n1.c1id, n1.c2id, n1.c2name
FROM neighbourHeight n1
WHERE n1.c2height >= ALL (
	SELECT n2.c2height FROM neighbourHeight n2
	WHERE n2.c1id = n1.c1id);

INSERT INTO Query1(SELECT n.c1id, c.cname AS c1name, n.c2id, n.c2name
FROM maxHeight n, country c
WHERE n.c1id = c.cid
ORDER BY c1name ASC);

DROP VIEW maxHeight;
DROP VIEW neighbourHeight;


-- Query 2 statements

CREATE VIEW landlockID (cid) AS
SELECT cid FROM country c
EXCEPT 
SELECT cid FROM oceanAccess o;

INSERT INTO Query2(SELECT l.cid, cname
FROM landlockID l, country c
WHERE l.cid = c.cid
ORDER BY cname ASC);

DROP VIEW landlockID;

-- Query 3 statements

CREATE VIEW landlockID (cid) AS
SELECT cid FROM country c
EXCEPT 
SELECT cid FROM oceanAccess o;

CREATE VIEW landlockWithNeighbour (c1id,c2id) AS
SELECT l.cid AS c1id, n.neighbor AS c2id
FROM landlockID l, neighbour n
WHERE l.cid = n.country;

CREATE VIEW exactlyOne (c1id,c2id) AS
SELECT * FROM landlockWithNeighbour l
WHERE NOT EXISTS (
	SELECT * FROM landlockWithNeighbour l2
	WHERE l2.c1id = l.c1id AND l2.c2id <> l.c2id);

INSERT INTO Query3(SELECT e.c1id, c1.cname AS c1name, e.c2id, c2.cname AS c2name
FROM exactlyOne e, country c1, country c2
WHERE e.c1id = c1.cid AND e.c2id = c2.cid
ORDER BY c1name ASC);

DROP VIEW exactlyOne;
DROP VIEW landlockWithNeighbour;
DROP VIEW landlockID;



-- Query 4 statements

CREATE VIEW neighborWithOcean (c1id,c2id,oid) AS
SELECT n.country AS c1id, n.neighbor AS c2id, o.oid
FROM neighbour n, oceanAccess o
WHERE n.neighbor = o.cid;

CREATE VIEW accessID (cid,oid) AS
SELECT * FROM oceanAccess
UNION
SELECT c1id AS cid, oid FROM neighborWithOcean;

INSERT INTO Query4(SELECT cname, oname
FROM accessID a, country c, ocean o
WHERE a.cid = c.cid AND a.oid=o.oid
ORDER BY cname ASC, oname DESC);

DROP VIEW accessID;
DROP VIEW neighborWithOcean;

-- Query 5 statements

CREATE VIEW fiveYears (cid, year, hdi_score) AS
SELECT * FROM hdi
WHERE year >= 2009 AND year <= 2013;

CREATE VIEW topAverage (cid, avghdi) AS
SELECT cid, avg(hdi_score) AS avghdi
FROM fiveYears
GROUP BY cid
ORDER BY avghdi DESC LIMIT 10;

INSERT INTO Query5(SELECT t.cid, cname, avghdi
FROM topAverage t, country c
WHERE t.cid = c.cid);

DROP VIEW topAverage;
DROP VIEW fiveYears;

-- Query 6 statements

CREATE VIEW fiveYears (cid, year, hdi_score) AS
SELECT * FROM hdi
WHERE year >= 2009 AND year <= 2013;

CREATE VIEW increasing (cid) AS
SELECT DISTINCT f.cid
FROM fiveYears f
WHERE NOT EXISTS (
	SELECT * FROM fiveYears f2, FiveYears f3
	WHERE f2.cid = f3.cid AND f2.year>f3.year AND f2.hdi_score < f3.hdi_score AND f2.cid = f.cid
);

INSERT INTO Query6(SELECT i.cid, cname
FROM increasing i, country c
WHERE i.cid = c.cid
ORDER BY cname ASC);

DROP VIEW increasing;
DROP VIEW fiveYears;

-- Query 7 statements

CREATE VIEW religionInCountry (cid,rid,pop) AS
SELECT r.cid, r.rid, c.population*r.rpercentage as pop
FROM country c, religion r
WHERE r.cid = c.cid;

CREATE VIEW religionPop (rid,followers) AS
SELECT rid, sum(pop) as followers
FROM religionInCountry
GROUP BY rid;

INSERT INTO Query7(SELECT DISTINCT p.rid,rname,followers
FROM religionPop p, religion r
WHERE p.rid = r.rid
ORDER BY followers DESC);

DROP VIEW religionPop;
DROP VIEW religionInCountry;


-- Query 8 statements

CREATE VIEW popularLanguage (c1id,c1name,l1id,l1name) AS
SELECT l1.cid AS c1id, c.cname AS c1name, l1.lid AS l1id, l1.lname AS l1name
FROM language l1, country c
WHERE c.cid = l1.cid AND lpercentage >= ALL(SELECT lpercentage FROM language l2 WHERE l1.cid = l2.cid);

CREATE VIEW neighborLanguage (c1id, c2id, c2name,l2name) AS
SELECT n.country AS c1id, n.neighbor AS c2id, c.cname AS c2name, p.l1name AS l2name
FROM popularLanguage p, neighbour n, country c
WHERE n.neighbor = p.c1id AND n.neighbor = c.cid;

INSERT INTO Query8(SELECT p.c1name, n.c2name, p.l1name AS lname
FROM popularLanguage p,neighborLanguage n
WHERE p.c1id = n.c1id AND p.l1name = n.l2name
ORDER BY lname ASC, c1name DESC);

DROP VIEW neighborLanguage;
DROP VIEW popularLanguage;



-- Query 9 statements

CREATE VIEW countryDepth (cid,oid,depth)AS 
SELECT cid, o.oid, depth
FROM oceanAccess oa, ocean o
WHERE o.oid = oa.oid;

CREATE VIEW maxDepth (cid,oid,depth) AS
SELECT c1.cid, c1.oid, c1.depth
FROM countryDepth c1
WHERE c1.depth >=ALL (SELECT c2.depth FROM countryDepth c2 WHERE c1.cid =c2.cid);

CREATE VIEW countryDiff (cid,cname,span) AS
SELECT c.cid, cname,c.height+cd.depth AS span
FROM country c, maxDepth cd
WHERE c.cid = cd.cid;

CREATE VIEW landlockID (cid) AS
SELECT cid FROM country c
EXCEPT 
SELECT cid FROM oceanAccess o;

CREATE VIEW landlockDiff (cid,cname,span) AS
SELECT cid, cname, height AS span
FROM country;

CREATE VIEW allDiff (cid,cname,span) AS
SELECT * FROM countryDiff
UNION
SELECT * FROM landlockDiff;

INSERT INTO Query9(SELECT cname, span AS totalspan
FROM allDiff a
WHERE  a.span >=ALL (SELECT a2.span FROM allDiff a2));

DROP VIEW allDiff;
DROP VIEW landlockDiff;
DROP VIEW landlockID;
DROP VIEW countryDiff;
DROP VIEW maxDepth;
DROP VIEW countryDepth;



-- Query 10 statements

CREATE VIEW totalBorder (cid,cname,borderslength) AS
SELECT country AS cid, cname,sum(length) AS borderslength
FROM neighbour n, country c
WHERE n.country = c.cid
GROUP BY country, cname;

INSERT INTO Query10(SELECT cname, borderslength
FROM totalBorder
WHERE borderslength >= ALL(SELECT borderslength FROM totalBorder));

DROP VIEW totalBorder;
