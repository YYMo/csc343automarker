-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW step1 AS 
SELECT c.cid c1id, c.cname c1name, n.neighbor c2id
FROM country c INNER JOIN neighbour n ON(c.cid = n.country)
;

CREATE VIEW aggr1 AS
SELECT step1.c1id, MAX(c.height) max_elev
FROM step1 INNER JOIN country c ON (step1.c2id = c.cid)
GROUP BY step1.c1id
;

INSERT INTO Query1 (
SELECT step1.c1id, step1.c1name, step1.c2id, c.cname c2name
FROM step1 INNER JOIN aggr1 ON(step1.c1id = aggr1.c1id)
INNER JOIN country c ON(step1.c2id = c.cid AND aggr1.max_elev = c.height)
ORDER BY step1.c1name ASC
);

-- SELECT * FROM Query1;

DROP VIEW IF EXISTS step1 CASCADE;
DROP VIEW IF EXISTS aggr1 CASCADE;

-- Query 2 statements

INSERT INTO Query2 (
SELECT c.cid, c.cname
FROM country c
WHERE c.cid IN (
SELECT c.cid FROM country c
EXCEPT
SELECT DISTINCT o.cid FROM oceanAccess o
)
ORDER BY c.cname ASC
);

-- SELECT * FROM Query2;

-- Query 3 statements

CREATE VIEW aggr1 AS
SELECT q.cid
FROM Query2 q INNER JOIN neighbour n ON(q.cid = n.country)
GROUP BY q.cid
HAVING COUNT(n.neighbor) = 1
;

INSERT INTO Query3 (
SELECT aggr1.cid c1id, q.cname c1name, n.neighbor c2id, c.cname c2name
FROM aggr1 INNER JOIN Query2 q ON (aggr1.cid = q.cid)
INNER JOIN neighbour n ON(aggr1.cid = n.country)
INNER JOIN country c ON(n.neighbor = c.cid)
ORDER BY q.cname
);

-- SELECT * FROM Query3;

DROP VIEW IF EXISTS aggr1 CASCADE;

-- Query 4 statements

CREATE VIEW step1 AS
SELECT n.country cid, oA.oid
FROM neighbour n INNER JOIN oceanAccess oA ON(n.neighbor = oA.cid)
UNION
SELECT oA.cid, oA.oid
FROM oceanAccess oA
;

INSERT INTO Query4 (
SELECT c.cname, o.oname
FROM country c INNER JOIN step1 ON(c.cid = step1.cid)
INNER JOIN ocean o ON(step1.oid = o.oid)
ORDER BY c.cname ASC, o.oname DESC
);

-- SELECT * FROM Query4;

DROP VIEW IF EXISTS step1 CASCADE;

-- Query 5 statements

CREATE VIEW step1 AS
SELECT h.cid
FROM hdi h
WHERE h.year = '2009'
INTERSECT
SELECT h.cid
FROM hdi h
WHERE h.year = '2010'
INTERSECT
SELECT h.cid
FROM hdi h
WHERE h.year = '2011'
INTERSECT
SELECT h.cid
FROM hdi h
WHERE h.year = '2012'
INTERSECT
SELECT h.cid
FROM hdi h
WHERE h.year = '2013'
;

CREATE VIEW aggr1 AS
SELECT step1.cid, SUM(h.hdi_score)/5.0 avghdi
FROM step1 INNER JOIN hdi h ON(step1.cid = h.cid)
GROUP BY step1.cid
;

INSERT INTO Query5 (
SELECT aggr1.cid, c.cname, aggr1.avghdi
FROM aggr1 INNER JOIN country c ON(aggr1.cid = c.cid)
ORDER BY aggr1.avghdi DESC
LIMIT 10
);

-- SELECT * FROM Query5;

DROP VIEW IF EXISTS step1 CASCADE;
DROP VIEW IF EXISTS aggr1 CASCADE;


-- Query 6 statements

CREATE VIEW step1 AS
SELECT h.cid
FROM hdi h
WHERE h.year = '2009'
INTERSECT
SELECT h.cid
FROM hdi h
WHERE h.year = '2010'
INTERSECT
SELECT h.cid
FROM hdi h
WHERE h.year = '2011'
INTERSECT
SELECT h.cid
FROM hdi h
WHERE h.year = '2012'
INTERSECT
SELECT h.cid
FROM hdi h
WHERE h.year = '2013'
;

CREATE VIEW step2 AS
SELECT step1.cid, h.year, h.hdi_score
FROM step1 INNER JOIN hdi h ON(step1.cid = h.cid) 
;

CREATE VIEW y9 AS
SELECT step2.cid, step2.hdi_score
FROM step2
WHERE step2.year = '2009'
;

CREATE VIEW y10 AS
SELECT step2.cid, step2.hdi_score
FROM step2
WHERE step2.year = '2010'
;

CREATE VIEW y11 AS
SELECT step2.cid, step2.hdi_score
FROM step2
WHERE step2.year = '2011'
;

CREATE VIEW y12 AS
SELECT step2.cid, step2.hdi_score
FROM step2
WHERE step2.year = '2012'
;

CREATE VIEW y13 AS
SELECT step2.cid, step2.hdi_score
FROM step2
WHERE step2.year = '2013'
;

CREATE VIEW step3 AS
SELECT y10.cid
FROM y9 INNER JOIN y10 ON(y9.cid = y10.cid)
WHERE y10.hdi_score > y9.hdi_score 
INTERSECT
SELECT y11.cid
FROM y10 INNER JOIN y11 ON(y10.cid = y11.cid)
WHERE y11.hdi_score > y10.hdi_score 
INTERSECT
SELECT y12.cid
FROM y11 INNER JOIN y12 ON(y11.cid = y12.cid)
WHERE y12.hdi_score > y11.hdi_score 
INTERSECT
SELECT y13.cid
FROM y12 INNER JOIN y13 ON(y12.cid = y13.cid)
WHERE y13.hdi_score > y12.hdi_score 
;

INSERT INTO Query6 (
SELECT step3.cid, c.cname
FROM step3 INNER JOIN country c ON(step3.cid = c.cid)
ORDER BY c.cname ASC
);

-- SELECT * FROM Query6;

DROP VIEW IF EXISTS step1 CASCADE;
DROP VIEW IF EXISTS step2 CASCADE;
DROP VIEW IF EXISTS y9 CASCADE;
DROP VIEW IF EXISTS y10 CASCADE;
DROP VIEW IF EXISTS y11 CASCADE;
DROP VIEW IF EXISTS y12 CASCADE;
DROP VIEW IF EXISTS y13 CASCADE;
DROP VIEW IF EXISTS step3 CASCADE;



-- Query 7 statements

CREATE VIEW aggr1 AS
SELECT r.rid, SUM(r.rpercentage * c.population / 100.0) followers
FROM country c INNER JOIN religion r ON(c.cid = r.cid)
GROUP BY r.rid
;

INSERT INTO Query7 (
SELECT aggr1.rid, r.rname, aggr1.followers
FROM aggr1 INNER JOIN religion r ON(aggr1.rid = r.rid)
ORDER BY followers DESC
);

-- SELECT * FROM Query7;

DROP VIEW IF EXISTS aggr1 CASCADE;

-- Query 8 statements

CREATE VIEW aggr1 AS
SELECT l.cid, MAX(l.lpercentage) lmax
FROM language l
GROUP BY l.cid
;

CREATE VIEW step1 AS
SELECT aggr1.cid, c.cname c1name, l.lid, n.neighbor
FROM aggr1 INNER JOIN language l ON(aggr1.cid = l.cid AND aggr1.lmax = l.lpercentage)
INNER JOIN country c ON(aggr1.cid = c.cid)
INNER JOIN neighbour n ON(aggr1.cid = n.country)
;

INSERT INTO Query8 (
SELECT step1.c1name, c.cname c2name, l.lname
FROM step1 INNER JOIN aggr1 ON(step1.neighbor = aggr1.cid)
INNER JOIN language l ON(step1.neighbor = l.cid AND aggr1.lmax = l.lpercentage)
INNER JOIN country c ON(step1.neighbor = c.cid)
WHERE step1.lid = l.lid
ORDER BY l.lname ASC, step1.c1name DESC
);

-- SELECT * FROM Query8;

DROP VIEW IF EXISTS aggr1 CASCADE;
DROP VIEW IF EXISTS step1 CASCADE;

-- Query 9 statements

CREATE VIEW aggr1 AS
SELECT oA.cid, MAX(o.depth) depth
FROM oceanAccess oA INNER JOIN ocean o ON(oA.oid = o.oid)
GROUP BY oA.cid
;

CREATE VIEW step1 AS
SELECT c.cname, c.height + aggr1.depth totalspan
FROM country c INNER JOIN aggr1 ON(c.cid = aggr1.cid)
;

CREATE VIEW step2 AS
SELECT c.cid
FROM country c
EXCEPT
SELECT oA.cid
FROM oceanAccess oA
;

CREATE VIEW step3 AS
SELECT c.cname, c.height totalspan
FROM country c INNER JOIN step2 ON(c.cid = step2.cid)
UNION
SELECT *
FROM step1
;

CREATE VIEW aggr2 AS
SELECT MAX(step3.totalspan) totalspan
FROM step3
;

INSERT INTO Query9 (
SELECT step3.cname, aggr2.totalspan
FROM step3 INNER JOIN aggr2 ON(step3.totalspan = aggr2.totalspan)
);

-- SELECT * FROM Query9;

DROP VIEW IF EXISTS aggr1 CASCADE;
DROP VIEW IF EXISTS step1 CASCADE;
DROP VIEW IF EXISTS step2 CASCADE;
DROP VIEW IF EXISTS step3 CASCADE;
DROP VIEW IF EXISTS aggr2 CASCADE;


-- Query 10 statements

CREATE VIEW aggr1 AS
SELECT n.country, SUM(n.length) borderslength
FROM neighbour n
GROUP BY n.country
;

CREATE VIEW aggr2 AS
SELECT MAX(aggr1.borderslength) borderslength
FROM aggr1
;

INSERT INTO Query10 (
SELECT c.cname, aggr2.borderslength
FROM aggr2 INNER JOIN aggr1 ON(aggr2.borderslength = aggr1.borderslength)
INNER JOIN country c ON(aggr1.country = c.cid)
);

-- SELECT * FROM Query10;

DROP VIEW IF EXISTS aggr1 CASCADE;
DROP VIEW IF EXISTS aggr2 CASCADE;
