-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

SET search_path TO a2;

-- Query 1 statements
CREATE VIEW nheight AS SELECT country, neighbor, height FROM
country c JOIN neighbour n ON c.cid = n.neighbor;

CREATE VIEW cids AS SELECT country AS c1id, cid AS c2id FROM
(SELECT country, max(height) FROM nheight GROUP BY country) maxes
JOIN country c ON c.height = maxes.max;

CREATE VIEW c1 AS SELECT cids.c1id, c.cname AS c1name, cids.c2id 
FROM cids JOIN country c ON cids.c1id = c.cid;

CREATE VIEW c2 AS SELECT c1id, c1name, c2id, c.cname AS c2name FROM
c1 JOIN country c ON c1.c2id = c.cid ORDER BY c1name ASC;

INSERT INTO Query1 (SELECT * FROM c2);

DROP VIEW c2;
DROP VIEW c1;
DROP VIEW cids;
DROP VIEW nheight;
-- Query 2 statements

INSERT INTO Query2 ((SELECT cid, cname FROM country) EXCEPT 
(SELECT c.cid, cname FROM oceanAccess o JOIN country c ON 
c.cid = o.cid) ORDER BY cid ASC);

-- Query 3 statements

CREATE VIEW q2 AS (SELECT cid, cname FROM country) EXCEPT (SELECT 
c.cid, cname FROM oceanAccess o JOIN country c ON c.cid = o.cid);

CREATE VIEW landneighbours AS SELECT q2.cid, q2.cname, n.neighbor 
FROM q2 JOIN neighbour n ON q2.cid = n.country;

CREATE VIEW lonely AS SELECT * FROM (SELECT cid, cname, 
count(neighbor) FROM landneighbours GROUP BY cid, cname) ln WHERE 
count = 1;

CREATE VIEW onlyneighbour AS SELECT l.cid AS c1id, l.cname AS 
c1name, n.neighbor AS c2id FROM lonely l JOIN neighbour n ON 
l.cid = n.country;

INSERT INTO Query3 (SELECT c1id, c1name, c2id, c.cname AS c2name 
FROM onlyneighbour o JOIN country c ON o.c2id = c.cid ORDER BY c1name
ASC);

DROP VIEW onlyneighbour;
DROP VIEW lonely;
DROP VIEW landneighbours;
DROP VIEW q2;

-- Query 4 statements

CREATE VIEW direct AS SELECT cid, cname, o.oname AS oname FROM 
(SELECT c.cid, c.cname AS cname, oid FROM country c JOIN 
oceanAccess oA ON c.cid = oA.cid) coa JOIN ocean o ON coa.oid = o.oid;

CREATE VIEW indirect1 AS SELECT n.country AS cid, d.oname FROM 
direct d JOIN neighbour n ON d.cid = n.neighbor;

CREATE VIEW indirect2 AS SELECT c.cname AS cname, i.oname AS oname
FROM indirect1 i JOIN country c ON i.cid = c.cid;

CREATE VIEW ans4 AS SELECT * FROM ((SELECT cname, oname FROM 
direct) UNION (SELECT * FROM indirect2)) i2 ORDER BY cname ASC, 
oname DESC;

INSERT INTO Query4 (SELECT * FROM ans4);

DROP VIEW ans4;
DROP VIEW indirect2;
DROP VIEW indirect1;
DROP VIEW direct;

-- Query 5 statements

CREATE VIEW hdi913 AS SELECT * FROM hdi WHERE year <= 2013 AND 
year >= 2009;

CREATE VIEW avghditable AS SELECT cid, avg(hdi_score) AS avghdi 
FROM hdi913 GROUP BY cid ORDER BY avghdi DESC LIMIT 10;

INSERT INTO Query5 (SELECT c.cid AS cid, c.cname AS cname, a.avghdi
AS avghdi FROM country c JOIN avghditable a ON c.cid = a.cid ORDER 
BY avghdi DESC);

DROP VIEW avghditable;
DROP VIEW hdi913;

-- Query 6 statements

CREATE VIEW hdi910 AS SELECT h1.cid, h2.hdi_score FROM hdi h1 
JOIN hdi h2 ON h1.hdi_score < h2.hdi_score AND h1.cid = h2.cid WHERE 
h1.year = 2009 AND h2.year = 2010;

CREATE VIEW hdi1011 AS SELECT h1.cid, h2.hdi_score FROM 
hdi910 h1 JOIN hdi h2 ON h1.hdi_score < h2.hdi_score AND 
h1.cid = h2.cid WHERE h2.year = 2011;

CREATE VIEW hdi1112 AS SELECT h1.cid, h2.hdi_score FROM 
hdi1011 h1 JOIN hdi h2 ON h1.hdi_score < h2.hdi_score AND 
h1.cid = h2.cid WHERE h2.year = 2012;

CREATE VIEW increasers AS SELECT h1.cid AS cid FROM 
hdi1112 h1 JOIN hdi h2 ON h1.hdi_score < h2.hdi_score AND 
h1.cid = h2.cid WHERE h2.year = 2013;

INSERT INTO Query6 (SELECT i.cid, cname FROM increasers i JOIN
country c ON i.cid = c.cid ORDER BY cname ASC);

DROP VIEW increasers;
DROP VIEW hdi1112;
DROP VIEW hdi1011;
DROP VIEW hdi910;

-- Query 7 statements

CREATE VIEW fBYc AS SELECT r.cid, r.rid, 
(c.population * r.rpercentage) AS followers FROM religion r JOIN
country c ON c.cid = r.cid;

CREATE VIEW fBYr AS SELECT f.rid, r.rname, sum(f.followers) AS 
followers FROM fBYc f JOIN religion r ON f.rid = r.rid GROUP BY 
f.rid, r.rname ORDER BY followers DESC;

INSERT INTO Query7 (SELECT * FROM fBYr);

DROP VIEW fBYr;
DROP VIEW fBYc;

-- Query 8 statements

CREATE VIEW language1 AS SELECT l.cid, lname FROM (SELECT cid, 
max(lpercentage) FROM language GROUP BY cid) maxlang JOIN 
language l ON l.lpercentage = maxlang.max AND maxlang.cid = l.cid;

CREATE VIEW pair1 AS SELECT n.country AS c1id, n.neighbor AS c2id,
l1.lname FROM language1 l1 JOIN neighbour n ON l1.cid = n.country;

CREATE VIEW pair2 AS SELECT c1id, c2id, p1.lname FROM pair1 p1 JOIN 
language1 l1 ON p1.c2id = l1.cid AND p1.lname = l1.lname;

CREATE VIEW namepair1 AS SELECT c.cname AS c1name, c2id, lname FROM
pair2 p2 JOIN country c ON c.cid = p2.c1id;

CREATE VIEW namepairs AS SELECT c1name, c.cname AS c2name, lname 
FROM namepair1 np JOIN country c ON c.cid = np.c2id ORDER BY lname 
ASC, c1name DESC;

INSERT INTO Query8 (SELECT * FROM namepairs);

DROP VIEW namepairs;
DROP VIEW namepair1;
DROP VIEW pair2;
DROP VIEW pair1;
DROP VIEW language1;

-- Query 9 statements
CREATE VIEW directs AS SELECT cid, depth FROM oceanAccess oa
JOIN ocean o ON o.oid = oa.oid;

CREATE VIEW direct2 AS SELECT cname, (height + depth) AS span FROM
directs d JOIN country c ON d.cid = c.cid;

CREATE VIEW spans AS SELECT cname, max(span) AS totalspan FROM 
direct2 GROUP BY cname;

CREATE VIEW ans9 AS (SELECT * FROM spans) UNION (SELECT cname, 
height AS totalspan FROM country WHERE NOT EXISTS (SELECT * FROM
spans));

-- Assumes question asked for country with largest span.
INSERT INTO Query9 (SELECT * FROM ans9 ORDER BY totalspan DESC
LIMIT 1);

DROP VIEW ans9;
DROP VIEW spans;
DROP VIEW direct2;
DROP VIEW directs;

-- Query 10 statements

CREATE VIEW lengths AS SELECT country, sum(length) AS borderslength 
FROM neighbour GROUP BY country ORDER BY borderslength DESC LIMIT 1;

INSERT INTO Query10 (SELECT cname, borderslength FROM lengths l 
JOIN country c ON c.cid = l.country);

DROP VIEW lengths;
