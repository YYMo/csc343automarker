-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW neighbourList AS 
SELECT c2.cid AS c1id, c2.cname AS c1name, n.neighbor AS c2id, c.cname AS c2name, c.height AS c2height
FROM country c JOIN neighbour n ON c.cid=n.neighbor JOIN country c2 ON c2.cid=neighbour.country;

INSERT INTO Query1
(SELECT c1id,c1name,c2id,c2name
FROM neighbourList
GROUP BY c1name
HAVING c2height=max(c2height)
ORDER BY c1name ASC);

DROP VIEW neighbourList;

-- Query 2 statements

INSERT INTO Query2
(SELECT o1.cid AS cid, c1.cname AS cname
FROM country c1,oceanAccess o1
WHERE o1.cid=c1.cid AND o1.cid NOT IN(
  SELECT o2.cid
  FROM country c2,oceanAccess o2
  WHERE c2.cid=o2.cid)
ORDER BY cname ASC);

-- Query 3 statements

CREATE VIEW neighbourList AS 
SELECT c2.cid AS c1id, c2.cname AS c1name, n.neighbor AS c2id, c.cname AS c2name,
FROM country c JOIN neighbour n ON c.cid=n.neighbor JOIN country c2 ON c2.cid=neighbour.country;

INSERT INTO Query3
(SELECT c1id,c1name,c2id,c2name
FROM neighbourList
GROUP BY c1name
HAVING count(c1name)=1
ORDER BY c1name ASC);

DROP VIEW neighbourList;

-- Query 4 statements

CREATE VIEW hasCoastline AS
SELECT o1.cid AS cid, o1.oid AS oid
FROM oceanAccess o1
WHERE o1.cid=c1.cid AND o1.cid IN(
  SELECT o2.cid
  FROM country c2,oceanAccess o2
  WHERE c2.cid=o2.cid);
  
CREATE VIEW neighbourHasCoastline AS
SELECT neighbour.country AS cid, oceanAccess.oid AS oid
FROM oceanAccess,neighbour
WHERE neighbour.neighbor=oceanAccess.cid;

CREATE VIEW List AS
SELECT hasCoastline.cid AS cid, oceanAccess.oid AS oid FROM hasCoastline
UNION
SELECT neighbourHasCoastline.cid AS cid, oceanAccess.oid AS oid FROM neighbourHasCoastline;

INSERT INTO Query4
(SELECT c.cname,o.oname
FROM country c, List l, ocean o
WHERE l.oid=o.oid AND l.cid=c.cid
ORDER BY cname ASC, oname DESC);

DROP VIEW hasCoastline;
DROP VIEW neighbourHasCoastline;
DROP VIEW List;

-- Query 5 statements

CREATE VIEW hdiList AS
SELECT hdi.cid, avg(hdi_score)
FROM hdi
GROUP BY hdi.year
HAVING hdi.year<=2013 AND hdi.year>=2009
limit 10;

INSERT INTO Query5
(SELECT c.cid, c.cname, h.avghdi
FROM hdiList h,country c
WHERE c.cid=h.cid
ORDER BY h.avghdi DESC);

DROP VIEW hdiList;

-- Query 6 statements

INSERT INTO Query6
(SELECT h.cid AS cid, c.cname AS cname (DISTINCT h.cid)
FROM hdi h, country c 
WHERE c.cid=h.cid AND h.year<=2013 AND h.year>=2009 AND h.cid NOT IN(
  SELECT h1.cid
  FROM hdi h1 JOIN hdi h2 ON h1.year<h2.year AND h1.hdi_score > h2.hdi_score)
ORDER BY cname ASC);

-- Query 7 statements

INSERT INTO Query7
(SELECT r.rid, r.name, (sum(rpercentage)*c.population) AS followers
FROM country c, religion r
GROUP BY r.rid
HAVING r.cid=c.cid
ORDER BY followers DESC);

-- Query 8 statements

CREATE VIEW neighbourList AS 
SELECT c2.cid AS c1id, c2.cname AS c1name, n.neighbor AS c2id, c.cname AS c2name, c.height AS c2height
FROM country c JOIN neighbour n ON c.cid=n.neighbor JOIN country c2 ON c2.cid=neighbour.country;

SELECT n.c1name, n.c2name, l1.lname
FROM neighbourList n, language l1 JOIN language l2 ON l1.lpercentage=l2.lpercentage
WHERE (l1.cid=n.c1id AND l2.cid=n.c2id) OR (l1.cid=n.c2id AND l2.cid=n.c1id)
ORDER BY lname ASC, c1name DESC);

DROP VIEW neighbourList;

-- Query 9 statements



-- Query 10 statements

CREATE VIEW lengthSum AS 
SELECT n.country AS cid, sum(n.length) AS borderslength
FROM neighbor n 
GROUP BY n.country;

INSERT INTO Query10
(SELECT c.cname AS cname, max(l.borderslength) AS borderslength
FROM country c, lengthSum l
WHERE c.cid=l.cid
);

DROP VIEW lengthSum;
