-- Add below your SQL statements.
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW S1 AS (
  SELECT n.country AS c1id, c.cid AS c2id, c.cname AS c2name, c.height AS c2height
  FROM neighbour n, country c
  WHERE n.neighbor=c.cid
);

CREATE VIEW S2 AS (
  SELECT c1id, MAX(c2height) AS c2height
  FROM S1
  GROUP BY c1id
);

CREATE VIEW S3 AS (
  SELECT S2.c1id, c.cname AS c1name, S2.c2height
  FROM S2, country c
  WHERE S2.c1id=c.cid
);

CREATE VIEW S4 AS (
  SELECT DISTINCT S3.c1id, S3.c1name, S1.c2id, S1.c2name
  FROM S3, S1
  WHERE S3.c1id=S1.c1id AND S3.c2height=S1.c2height
  ORDER BY S3.c1name
);

INSERT INTO Query1 (
  SELECT * FROM S4
);

DROP VIEW IF EXISTS S1, S2, S3, S4 CASCADE;

-- Query 2 statements

INSERT INTO Query2 (
  SELECT cid, cname
  FROM country
  WHERE country.cid NOT IN (SELECT cid FROM oceanAccess)
);

-- Query 3 statements

CREATE VIEW S1 AS (
  SELECT cid AS c1id, cname AS c1name
  FROM country
  WHERE country.cid NOT IN (SELECT cid FROM oceanAccess)
);

CREATE VIEW S2 AS (
  SELECT S1.c1id, S1.c1name, n.neighbor AS c2id
  FROM S1, neighbour n
  WHERE S1.c1id=n.country
);

CREATE VIEW S3 AS (
  SELECT c1id
  FROM S2
  GROUP BY c1id
  HAVING count(c1id) = 1
);

CREATE VIEW S4 AS (
  SELECT *
  FROM S2
  WHERE c1id IN (SELECT c1id FROM S3)
);

CREATE VIEW S5 AS (
  SELECT S4.c1id, S4.c1name, S4.c2id, c.cname AS c2name
  FROM S4, country c
  WHERE S4.c2id=c.cid
  ORDER BY c1name
);

INSERT INTO Query3 (
  SELECT * FROM S5
);

DROP VIEW IF EXISTS S1, S2, S3, S4, S5 CASCADE;

-- Query 4 statements

INSERT INTO Query4 (
  (SELECT neighbor, oid
  FROM oceanaccess, neighbour
  WHERE oceanaccess.cid = neighbour.country)
      UNION
  (SELECT cid, oid FROM oceanaccess)
);

-- Query 5 statements

CREATE VIEW S1 AS (
  SELECT *
  FROM hdi
  WHERE year > 2008 AND year < 2014
);

CREATE VIEW S2 AS (
  SELECT cid, AVG(hdi_score) AS avghdi
  FROM S1
  GROUP BY cid
);

CREATE VIEW S3 AS (
  SELECT S2.cid, c.cname, S2.avghdi
  FROM S2, country c
  WHERE S2.cid=c.cid
  ORDER BY S2.avghdi DESC
  LIMIT 10
);

INSERT INTO Query5 (
  SELECT * FROM S3
);

DROP VIEW IF EXISTS S1, S2, S3 CASCADE;

-- Query 6 statements

INSERT INTO Query6 (
  SELECT DISTINCT h1.cid, country.cname
  FROM hdi h1, country
  WHERE country.cid = h1.cid
  AND
  ((SELECT h2.hdi_score
  FROM hdi h2
  WHERE year = 2009
  AND h2.cid = h1.cid) <
  (SELECT h2.hdi_score
  FROM hdi h2
  WHERE year = 2010
  AND h2.cid = h1.cid))
  AND
  ((SELECT h2.hdi_score
  FROM hdi h2
  WHERE year = 2010
  AND h2.cid = h1.cid) <
  (SELECT h2.hdi_score
  FROM hdi h2
  WHERE year = 2011
  AND h2.cid = h1.cid))
  AND
  ((SELECT h2.hdi_score
  FROM hdi h2
  WHERE year = 2011
  AND h2.cid = h1.cid) <
  (SELECT h2.hdi_score
  FROM hdi h2
  WHERE year = 2012
  AND h2.cid = h1.cid))
  AND
  ((SELECT h2.hdi_score
  FROM hdi h2
  WHERE year = 2012
  AND h2.cid = h1.cid) <
  (SELECT h2.hdi_score
  FROM hdi h2
  WHERE year = 2013
  AND h2.cid = h1.cid))
  ORDER BY country.cname ASC
);

-- Query 7 statements

CREATE VIEW S1 AS (
  SELECT r.rid, r.cid, (SUM(c.population)*r.rpercentage) AS cfollowers
  FROM religion r, country c
  WHERE r.cid=c.cid
  GROUP BY r.rid, r.cid
);

CREATE VIEW S2 AS (
  SELECT S1.rid, r.rname, SUM(cfollowers) AS followers
  FROM S1, religion r
  WHERE S1.rid=r.rid
  GROUP BY S1.rid, r.rname
  ORDER BY SUM(cfollowers) DESC
);

INSERT INTO Query7 (
  SELECT * FROM S2
);

DROP VIEW IF EXISTS S1, S2 CASCADE;

-- Query 8 statements

CREATE VIEW S1 AS (
  SELECT curr.cid, curr.lname
  FROM language curr
  WHERE NOT EXISTS
  (SELECT * FROM language high WHERE high.cid = curr.cid AND high.lpercentage > curr.lpercentage)
);

CREATE VIEW S2 AS (
  SELECT n.country AS c1name, n.neighbor AS c2name, S1.lname
  FROM S1, neighbour n
  WHERE n.country=S1.cid
);

CREATE VIEW S3 AS (
  SELECT S2.c1name, S2.c2name, S2.lname, S1.lname AS lname2
  FROM S2, S1
  WHERE S1.cid=S2.c2name
);

CREATE VIEW S4 AS (
  SELECT c1name, c2name, lname
  FROM S3
  WHERE c1name != c2name AND lname = lname2
  ORDER BY lname, c1name DESC
);

CREATE VIEW S5 AS (
  SELECT c.cname AS c1name, S4.c2name, S4.lname
  FROM S4, country c
  WHERE S4.c1name=c.cid
);

CREATE VIEW S6 AS (
  SELECT S5.c1name, c.cname AS c2name, S5.lname
  FROM S5, country c
  WHERE S5.c2name=c.cid
);

INSERT INTO Query8 (
  SELECT * FROM S6
);

DROP VIEW IF EXISTS S1, S2, S3, S4, S5, S6 CASCADE;

-- Query 9 statements

CREATE VIEW S1 AS (
  SELECT c.cname, c.height, o.oid
  FROM country c, oceanAccess o
  WHERE c.cid=o.cid
);

CREATE VIEW S2 AS (
  SELECT S1.cname, S1.height, S1.oid, o.depth
  FROM S1, ocean o
  WHERE S1.oid=o.oid
);

CREATE VIEW S3 AS (
  SELECT cname, (height+depth) AS totalspan
  FROM S2
  ORDER BY totalspan DESC
  LIMIT 1
);

INSERT INTO Query9 (
  SELECT * FROM S3
);

DROP VIEW IF EXISTS S1, S2, S3 CASCADE;

-- Query 10 statements

CREATE VIEW S1 AS (
  SELECT country, SUM(length) AS borderslength
  FROM neighbour
  GROUP BY country
);

CREATE VIEW S2 AS (
  SELECT c.cname, S1.borderslength
  FROM S1, country c
  WHERE S1.country=c.cid
  ORDER BY borderslength DESC
  LIMIT 1
);

INSERT INTO Query10 (
  SELECT *
  FROM S2
);

DROP VIEW IF EXISTS S1, S2 CASCADE;
