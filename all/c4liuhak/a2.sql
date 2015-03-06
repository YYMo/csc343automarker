-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW ids AS(SELECT N.country AS cid, N.neighbor AS nid max(C.height) AS maxNeibHeigt
FROM neighbor N JOIN country C ON N.neighbor = C.cid
GROUP BY N.cid );

INSERT INTO Query1 (
SELECT ids.cid AS c1id, C1.cname AS c1name, ids.nid AS c2id, C2.cname AS c2name 
FROM country C1, ids, country C2
WHERE C1.cid=ids.cid AND C2.cid=ids.nid
ORDER BY c1name ASC);

DROP VIEW ids;


-- Query 2 statements
INSERT INTO Query2 (
SELECT country.cid as cid, country.cname as cname
FROM country
WHERE (country.cid NOT IN (SELECT cid FROM oceanAccess))
ORDER BY cname ASC
);


-- Query 3 statements

CREATE VIEW code AS(SELECT country AS cid, neighbor AS nid
FROM neighbour
GROUP BY country
HAVING count(neighbor)=1);

INSERT INTO Query3 (
SELECT C1.cid AS c1id, C1.cname AS c1name, C2.cid AS c2id, C2.cname AS c2name
FROM country C1, code, country C2
WHERE C1.cid = code.cid AND C2.cid = code.nid
ORDER BY c1name ASC);

DROP VIEW code;


-- Query 4 statements

CREATE VIEW indirect AS (SELECT neighbour.neighbor AS cid, oceanAccess.oid AS oid 
FROM neighbour, oceanAccess 
WHERE neighbour.neighbor = oceanAccess.cid);

CREATE VIEW cod AS (indirect UNION oceanAccess);

INSERT INTO Query4 (
SELECT country.cname AS cname, ocean.oname AS oname
FROM cod, country, ocean
WHERE cod.cid = country.cid AND cod.oid = ocean.oid
ORDER BY cname ASC, oname DESC);

DROP VIEW indirect;
DROP VIEW cod;


-- Query 5 statements

CREATE VIEW code AS
(SELECT TOP(10) cid, AVG(hdi_score) as avghdi
FROM
(SELECT * FROM hdi WHERE year=2009 OR year=2010 OR year=2011 OR year=2012 OR year=2013)
GROUP BY cid
ORDER BY avghdi DESC);

INSERT INTO Query5 (
SELECT country.cid AS cid, country.cname AS cname, code.avghdi as avghdi
FROM country, code
WHERE country.cid = code.cid
);

DROP VIEW code;


-- Query 6 statements

CREATE VIEW A AS (
SELECT * FROM hdi 
WHERE year=2009 OR year=2010 OR year=2011 OR year=2012 OR year=2013);

CREATE VIEW B AS (
SELECT * FROM hdi 
WHERE year=2009 OR year=2010 OR year=2011 OR year=2012 OR year=2013);

CREATE VIEW hdi_changes AS (
SELECT A.cid AS id, A.year AS year B.hdi_score-A.hdi_score AS change
FROM A, B
WHERE A.cid = B.cid AND A.year = (B.year-1));

CREATE VIEW id_ans AS (
SELECT id 
FROM hdi_changes
GROUP BY id
HAVING ALL(change)>0);

INSERT INTO Query6 (
SELECT id_ans.id AS cid, country.cname AS cname 
FROM id_ans, country
WHERE id_ans.id = country.cid
ORDER BY cname ASC);

DROP VIEW A;
DROP VIEW B;
DROP VIEW hdi_changes;
DROP VIEW id_ans;


-- Query 7 statements

CREATE VIEW R_pop AS
(SELECT religion.rid as rid, religion.rname as rname, CAST(R.rpercentage*C.population AS INT) AS follower
FROM religion R, country C
WHERE R.cid=C.cid);

INSERT INTO Query7 (
SELECT rid, rname, sum(follower) AS followers
FROM R_pop
GROUP BY rid 
ORDER BY followers SESC);

DROP VIEW R_pop;


-- Query 8 statements

CREATE VIEW A AS (
SELECT cid, lid, lname, max(lpercentage) AS maxLan
FROM language
GROUP BY cid);

CREATE VIEW C1 AS(
SELECT country.cid AS c1id, country.cname AS c1name, A.lname AS lname
FROM country, A
WHERE country.cid = A.cid
);

CREATE VIEW C2 AS(
SELECT country.cid AS c2id, country.cname AS c2name, A.lname AS lname
FROM country, A
WHERE country.cid = A.cid
);

INSERT INTO Query8 (
SELECT C1.c1name AS c1name, C2.c2name AS c2name, C1.lname as lname
FROM C1, C2, neighbour
WHERE C1.c1id = neighbour.country AND C2.c2id = neighbour.neighbor AND C1.lname = C2.lname
ORDER BY lname ASC, c1name AS DESC
);

DROP VIEW A;
DROP VIEW C1;
DROP VIEW C2;


-- Query 9 statements

CREATE VIEW hasocean AS(
SELECT country.cid AS id, country.cname AS name, country.height+ocean.depth AS totalspan
FROM country, ocean, oceanAccess
WHERE country.cid=oceanAccess.cid AND oceanAccess.oid=ocean.oid)

CREATE VIEW hasNOocean AS(
SELECT country.cid AS id, country.cname AS name, country.height AS totalspan
FROM country
WHERE country.cid NOT IN (SELECT id FROM hasocean))

INSERT INTO Query9 (
SELECT name AS cname, max(totalspan) AS totalspan
FROM hasocean UNION hasNOocean)

DROP VIEW hasocean;
DROP VIEW hasNOocean;


-- Query 10 statements

CREATE VIEW coun_wt_len AS(
SELECT country, sum(length) AS borderslength
FROM neighbour
GROUP BY country);

INSERT INTO Query10 (
SELECT country.cname AS cname, max(coun_wt_len.borderslength) AS borderslength
FROM country, coun_wt_len
WHERE country.cid = coun_wt_len.country);

DROP VIEW coun_wt_len;