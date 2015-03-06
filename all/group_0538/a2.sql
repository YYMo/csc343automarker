-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements


CREATE VIEW Query1i (c1id,c2id,c2name,c2height) AS
SELECT n.country AS c1id, n.neighbor AS c2id, c.cname AS c2name, c.height AS c2height
FROM country c, neighbour n
WHERE n.neighbor = c.cid;


INSERT INTO Query1 (
SELECT n1.c1id,c.cname AS c1name, n1.c2id, n1.c2name
FROM Query1i n1, country c
WHERE n1.c1id = c.cid AND n1.c2height >= ALL (
	SELECT n2.c2height
	FROM Query1i n2
	WHERE n2.c1id = n1.c1id)
ORDER BY c1name ASC
);

DROP VIEW Query1i;

-- Query 2 statements

INSERT INTO Query2 (
SELECT c.cid, c.cname
FROM country c
WHERE NOT EXISTS (SELECT c.cid FROM oceanAccess oa WHERE c.cid = oa.cid)
ORDER BY cname ASC
);

-- Query 3 statements

INSERT INTO Query3 (
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name
FROM Query2 c1, neighbour n, Query2 c2
WHERE c1.cid = n.country AND n.neighbor = c2.cid AND c1.cid IN (
	SELECT c1.cid
	FROM Query2 c1, neighbour n
	WHERE c1.cid = n.country
	GROUP BY c1.cid
	HAVING COUNT(c1.cid) = 1
	)
ORDER BY c1name ASC
);

-- Query 4 statements

INSERT INTO Query4 (
SELECT DISTINCT c.cname, o.oname
FROM country c, neighbour n, oceanAccess oa, ocean o
WHERE oa.oid=o.oid AND (c.cid = oa.cid OR (c.cid = n.country AND n.neighbor = oa.cid))
ORDER BY cname ASC, oname DESC
);

-- Query 5 statements

INSERT INTO Query5 (
SELECT c.cid, c.cname, AVG(hdi_score) AS avghdi
FROM country c, hdi h
WHERE c.cid = h.cid AND year <= 2013 AND year >= 2009
GROUP BY c.cid
ORDER BY avghdi DESC
LIMIT 10
);

-- Query 6 statements

INSERT INTO Query6 (
SELECT cid, cname
FROM country
WHERE cid in (
	SELECT h9.cid AS cid
	FROM hdi h9, hdi h10, hdi h11, hdi h12, hdi h13
	WHERE h9.cid = h10.cid AND h9.cid = h11.cid AND h9.cid = h12.cid AND h9.cid = h13.cid 
		AND h9.year = 2009 AND h10.year = 2010 AND h11.year = 2011 AND h12.year = 2012 
		AND h13.year = 2013 AND h9.hdi_score<h10.hdi_score AND h10.hdi_score<h11.hdi_score 
		AND h11.hdi_score<h12.hdi_score AND h12.hdi_score<h13.hdi_score
	GROUP BY h9.cid
	)
ORDER BY cname ASC
);

-- Query 7 statements

-- select religion and total followers
CREATE VIEW Query7i as
SELECT rid, ROUND(SUM(rpercentage * population)) AS followers
FROM religion r, country c
WHERE r.cid = c.cid
GROUP BY rid;

INSERT INTO Query7 (
SELECT DISTINCT q.rid, rname, followers
FROM Query7i q, religion r
WHERE q.rid = r.rid
ORDER BY followers DESC
);

DROP VIEW Query7i;

-- Query 8 statements

-- select country and its most popular language
CREATE VIEW Query8i as
SELECT DISTINCT ON (cid)
	l.cid, lname, cname
FROM language l, country c
WHERE l.cid=c.cid
ORDER BY l.cid, l.lpercentage DESC;

INSERT INTO Query8 (
SELECT l1.cname AS c1name, l2.cname AS c2name, l1.lname
FROM Query8i l1, neighbour n, Query8i l2
WHERE l1.cid=n.country AND n.neighbor=l2.cid AND l1.lname=l2.lname
ORDER BY lname ASC, c1name DESC
);

DROP VIEW Query8i;

-- Query 9 statements

CREATE VIEW Query9i as
SELECT DISTINCT ON (oa.cid)
	oa.cid, o.depth
FROM oceanAccess oa, ocean o
WHERE oa.oid=o.oid
ORDER BY oa.cid, o.depth DESC;

INSERT INTO Query9 (
SELECT c.cname, (c.height + COALESCE(o.depth, 0)) AS totalspan
FROM country c
LEFT OUTER JOIN Query9i o
ON c.cid = o.cid
ORDER BY totalspan DESC
LIMIT 1
);

DROP VIEW Query9i;

-- Query 10 statements

INSERT INTO Query10 (
SELECT c.cname, SUM(length) AS borderslength
FROM country c, neighbour n
WHERE c.cid = n.country
GROUP BY c.cid
ORDER BY borderslength DESC
LIMIT 1
);
