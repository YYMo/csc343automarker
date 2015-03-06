-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
DELETE FROM Query1 WHERE Query1.c1id = ANY (SELECT c1id FROM Query1);

INSERT INTO Query1 (
	SELECT c1.cid, c1.cname, c2.cid, c2.cname
	FROM  (
		SELECT t1.country, neighbor
		FROM (
			SELECT n.country, MAX(c.height) m
			FROM country c, neighbour n
			WHERE c.cid = n.neighbor
			GROUP BY n.country) t1,
			(SELECT * FROM country c, neighbour n
			WHERE c.cid = n.neighbor) t2
		WHERE t1.country = t2.country
		AND t1.m = t2.height) t, country c1, country c2
	WHERE t.country = c1.cid
	AND t.neighbor = c2.cid
	ORDER BY c1.cname ASC);


-- Query 2 statements
DELETE FROM Query2 WHERE Query2.cid = ANY (SELECT cid FROM Query2);

INSERT INTO Query2 (
	SELECT cid, cname
	FROM country c
	WHERE c.cid <> ALL (SELECT cid FROM oceanAccess)
	ORDER BY cname ASC);


-- Query 3 statements
DELETE FROM Query3 WHERE Query3.c1id = ANY (SELECT c1id FROM Query3);

INSERT INTO Query3 (
	SELECT c1.cid, c1.cname, c2.cid, c2.cname
	FROM (
		SELECT country, m
		FROM (
			SELECT n1.country, n1.m
			FROM (
				SELECT country, MAX(neighbor) m
				FROM neighbour
				GROUP BY country) n1,
				(SELECT country, MIN(neighbor) m
				FROM neighbour
				GROUP BY country) n2
				WHERE n1.country = n2.country
				AND n1.m = n2.m) n1,
        			(SELECT cid, cname
				FROM country c
			WHERE c.cid <> ALL (SELECT cid FROM oceanAccess)) n2
		WHERE n1.country = n2.cid) t1,
		country c1, country c2
	WHERE t1.country = c1.cid
	AND t1.m = c2.cid
	ORDER BY c1.cname ASC);


-- Query 4 statements
DELETE FROM Query4 WHERE Query4.cname = ANY (SELECT cname FROM Query4);

INSERT INTO Query4 (
	SELECT c.cname, o.oname
	FROM (
		SELECT * FROM oceanAccess
		UNION
		SELECT neighbor, oid
		FROM oceanAccess o, neighbour n
		WHERE o.cid = n.country) t1, country c, ocean o
	WHERE t1.cid = c.cid AND t1.oid = o.oid
	ORDER BY cname ASC, oname DESC);

-- Query 5 statements
DELETE FROM Query5 WHERE Query5.cid = ANY (SELECT cid FROM Query5);

INSERT INTO Query5 (
	SELECT t1.cid, c.cname, avghdi
	FROM (
		SELECT cid, AVG(hdi_score) avghdi
		FROM hdi
		WHERE year >= 2009 AND year <= 2013
		GROUP BY cid
		ORDER BY avghdi DESC LIMIT 10) t1,
		country c
	WHERE c.cid = t1.cid
	ORDER BY avghdi DESC); 


-- Query 6 statements
DELETE FROM Query6 WHERE Query6.cid = ANY (SELECT cid FROM Query6);

	CREATE VIEW yhdi (cid, year, hdi_score) AS SELECT * FROM hdi WHERE year >= 2009 AND year <=2013;

INSERT INTO Query6 (
	SELECT DISTINCT c.cid, cname
	FROM country c, 
	(SELECT cid
	FROM yhdi h
	WHERE h.cid NOT IN (
		SELECT h1.cid
		FROM yhdi h1, yhdi h2
		WHERE h1.cid = h2.cid
		AND h1.year < h2.year
		AND h1.hdi_score > h2.hdi_score)) c1
	WHERE c.cid = c1.cid
	ORDER BY cname ASC);

DROP VIEW IF EXISTS yhdi;

-- Query 7 statements
DELETE FROM Query7 WHERE Query7.rid = ANY (SELECT rid FROM Query7);

INSERT INTO Query7(
	SELECT rid, rname, SUM(population * rpercentage) s
	FROM religion r, country c
	WHERE r.cid = c.cid
	GROUP BY rid, rname
	ORDER BY s DESC);

-- Query 8 statements
DELETE FROM Query8 WHERE Query8.c1name = ANY (SELECT c1name FROM Query8);

CREATE VIEW p8 (cid, lid, lname, neighbor) AS
	SELECT cid, lid, lname, neighbor FROM 
	(SELECT t1.cid, lid, lname FROM 
	(SELECT cid, MAX(lpercentage) m
	FROM language
	GROUP BY cid) t1,
	language c
	WHERE c.cid = t1.cid
	AND lpercentage = m) t2,
	neighbour c
	WHERE c.country = t2.cid;

INSERT INTO Query8 (
	SELECT c1.cname, c2.cname, t3.lname FROM (
	SELECT DISTINCT t1.cid, t1.neighbor, t1.lid, t1.lname
	FROM p8 t1, p8 t2
	WHERE t1.neighbor = t2.cid
	AND t1.lid = t2.lid) t3,
	country c1,
	country c2
	WHERE c1.cid = t3.cid
	AND t3.neighbor = c2.cid
	ORDER BY t3.lname ASC, c1.cname DESC);

DROP VIEW IF EXISTS p8; 

-- Query 9 statements
DELETE FROM Query9 WHERE Query9.cname = ANY (SELECT cname FROM Query9);

CREATE VIEW p9 (span, cid, cname) AS
	SELECT (o1.depth + height) span, c.cid, cname
	FROM OceanAccess o, ocean o1, country c
	WHERE o.cid = c.cid
	AND o1.oid = o.oid;

INSERT INTO Query9 (
	SELECT cname, span
	FROM p9
	WHERE  p9.span >= ALL (SELECT span FROM p9));

DROP VIEW IF EXISTS p9;

-- Query 10 statements
DELETE FROM Query10 WHERE Query10.cname = ANY (SELECT cname FROM Query10);

INSERT INTO Query10 (
	
	SELECT cname, s
	FROM (
		SELECT SUM(length) s, country
		FROM neighbour
		GROUP BY country) t1,
	country c
	WHERE c.cid = country
	AND s >= ALL (SELECT SUM(length) FROM neighbour GROUP BY country));

