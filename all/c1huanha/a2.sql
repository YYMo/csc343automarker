-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
DROP VIEW IF EXISTS q1 CASCADE;

CREATE VIEW q1 AS (SELECT c1.cid AS c1id, c1.cname As c1name, c2.cid AS c2id, c2.cname AS c2name, c2.height AS height FROM neighbour n, 
	country c1, country c2  WHERE n.country = c1.cid and n.neighbor = c2.cid);


INSERT INTO Query1(SELECT t1.c1id, t1.c1name, t1.c2id, t1.c2name FROM q1 t1 LEFT JOIN q1 t2 
	ON (t1.c1id = t2.c1id AND t1.height < t2.height) WHERE t2.height IS NULL ORDER BY c1name ASC);


-- Query 2 statements
INSERT INTO Query2(SELECT c.cid, c.cname FROM country c LEFT JOIN oceanAccess o ON (c.cid = o.cid) WHERE o.cid IS NULL ORDER BY cname ASC);


-- Query 3 statements
DROP VIEW IF EXISTS q3 CASCADE;
DROP VIEW IF EXISTS q4 CASCADE;
DROP VIEW IF EXISTS q5 CASCADE;
DROP VIEW IF EXISTS q6 CASCADE;


CREATE VIEW q3 AS (SELECT c.cid AS c1id, c.cname AS c1name FROM country c LEFT JOIN oceanAccess o ON (c.cid = o.cid) WHERE o.cid IS NULL);
CREATE VIEW q4 AS (SELECT country AS c1id, count(country) AS num FROM neighbour GROUP BY country);
CREATE VIEW q5 AS (SELECT q3.c1id, q3.c1name FROM q3, q4 WHERE q3.c1id = q4.c1id AND q4.num = 1);

INSERT INTO Query3(SELECT q5.c1id, q5.c1name, c.cid AS c2id, c.cname AS c2name FROM q5, country c, neighbour n WHERE q5.c1id = n.country AND n.neighbor = c.cid 
	ORDER BY c1name ASC);

-- Query 4 statements
DROP VIEW IF EXISTS q6 CASCADE;
DROP VIEW IF EXISTS q7 CASCADE;
DROP VIEW IF EXISTS q8 CASCADE;


CREATE VIEW q6 AS (SELECT c.cid, c.cname, o1.oid, o2.oname FROM oceanAccess o1, country c, ocean o2 
	WHERE o1.oid = o2.oid AND o1.cid = c.cid);
CREATE VIEW q7 AS (SELECT c1.cid AS c1id, c1.cname As c1name, c2.cid AS c2id, c2.cname AS c2name, c2.height AS height FROM neighbour n, 
	country c1, country c2  WHERE n.country = c1.cid and n.neighbor = c2.cid); 
CREATE VIEW q8 AS (SELECT q7.c1name AS cname, q6.oname FROM q7, q6 WHERE q7.c2id = q6.cid);

INSERT INTO Query4(SELECT * FROM q8 UNION SELECT q6.cname, q6.oname FROM q6 ORDER BY cname ASC, oname DESC);

-- Query 5 statements
DROP VIEW IF EXISTS q9 CASCADE;

CREATE VIEW q9 AS (SELECT h.cid, avg(h.hdi_score) AS avghdi FROM hdi h WHERE h.year >= 2009 
	AND h.year <= 2013 GROUP BY h.cid);

INSERT INTO Query5(SELECT q9.cid, c.cname, q9.avghdi FROM q9, country c WHERE c.cid = q9.cid ORDER BY avghdi DESC LIMIT 10);

-- Query 6 statements
DROP VIEW IF EXISTS q10 CASCADE;

CREATE VIEW q10 AS (SELECT h.cid, c.cname, h.hdi_score, h.year FROM hdi h, country c WHERE h.year >= 2009 
	AND h.year <= 2013 AND c.cid = h.cid);

INSERT INTO Query6(SELECT t1.cid, t1.cname FROM q10 t1, q10 t2, q10 t3, q10 t4, q10 t5 WHERE t1.year = 2009 
	AND t2.year = 2010 AND t3.year = 2011 AND t4.year = 2012 AND t5.year = 2013 
	AND t1.hdi_score < t2.hdi_score AND t2.hdi_score < t3.hdi_score AND t3.hdi_score < t4.hdi_score 
	AND t4.hdi_score < t5.hdi_score AND t1.cid = t2.cid AND t2.cid = t3.cid 
	AND t3.cid = t4.cid AND t4.cid = t5.cid ORDER BY cname ASC);

-- Query 7 statements
INSERT INTO Query7(SELECT r.rid, r.rname, sum(c.population * r.rpercentage) AS followers FROM religion r, country c 
	WHERE r.cid = c.cid GROUP BY r.rid, r.rname ORDER BY followers DESC);

-- Query 8 statements
DROP VIEW IF EXISTS q11 CASCADE;

CREATE VIEW q11 AS (SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name FROM country c1, country c2, neighbour n 
	WHERE c1.cid = n.country AND c2.cid = n.neighbor);


INSERT INTO Query8(SELECT t1.c1name, t1.c2name, t2.lname FROM q11 t1, language t2, language t3 WHERE t1.c1id = t2.cid AND t1.c2id = t3.cid 
	AND t2.lpercentage >= 0.5 AND t3.lpercentage >= 0.5 AND t2.lname = t3.lname ORDER BY t2.lname ASC, t1.c1name DESC);


-- Query 9 statements
DROP VIEW IF EXISTS q12 CASCADE;

CREATE VIEW q12 AS (SELECT c.cname, max(c.height + o.depth) AS totalspan FROM country c, oceanAccess oa, ocean o 
	WHERE c.cid = oa.cid AND o.oid = oa.oid GROUP BY c.cname);

INSERT INTO Query9(SELECT cname, max(totalspan) FROM (SELECT c.cname, c.height AS totalspan FROM country c UNION SELECT * FROM q12) A 
	GROUP BY cname ORDER BY max(totalspan) DESC LIMIT 1);


-- Query 10 statements
INSERT INTO Query10(SELECT c1.cname, sum(n.length) AS borderslength
	FROM country c1, country c2, neighbour n WHERE c1.cid = n.country AND c2.cid = n.neighbor 
	GROUP BY c1.cname ORDER BY sum(n.length) DESC LIMIT 1);


