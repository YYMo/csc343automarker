-- Add below your SQL statements. 

-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.

-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.



-- Query 1 statements

CREATE VIEW q1temp AS
        (SELECT c1.cid as c1cid, c1.cname AS c1cname, c1.height AS c1height, c1.population AS c1population, 
        c2.cid AS c2cid, c2.cname AS c2cname, c2.height AS c2height, c2.population AS c2population
        FROM (country c1 JOIN neighbour n ON c1.cid = n.country JOIN country c2 on c2.cid = n.neighbor));

-- SELECT c1cid AS c1id, c1cname AS c1name, c2cid as c2id, c2cname as c2name 
--         FROM q1temp 
-- 	WHERE q1temp.c2height =
--                 (SELECT MAX(tmp.c2height) FROM q1temp tmp 
--                 WHERE q1temp.c1cid = tmp.c1cid 
--                 GROUP BY tmp.c1cid) 
--         ORDER BY c1name ASC;

INSERT INTO Query1 (
	SELECT c1cid AS c1id, c1cname AS c1name, c2cid as c2id, c2cname as c2name         
        	FROM q1temp 
        	WHERE q1temp.c2height =
                	(SELECT MAX(tmp.c2height) FROM q1temp tmp   
                	WHERE q1temp.c1cid = tmp.c1cid 
                	GROUP BY tmp.c1cid)
        	ORDER BY c1name ASC);

DROP VIEW q1temp;

-- Query 2 statements

-- SELECT c.cid AS cid, c.cname AS cname
--         FROM country c
--         WHERE c.cid NOT IN (SELECT oa.cid as cid FROM oceanAccess oa)
--         ORDER BY cname ASC;

INSERT INTO Query2 (
	SELECT c.cid AS cid, c.cname AS cname
        	FROM country c
        	WHERE c.cid NOT IN (SELECT oa.cid as cid FROM oceanAccess oa)
        	ORDER BY cname ASC);

-- Query 3 statements

CREATE VIEW q3temp AS (
	SELECT c.cid AS cid, c.cname AS cname 
	FROM country c 
	WHERE c.cid NOT IN (SELECT oa.cid as cid FROM oceanAccess oa) 
	ORDER BY cname ASC);

CREATE VIEW q3temp1 AS (
        SELECT c1.cid, c1.cname
        FROM (q3temp c1 JOIN neighbour n ON c1.cid = n.country JOIN q3temp c2 ON c2.cid = n.neighbor)
        GROUP BY c1.cid, c1.cname
        HAVING COUNT(c2.cid) = 1);

-- SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name 
-- 	FROM (q3temp1 c1 JOIN neighbour n ON c1.cid = n.country JOIN q3temp1 c2 ON c2.cid = n.neighbor) 
-- 	ORDER BY c1name ASC;

INSERT INTO Query3(
	SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name 
        	FROM (q3temp1 c1 JOIN neighbour n ON c1.cid = n.country JOIN q3temp1 c2 ON c2.cid = n.neighbor) 
        	ORDER BY c1name ASC);

DROP VIEW q3temp1;
DROP VIEW q3temp;

-- Query 4 statements

CREATE VIEW q4temp1 AS(
        SELECT c.cid as cname, o.oname as oname 
        FROM (country c JOIN oceanAccess oa ON oa.cid = c.cid JOIN ocean o on o.oid = oa.oid));

CREATE VIEW q4temp2 AS(
        SELECT c1.cid as cname, o.oname as oname
        FROM (country c1 JOIN neighbour n ON c1.cid = n.country JOIN country c2 ON c2.cid = n.neighbor 
                JOIN oceanAccess oa ON oa.cid = c2.cid JOIN ocean o on o.oid = oa.oid));

-- SELECT * FROM q4temp1 UNION SELECT * FROM q4temp2 ORDER BY cname ASC, oname DESC;

INSERT INTO Query4 (
	SELECT * FROM q4temp1 UNION SELECT * FROM q4temp2 ORDER BY cname ASC, oname DESC);

DROP VIEW q4temp1;
DROP VIEW q4temp2;

-- Query 5 statements

CREATE VIEW q5temp1 AS (
	SELECT cid, AVG(hdi_score) AS avghdi 
		FROM hdi 
		WHERE year >= 2009 AND year <= 2013 
		GROUP BY cid);

-- SELECT c.cid, c.cname, tmp.avghdi 
-- 	FROM (q5temp1 tmp JOIN country c ON c.cid = tmp.cid) 
-- 	ORDER BY avghdi DESC 
-- 	LIMIT 10;

INSERT INTO Query5(
	SELECT c.cid, c.cname, tmp.avghdi 
        	FROM (q5temp1 tmp JOIN country c ON c.cid = tmp.cid) 
        	ORDER BY avghdi DESC 
        	LIMIT 10);

DROP VIEW q5temp1;

-- Query 6 statements

CREATE VIEW q6_temp1 AS (
	SELECT cid, hdi_score AS hdi_2009 
	FROM hdi 
	WHERE year = 2009);

CREATE VIEW q6_temp2 AS (
        SELECT cid, hdi_score AS hdi_2010 
        FROM hdi 
        WHERE year = 2010);

CREATE VIEW q6_temp3 AS (
        SELECT cid, hdi_score AS hdi_2011 
        FROM hdi 
        WHERE year = 2011);

CREATE VIEW q6_temp4 AS (
        SELECT cid, hdi_score AS hdi_2012 
        FROM hdi 
        WHERE year = 2012);

CREATE VIEW q6_temp5 AS (
        SELECT cid, hdi_score AS hdi_2013 
        FROM hdi 
        WHERE year = 2013);

CREATE VIEW q6_temp6 AS (
        SELECT t1.cid, hdi_2009, hdi_2010, hdi_2011, hdi_2012, hdi_2013 
        FROM q6_temp1 t1 
		JOIN q6_temp2 t2 ON t1.cid = t2.cid 
		JOIN q6_temp3 t3 ON t1.cid = t3.cid 
		JOIN q6_temp4 t4 ON t1.cid = t4.cid 
		JOIN q6_temp5 t5 ON t1.cid = t5.cid);

-- SELECT c.cid, c.cname 
-- 	FROM q6_temp6 t6 JOIN country c ON t6.cid = c.cid
-- 	WHERE (hdi_2013 > hdi_2012) AND 
-- 		(hdi_2012 > hdi_2011) AND
-- 		(hdi_2011 > hdi_2010) AND  
-- 		(hdi_2010 > hdi_2009)
-- 	ORDER BY cname ASC; 

INSERT INTO Query6(
	SELECT c.cid, c.cname
        	FROM q6_temp6 t6 JOIN country c ON t6.cid = c.cid
        	WHERE (hdi_2013 > hdi_2012) AND
        		(hdi_2012 > hdi_2011) AND
        		(hdi_2011 > hdi_2010) AND   
        		(hdi_2010 > hdi_2009) 
        	ORDER BY cname ASC);

DROP VIEW q6_temp6;
DROP VIEW q6_temp1;
DROP VIEW q6_temp2;
DROP VIEW q6_temp3;
DROP VIEW q6_temp4;
DROP VIEW q6_temp5;

-- Query 7 statements

-- SELECT rid, rname, SUM(c.population * r.rpercentage) AS followers 
-- 	FROM religion r JOIN country c ON r.cid = c.cid 
-- 	GROUP BY r.rid, r.rname 
-- 	ORDER BY followers DESC;

INSERT INTO Query7(
	SELECT rid, rname, SUM(c.population * r.rpercentage) AS followers 
        	FROM religion r JOIN country c ON r.cid = c.cid 
        	GROUP BY r.rid, r.rname 
        	ORDER BY followers DESC);

-- Query 8 statements

CREATE VIEW q8_temp1 AS (
	SELECT lang.cid, lang.lid, lang.lname, lang.lpercentage 
		FROM language lang 
		WHERE lang.lpercentage = (
			SELECT MAX(lpercentage) 
			FROM language tmp 
			WHERE tmp.cid = lang.cid 
			GROUP BY tmp.cid));

-- SELECT c1.cname AS c1name, c2.cname AS c2name, tmp1.lname AS lname
--         FROM (country c1 JOIN neighbour n ON c1.cid = n.country 
-- 		JOIN country c2 on c2.cid = n.neighbor
-- 		JOIN q8_temp1 tmp1 on c1.cid = tmp1.cid
-- 		JOIN q8_temp1 tmp2 on c2.cid = tmp2.cid)
-- 	WHERE tmp1.lid = tmp2.lid
-- 	ORDER BY lname ASC, c1name DESC;

INSERT INTO Query8 (
	SELECT c1.cname AS c1name, c2.cname AS c2name, tmp1.lname AS lname
        	FROM (country c1 JOIN neighbour n ON c1.cid = n.country
                	JOIN country c2 on c2.cid = n.neighbor
                	JOIN q8_temp1 tmp1 on c1.cid = tmp1.cid 
                	JOIN q8_temp1 tmp2 on c2.cid = tmp2.cid) 
        	WHERE tmp1.lid = tmp2.lid 
        	ORDER BY lname ASC, c1name DESC);

DROP VIEW q8_temp1;

-- Query 9 statements

CREATE VIEW q9_temp1 AS(
	SELECT c.cid, c.cname, c.height, o.depth 
		FROM country c LEFT JOIN oceanaccess oa ON oa.cid = c.cid LEFT JOIN ocean o on o.oid = oa.oid);

-- SELECT t.cname, (t.height + t.depth) AS totalspan
-- 	FROM q9_temp1 t
--         WHERE (t.height + t.depth) = (
--         	SELECT MAX(tmp.height + tmp.depth)
-- 		FROM q9_temp1 tmp);

INSERT INTO Query9(
	SELECT t.cname, (t.height + t.depth) AS totalspan
                FROM q9_temp1 t                  
                WHERE (t.height + t.depth) = (
                        SELECT MAX(tmp.height + tmp.depth)
                        FROM q9_temp1 tmp));

DROP VIEW q9_temp1;

-- Query 10 statements

CREATE VIEW q10_temp1 AS(
        SELECT c.cname, n.country, SUM(n.length) AS borderslength
                FROM country c JOIN neighbour n ON n.country = c.cid 
		GROUP BY c.cname, n.country);

-- SELECT t.cname, t.borderslength
--                 FROM q10_temp1 t
--                 WHERE t.borderslength = (
--                         SELECT MAX(tmp.borderslength)
--                         FROM q10_temp1 tmp);

INSERT INTO Query10(
	SELECT t.cname, t.borderslength
        	FROM q10_temp1 t
        	WHERE t.borderslength = (
                	SELECT MAX(tmp.borderslength)
                	FROM q10_temp1 tmp));

DROP VIEW q10_temp1;