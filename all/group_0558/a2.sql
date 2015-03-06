-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW q1a as
SELECT *
FROM 
	(SELECT cid as neighbor, cname as name2, height as height1 FROM country) a
	NATURAL JOIN neighbour;


CREATE VIEW q1b as
SELECT * 
FROM q1a 
NATURAL JOIN 
	(SELECT cid as country, cname, height 
	FROM country) b;

CREATE VIEW q1c as
SELECT country, max(height1)
FROM q1b
GROUP BY country;

INSERT INTO Query1
(SELECT country as c1id, cname as c1name, neighbor as c2id, name2 as c2name
FROM q1b NATURAL JOIN q1c
WHERE q1c.max = q1b.height1
ORDER BY c1name);		


DROP VIEW IF EXISTS q1a CASCADE;
DROP VIEW IF EXISTS q1b CASCADE;
DROP VIEW IF EXISTS q1c CASCADE;



-- Query 2 statements
CREATE VIEW q2 as
SELECT cid, cname 
FROM country 
WHERE country.cid 
NOT IN 
	(SELECT o.cid 
	FROM oceanAccess o)
ORDER BY cname ASC;

INSERT INTO Query2(SELECT * FROM q2);

DROP VIEW IF EXISTS q2;



-- Query 3 statements

CREATE VIEW q3a as
SELECT cid, cname FROM country
WHERE country.cid NOT IN
(SELECT o.cid FROM  oceanAccess o)
ORDER BY cname ASC;

CREATE VIEW q3b as
SELECT country
FROM neighbour
GROUP BY country
HAVING count(neighbor) = 1;

CREATE VIEW q3c as
SELECT landlock.country, landlock.neighbor
FROM
	(SELECT neighbour.country, neighbour,neighbor
	FROM neighbour 
	JOIN q3a ON q3a.cid = neighbour.country) landlock
JOIN q3b ON landlock.country = q3b.country;

CREATE VIEW q3d as
SELECT C1.c1id, C1.c1name, C1.c2id, country.cname as c2name
FROM
	(SELECT q3c.country as c1id, country.cname as c1name, q3c.neighbor as c2id
	FROM q3c JOIN country ON q3c.country = country.cid) C1
	JOIN country ON C1.c2id = country.cid;
	
INSERT INTO Query3(SELECT * FROM q3d ORDER BY c1name ASC);

DROP VIEW IF EXISTS q3a CASCADE;
DROP VIEW IF EXISTS q3b CASCADE;
DROP VIEW IF EXISTS q3c CASCADE;
DROP VIEW IF EXISTS q3d CASCADE;



-- Query 4 statements

CREATE VIEW q4a AS
SELECT oceanAccess.cid, oname 
FROM oceanAccess 
JOIN ocean ON oceanAccess.oid = ocean.oid;

CREATE VIEW q4b AS 
SELECT country.cname, q4a.oname 
FROM q4a 
JOIN country ON q4a.cid = country.cid;

CREATE VIEW q4c AS
SELECT neighbour.neighbor as cid, oid
FROM neighbour 
JOIN oceanAccess ON neighbour.country = oceanAccess.cid;

CREATE VIEW q4d AS
SELECT cname, oid
FROM q4c 
JOIN country ON q4c.cid = country.cid;

CREATE VIEW q4e AS
SELECT cname, oname 
FROM q4d 
JOIN ocean ON q4d.oid = ocean.oid;

CREATE VIEW q4 AS
(SELECT * FROM q4b)
UNION
(SELECT * FROM q4e);

INSERT INTO Query4(SELECT * FROM q4 ORDER BY cname ASC, oname DESC);

DROP VIEW IF EXISTS q4a CASCADE;
DROP VIEW IF EXISTS q4b CASCADE;
DROP VIEW IF EXISTS q4c CASCADE;
DROP VIEW IF EXISTS q4d CASCADE;



-- Query 5 statements
CREATE VIEW q5a AS
SELECT cid, hdi_score
FROM hdi 
WHERE YEAR >= 2009 AND year <= 2013;

CREATE VIEW q5b AS
SELECT cid, avg(hdi_score) as avghdi
FROM q5a
GROUP BY cid
LIMIT 10;

CREATE VIEW q5c AS
SELECT q5b.cid, country.cname, q5b.avghdi 
FROM q5b 
JOIN country ON q5b.cid = country.cid
ORDER BY q5b.avghdi  DESC;

INSERT INTO Query5(SELECT * FROM q5c);

DROP VIEW IF EXISTS q5a CASCADE;
DROP VIEW IF EXISTS q5b CASCADE;
DROP VIEW IF EXISTS q5c CASCADE;


-- Query 6 statements

CREATE VIEW q6a as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2009;

CREATE VIEW q6b as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2010;

CREATE VIEW q6c as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2011;

CREATE VIEW q6d as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2012;

CREATE VIEW q6e as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2013;


CREATE VIEW q6f as
SELECT q6a.cid
FROM q6a 
JOIN q6b ON q6a.cid = q6b.cid
JOIN q6c ON q6a.cid = q6c.cid
JOIN q6d ON q6a.cid = q6d.cid
JOIN q6e ON q6a.cid = q6e.cid
WHERE q6a.hdi_score < q6b.hdi_score
AND q6b.hdi_score < q6c.hdi_score
AND q6c.hdi_score < q6d.hdi_score
AND q6d.hdi_score < q6e.hdi_score;

CREATE VIEW q6g as
	SELECT country.cid, country.cname
	FROM q6f 
	JOIN country ON country.cid = q6f.cid;

INSERT INTO Query6(SELECT * from q6g ORDER BY cname ASC);
		

DROP VIEW IF EXISTS q6a CASCADE;
DROP VIEW IF EXISTS q6b CASCADE;
DROP VIEW IF EXISTS q6c CASCADE;
DROP VIEW IF EXISTS q6d CASCADE;
DROP VIEW IF EXISTS q6e CASCADE;
DROP VIEW IF EXISTS q6f CASCADE;
DROP VIEW IF EXISTS q6g CASCADE;



-- Query 7 statements

CREATE VIEW q7a AS
SELECT country.cid, religion.rid, religion.rname, religion.rpercentage*country.population as followers
FROM religion 
JOIN country ON religion.cid = country.cid; 

CREATE VIEW q7b AS
SELECT rid, rname, sum(followers) as followers
FROM q7a
GROUP BY rname, rid
ORDER BY followers DESC;

INSERT INTO Query7(SELECT * FROM q7b);

DROP VIEW IF EXISTS q7a CASCADE;
DROP VIEW IF EXISTS q7b CASCADE;



-- Query 8 statements
CREATE VIEW q8a as
SELECT cid, max(lpercentage) as lpercentage
FROM language  
GROUP BY cid;

CREATE VIEW q8b as
SELECT q8a.cid, q8a.lpercentage, l1.lname
FROM language l1
JOIN q8a ON q8a.cid = l1.cid AND q8a.lpercentage = l1.lpercentage;

CREATE VIEW q8c as
SELECT q8b.cid, neighbour.neighbor, q8b.lname, q8b.lpercentage 
FROM q8b 
JOIN neighbour ON q8b.cid = neighbour.country;

CREATE VIEW q8d as
SELECT q8c.cid, q8c.neighbor, q8c.lname
FROM q8b 
JOIN q8c ON q8b.cid = q8c.neighbor AND q8b.lname = q8c.lname;

CREATE VIEW q8e as
SELECT country.cname, q8d.neighbor, q8d.lname
FROM q8d 
JOIN country ON q8d.cid = country.cid;

CREATE VIEW q8f as
SELECT q8e.cname as c1name, country.cname as c2name, q8e.lname
FROM q8e 
JOIN country ON country.cid = q8e.neighbor;

INSERT INTO Query8(SELECT * FROM q8f ORDER BY lname ASC, c1name DESC);

DROP VIEW IF EXISTS q8a CASCADE;
DROP VIEW IF EXISTS q8b CASCADE;
DROP VIEW IF EXISTS q8c CASCADE;
DROP VIEW IF EXISTS q8d CASCADE;
DROP VIEW IF EXISTS q8e CASCADE;
DROP VIEW IF EXISTS q8f CASCADE;



-- Query 9 statements

CREATE VIEW q9a AS
SELECT country.cid as cid, cname, country.height as height, oceanAccess.oid as oid
FROM country 
JOIN oceanAccess ON country.cid = oceanAccess.cid;

CREATE VIEW q9b AS
SELECT cid, cname, ocean.oid, (height + depth) as span
FROM q9a 
JOIN ocean on q9a.oid = ocean.oid;

CREATE VIEW q9c AS
SELECT cname, max(span) as totalspan
FROM q9b
GROUP BY cname;

CREATE VIEW q9d AS
SELECT cid, cname, height
FROM country
WHERE cid NOT IN 
	(SELECT cid from oceanAccess);

CREATE VIEW q9e AS
SELECT cname, height as totalspan 
FROM q9d;

CREATE VIEW q9f AS
SELECT * FROM (
(SELECT * FROM q9c)
UNION
(SELECT * FROM q9e)) a
GROUP BY a.cname, a.totalspan;

CREATE VIEW q9g AS
SELECT cname, max(totalspan) as totalspan
FROM q9f
GROUP BY cname, totalspan;

INSERT INTO Query9(SELECT * FROM q9g);

DROP VIEW IF EXISTS q9a CASCADE;
DROP VIEW IF EXISTS q9b CASCADE;
DROP VIEW IF EXISTS q9c CASCADE;
DROP VIEW IF EXISTS q9d CASCADE;
DROP VIEW IF EXISTS q9e CASCADE;
DROP VIEW IF EXISTS q9f CASCADE;
DROP VIEW IF EXISTS q9g CASCADE;



-- Query 10 statements
CREATE VIEW q10a as
SELECT country, sum(length) as borderslength
FROM neighbour
GROUP BY country;


CREATE VIEW q10b as
SELECT country.cname, q10a.borderslength
FROM q10a 
JOIN country on country.cid = q10a.country
WHERE q10a.borderslength in 
	(SELECT max(borderslength) FROM q10a);

INSERT INTO Query10 (SELECT * from q10b);

DROP VIEW IF EXISTS q10a CASCADE;
DROP VIEW IF EXISTS q10b CASCADE;
