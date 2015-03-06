-- csc343 - a2 
-- Shivain Thapar - g3thapar
-- Szymon Stopyra - g3stopyr
--
-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW countries AS 
SELECT cid AS c1id, cname AS c1name, neighbor 
FROM country JOIN neighbour ON cid = country;

CREATE VIEW neighbours AS 
SELECT c1id, c1name, neighbor, cname, height 
FROM countries JOIN country ON neighbor = cid;

INSERT INTO Query1 
(SELECT c1id, c1name, neighbor AS c2id, cname AS c2name 
FROM neighbours 
WHERE height >= ALL
	(SELECT max(n.height) 
	FROM neighbours n 
	WHERE n.c1id = neighbours.c1id 
	GROUP BY n.c1id) 
ORDER BY c1name ASC); 

DROP VIEW neighbours CASCADE;
DROP VIEW countries CASCADE;


-- Query 2 statements
INSERT INTO Query2 
(SELECT cid, cname 
FROM country c 
WHERE NOT EXISTS 
	(SELECT cid 
	FROM oceanAccess 
	WHERE cid = c.cid)
ORDER BY cname ASC);

-- Query 3 statements
CREATE VIEW countries AS 
SELECT cid AS c1id, cname AS c1name, neighbor 
FROM country JOIN neighbour ON cid = country;

CREATE VIEW landlocked AS 
SELECT cid 
FROM country c 
WHERE NOT EXISTS 
	(SELECT cid 
	FROM oceanAccess 
	WHERE cid = c.cid);
	
CREATE VIEW oneCountry AS 
SELECT c1id FROM countries 
GROUP BY c1id 
HAVING COUNT(c1id) = 1;

CREATE VIEW final AS 
SELECT cid 
FROM landlocked 
INTERSECT 
(SELECT * FROM oneCountry);
	
INSERT INTO Query3 
(SELECT c1id, c1name, c.cid AS c2id, c.cname AS c2name 
FROM final JOIN countries ON cid = c1id JOIN country c ON neighbor = c.cid
ORDER BY c1name ASC);

DROP VIEW final CASCADE;
DROP VIEW oneCountry CASCADE;
DROP VIEW landlocked CASCADE;
DROP VIEW countries CASCADE;

-- Query 4 statements
CREATE VIEW countries AS 
SELECT cid AS c1id, cname AS c1name, neighbor 
FROM country JOIN neighbour ON cid = country;

CREATE VIEW ownOcean AS 
SELECT c.cid, o.oid 
FROM country c JOIN oceanAccess o ON c.cid = o.cid;

CREATE VIEW otherOceans AS 
SELECT DISTINCT c.c1id AS cid, o.oid 
FROM countries c JOIN oceanAccess o ON c.neighbor = o.cid 
ORDER BY c.c1id;

CREATE VIEW final AS 
SELECT cid, oid 
FROM otherOceans 
UNION 
(SELECT * FROM ownOcean);

INSERT INTO Query4 
(SELECT c.cname AS cname, o.oname AS oname
FROM final f JOIN ocean o ON f.oid = o.oid JOIN country c ON f.cid = c.cid 
ORDER BY c.cname ASC, o.oname DESC);

DROP VIEW final CASCADE;
DROP VIEW otherOceans CASCADE;
DROP VIEW ownOcean CASCADE;
DROP VIEW countries CASCADE;

-- Query 5 statements
INSERT INTO Query5 
(SELECT c.cid AS cid, c.cname AS cname, SUM(hdi_score) / 5 AS avghdi 
FROM country c JOIN hdi h ON c.cid = h.cid 
WHERE h.year >= 2009 
GROUP BY c.cid 
HAVING COUNT(hdi_score) >= 5 
ORDER BY SUM(hdi_score) / 5 
DESC LIMIT 10);

-- Query 6 statements
CREATE VIEW asure AS 
SELECT c.cid AS cid, c.cname AS cname 
FROM country c JOIN hdi h ON c.cid = h.cid 
WHERE h.year >= 2009 
GROUP BY c.cid 
HAVING COUNT(hdi_score) >= 5;

CREATE VIEW get_hdi AS 
SELECT a.cid AS cid, a.cname AS cname, h.year AS year, h.hdi_score AS score 
FROM asure a JOIN hdi h ON a.cid = h.cid;

INSERT INTO Query6
(SELECT g1.cid AS cid, g1.cname AS cname
FROM get_hdi g1 
WHERE year = 2013 AND g1.score > 
	(SELECT g2.score 
	FROM get_hdi g2 
	WHERE g2.cid = g1.cid AND g2.year = 2012 AND g2.score >
		(SELECT g3.score 
		FROM get_hdi g3 
		WHERE g3.cid = g2.cid AND g3.year = 2011 AND g3.score > 
			(SELECT g4.score 
			FROM get_hdi g4 
			WHERE g4.cid = g3.cid AND g4.year = 2010 AND g4.score > 
				(SELECT g5.score 
				FROM get_hdi g5 
				WHERE g5.cid = g4.cid AND g5.year = 2009))))
ORDER BY g1.cname ASC);
				
DROP VIEW get_hdi CASCADE;
DROP VIEW asure CASCADE;

-- Query 7 statements
INSERT INTO Query7 
(SELECT rid, rname, SUM(rpercentage * 
	(SELECT population 
	FROM country 
	WHERE cid = r.cid)) 
	AS followers 
FROM religion r 
GROUP BY rid, rname 
ORDER BY followers DESC);

-- Query 8 statements
CREATE VIEW countries AS 
SELECT cid AS c1id, cname AS c1name, neighbor 
FROM country JOIN neighbour ON cid = country;

CREATE VIEW neighbours AS 
SELECT c1id, c1name, neighbor, cname 
FROM countries join country on neighbor = cid;

INSERT INTO Query8 
(SELECT n.c1name AS c1name, n.cname AS c2name, l1.lname AS lname 
FROM language l1 JOIN neighbours n ON n.c1id = l1.cid JOIN language l2 ON l2.cid = n.neighbor 
WHERE
 l1.lpercentage >= ALL 
	(SELECT max(lpercentage) 
	FROM language 
	WHERE cid = n.c1id) AND
 l2.lpercentage >= ALL 
	(SELECT max(lpercentage) 
	FROM language 
	WHERE cid = n.neighbor) AND
 l1.lid = l2.lid 
ORDER BY l1.lname ASC, n.c1name DESC);
 
DROP VIEW neighbours CASCADE;
DROP VIEW countries CASCADE;

-- Query 9 statements
CREATE VIEW low AS 
(SELECT c.cid, MAX(depth) AS depth 
FROM country c LEFT JOIN oceanAccess oA ON oA.cid = c.cid LEFT JOIN ocean o ON o.oid = oA.oid 
GROUP BY c.cid 
ORDER BY c.cid ASC) 
UNION 
(SELECT cid, 0 as depth FROM country);

CREATE VIEW lowest AS 
SELECT cid, max(depth) AS depth 
FROM (SELECT * FROM low) AS new 
GROUP BY cid 
ORDER BY cid;

CREATE VIEW largest_difference AS 
SELECT (h.height + l.depth) AS totalspan 
FROM lowest l JOIN 
	(SELECT cid, height 
	FROM country) h ON l.cid = h.cid JOIN country c ON c.cid = h.cid 
ORDER BY (h.height + l.depth) 
DESC LIMIT 1;

INSERT INTO Query9 
(SELECT c.cname AS cname, (h.height + l.depth) AS totalspan 
FROM lowest l JOIN 
	(SELECT cid, height 
	FROM country) h ON l.cid = h.cid 
	JOIN country c ON c.cid = h.cid 
WHERE (h.height + l.depth) >= 
	(SELECT * 
	FROM largest_difference));

DROP VIEW largest_difference CASCADE;
DROP VIEW lowest CASCADE;
DROP VIEW low CASCADE;

-- Query 10 statements
CREATE VIEW countries AS 
SELECT cid AS c1id, cname AS c1name, neighbor, length 
FROM country JOIN neighbour ON cid = country;

CREATE VIEW maxlength AS 
SELECT SUM(length) 
FROM countries 
GROUP BY c1name 
ORDER BY SUM(length) 
DESC LIMIT 1;

INSERT INTO Query10 
(SELECT c1name AS cname, SUM(length) AS borderslength 
FROM countries 
GROUP BY c1name 
HAVING SUM(length) >= 
	(SELECT * 
	FROM maxlength));

DROP VIEW maxlength CASCADE;
DROP VIEW countries CASCADE;