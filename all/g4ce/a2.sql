-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

--Creating Views
CREATE VIEW neighbours AS (SELECT n.country, c.cid, c.cname FROM neighbour AS n JOIN country AS c ON c.cid = n.neighbor GROUP BY c.cid);
CREATE VIEW landlocked AS (SELECT cid, cname FROM country WHERE cid NOT IN (SELECT cid FROM oceanAccess) ORDER BY cname ASC);
CREATE VIEW directAccess AS (SELECT c.cname, o.oname FROM oceanAccess AS a JOIN ocean AS o ON a.oid=o.oid JOIN country AS c ON a.cid=c.cid);
CREATE VIEW between9And13 AS (SELECT * FROM hdi WHERE year BETWEEN 2009 AND 2013);
CREATE VIEW popularlanguage AS (SELECT c.cid, c.cname, l.lname, MAX(l.lpercentage) FROM country AS c JOIN language AS l ON c.cid = l.cid GROUP BY c.cid);
CREATE VIEW span AS (SELECT cname, MAX(height+depth) AS totalspan FROM country AS c JOIN oceanAccess AS oa ON c.cid=oa.cid JOIN ocean AS o ON oa.oid=o.oid GROUP BY cname);

-- Query 1 statements
INSERT INTO  Query1 (SELECT c.cid, c.cname, n.cid, n.cname FROM country AS c LEFT INNER JOIN 
neighbours AS n
ON c.cid = n.country GROUP BY c.cid ORDER BY c.cname ASC);


-- Query 2 statements
INSERT INTO Query2 (SELECT cid, cname FROM landlocked);


-- Query 3 statements
INSERT INTO Query3 (SELECT l.cid, l.cname, n.cid, n.cname FROM landlocked AS l JOIN neighbours AS n ON l.cid = n.country GROUP BY l.cid HAVING COUNT(*)=1 ORDER BY l.cname ASC);


-- Query 4 statements
INSERT INTO Query4 ( (SELECT * FROM directAccess) UNION 
	(SELECT nda.cname, da.oname FROM 
		(SELECT cid, cname FROM country WHERE cid NOT IN (SELECT cid FROM oceanAccess)) AS nda 
		JOIN neighbours AS n ON n.country=nda.cid JOIN directAccess AS da ON n.cname=da.cname)
	ORDER BY cname ASC, oname DESC);


-- Query 5 statements
INSERT INTO Query5 (SELECT c.cid, c.cname, AVG(hdi_score) AS avghdi FROM 
between9And13 AS h 
JOIN country AS c ON h.cid=c.cid 
GROUP BY c.cid 
ORDER BY avghdi DESC);


-- Query 6 statements
INSERT INTO Query6 (SELECT h.cid, c.cname FROM 
	(SELECT h1.cid FROM between9And13 AS h1 CROSS JOIN between9And13 AS h2 ON h1.cid=h2.cid CROSS JOIN 
		between9And13 AS h3 ON h2.cid=h3.cid CROSS JOIN between9And13 AS h4 ON h3.cid=h4.cid CROSS JOIN 
		between9And13 AS h5 ON h4.cid=h5.cid WHERE 
		h1.year<h2.year<h3.year<h4.year<h5.year AND 
		h1.hdi_score<h2.hdi_score<h3.hdi_score<h4.hdi_score<h5.hdi_score) AS 
	h JOIN country AS c ON h.cid=c.cid ORDER BY c.cname ASC);


-- Query 7 statements
INSERT INTO Query6 (SELECT r.rid, r.rname, SUM(c.population*r.rpercentage) AS followers FROM country AS c JOIN religion AS r ON c.cid=r.cid GROUP BY r.rid);


-- Query 8 statements
INSERT INTO Query8 (SELECT p1.cname AS c1name, p2.cname AS c2name, p1.lname FROM 
	popularlanguage AS p1 JOIN neighbour AS n ON p1.cid=n.country JOIN popularlanguage AS p2 ON n.neighbor=p2.cid 
	WHERE p1.lname=p2.lname ORDER BY p1.lname ASC, p1.cname DESC);


-- Query 9 statements
INSERT INTO Query9 (SELECT * FROM span UNION SELECT cname, height FROM country WHERE cname NOT IN (SELECT cname FROM span));


-- Query 10 statements
INSERT INTO Query10 (SELECT c.cname, SUM(length) AS borderslength FROM country AS c JOIN neighbour AS n ON c.cid=n.country GROUP BY c.cid);

-- Dropping Views
DROP VIEW neighbours;
DROP VIEW landlocked;
DROP VIEW directAccess;
DROP VIEW between9And13;
DROP VIEW popularlanguage;
DROP VIEW span;