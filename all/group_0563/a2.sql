-- Add below your SQL statements.
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

INSERT INTO Query1(
SELECT q1.cid AS c1id, q1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name FROM
	(SELECT c1.cid, c1.cname, MAX(c2.height) AS neighborHeight FROM country c1
		INNER JOIN neighbour n ON c1.cid = n.country
		INNER JOIN country c2 ON n.neighbor = c2.cid
		GROUP BY c1.cid) q1
INNER JOIN neighbour n ON q1.cid = n.country
INNER JOIN country c2 ON n.neighbor = c2.cid AND q1.neighborHeight = c2.height
ORDER BY c1name
);

-- Query 2 statements

INSERT INTO Query2(
SELECT c1.cid, c1.cname FROM country c1 INNER JOIN
	(SELECT cid FROM country EXCEPT SELECT cid FROM oceanAccess) q1
ON c1.cid = q1.cid
ORDER BY c1.cname
);

-- Query 3 statements

INSERT INTO Query3(
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name FROM
	(SELECT cid FROM
		(SELECT cid, COUNT(neighbor) AS neighbors FROM neighbour n
		INNER JOIN Query2 q1 ON n.country = q1.cid GROUP BY cid) q2
	WHERE q2.neighbors = 1) q3
INNER JOIN country c1 ON q3.cid = c1.cid
INNER JOIN neighbour n ON c1.cid = n.country
INNER JOIN country c2 ON n.neighbor = c2.cid
ORDER BY c1name
);

-- Query 4 statements

INSERT INTO Query4(
SELECT DISTINCT cname, oname FROM
	(SELECT cid, oid FROM oceanAccess UNION
		(SELECT n.country AS cid, oA.oid FROM oceanAccess oA
		INNER JOIN neighbour n ON oA.cid = n.neighbor)) q1
INNER JOIN country c1 ON q1.cid = c1.cid
INNER JOIN ocean o1 ON q1.oid = o1.oid
ORDER BY cname, oname DESC
);

-- Query 5 statements

INSERT INTO Query5(
SELECT cid, cname, avghdi FROM
	(SELECT cid, AVG(hdi_score) AS avghdi FROM hdi
	WHERE 2009 <= year AND year <= 2013
	GROUP BY cid ORDER BY avghdi DESC LIMIT 10) q1
INNER JOIN country c1 USING (cid)
ORDER BY avghdi DESC
);

-- Query 6 statements

INSERT INTO Query6(
SELECT cid, cname FROM country JOIN hdi h1 USING (cid) JOIN hdi h2 USING (cid)
WHERE h1.year-h2.year=-1 AND 2009<=h1.year AND h2.year<=2013 AND h1.hdi_score<h2.hdi_score
GROUP BY cid HAVING COUNT(*)=(2013-2009)
ORDER BY cname
);

-- Query 7 statements

INSERT INTO Query7(
SELECT rid, rname, SUM(rpercentage * population) AS followers
FROM religion INNER JOIN country USING (cid)
GROUP BY rid, rname
ORDER BY followers DESC
);

-- Query 8 statements

CREATE VIEW poplanguage AS
SELECT * FROM (SELECT cid, MAX(lpercentage) AS lpercentage FROM language GROUP BY cid) q1
	NATURAL JOIN language;

INSERT INTO Query8(
SELECT c1.cname AS c1name, c2.cname AS c2name, p1.lname
FROM poplanguage p1 INNER JOIN poplanguage p2 ON p1.lid = p2.lid AND p1.cid != p2.cid
INNER JOIN neighbour n ON p1.cid = n.country AND p2.cid = n.neighbor
INNER JOIN country c1 ON c1.cid = n.country
INNER JOIN country c2 ON c2.cid = n.neighbor
ORDER BY lname, c1name DESC
);

DROP VIEW poplanguage;

-- Query 9 statements

CREATE VIEW maxspan AS
SELECT cid, MAX(span) AS totalspan FROM (
	SELECT cid, height + depth AS span FROM country
	INNER JOIN oceanAccess USING (cid)
	INNER JOIN ocean USING (oid)
	UNION
	SELECT cid, height AS span FROM country
	WHERE cid NOT IN (SELECT cid FROM oceanAccess)) q1
GROUP BY cid;

INSERT INTO Query9(
SELECT cname, totalspan FROM maxspan INNER JOIN country USING (cid)
WHERE totalspan >= ALL (SELECT totalspan FROM maxspan)
);

DROP VIEW maxspan;

-- Query 10 statements

CREATE VIEW maxlength AS
SELECT country, SUM(length) AS borderslength FROM neighbour GROUP BY country;

INSERT INTO Query10(
SELECT cname, borderslength FROM maxlength INNER JOIN country ON country.cid = maxlength.country
WHERE borderslength >= ALL (SELECT borderslength FROM maxlength)
);

DROP VIEW maxlength;
