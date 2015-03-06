-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1 (
	SELECT c1.cid AS c1id, c1.cname AS c1name, 
	       c2.cid AS c2id, c2.cname AS c2name 
	FROM (country c1 LEFT JOIN neighbour n ON c1.cid = n.country)
	      LEFT JOIN country c2 ON n.neighbor = c2.cid 
	WHERE (c2.height >= ALL (
	      	SELECT height
	      	FROM (neighbour JOIN country ON neighbor = cid) nc
	      	WHERE country = c1.cid)) 
	ORDER BY c1.cname ASC
) ;

-- Query 2 statements
INSERT INTO Query2 (
	SELECT cid, cname 
	FROM country 
	WHERE cid NOT IN (
		SELECT cid 
		FROM oceanAccess)
	ORDER BY cname ASC
) ;

-- Query 3 statements
CREATE VIEW oneNeighbour AS (
	SELECT cid 
	FROM (Query2 join neighbour on cid = country)
	GROUP BY cid
	HAVING count (neighbor) = 1
) ;

INSERT INTO Query3 (
	SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, 
	       c2.cname AS c2name
	FROM country c1, neighbour n, country c2
	WHERE (c1.cid = n.country) AND (n.neighbor = c2.cid)
	      AND (c1.cid IN (SELECT cid FROM oneNeighbour))
	ORDER BY c1.cname ASC
) ;

DROP VIEW IF EXISTS oneNeighbour CASCADE ;

-- Query 4 statements
CREATE VIEW directAccess AS (
	SELECT c.cname AS cname, o.oname AS oname
	FROM country c, oceanAccess oA, ocean o
	WHERE (c.cid = oA.cid) AND (oA.oid = o.oid)
) ;

CREATE VIEW IndirectAcess AS (
	SELECT c1.cname AS cname, d.oname AS oname
	FROM country c1, neighbour n, country c2, directAccess d
	WHERE (c1.cid = n.country) AND (n.neighbor = c2.cid) AND
	      (c2.cname = d.cname)
) ;

CREATE VIEW allAccess AS (
	(SELECT * FROM directAccess)
     UNION 
    (SELECT * FROM IndirectAcess)
) ;

INSERT INTO Query4 (
	SELECT c.cname AS cname, a.oname AS oname
    FROM (country c LEFT JOIN allAccess a ON c.cname = a.cname)
	GROUP BY c.cname, a.oname
	ORDER BY c.cname ASC , a.oname DESC
) ;

DROP VIEW  IF EXISTS directAccess CASCADE ;
DROP VIEW  IF EXISTS IndirectAcess CASCADE ;
DROP VIEW  IF EXISTS allAccess CASCADE ;

-- Query 5 statements
CREATE VIEW cidAvg AS (
	SELECT cid, AVG (hdi_score) AS avghdi
	FROM hdi
	WHERE (year >= 2009) AND (year <= 2013)
	GROUP BY cid
	ORDER BY AVG (hdi_score) DESC
	LIMIT 10
) ;

INSERT INTO Query5 (
	SELECT country.cid AS cid, cname, avghdi
	FROM (country JOIN cidAvg ON country.cid = cidAvg.cid)
) ;

DROP VIEW  IF EXISTS cidAvg CASCADE ;

-- Query 6 statements
CREATE VIEW increasingHDIS As (
	(SELECT DISTINCT cid FROM hdi)
	 EXCEPT
	(SELECT DISTINCT cid
	 FROM hdi h
	 WHERE EXISTS (
	 	SELECT hdi_score
	 	FROM hdi
	 	WHERE (h.cid = cid) AND (h.year < year) AND 
	 	      (h.hdi_score > hdi_score)
	 	)
	)
) ;

INSERT INTO Query6 (
	SELECT c.cid AS cid, c.cname AS cname
	FROM country c JOIN increasingHDIS h ON (c.cid = h.cid)
	ORDER BY c.cname ASC
) ;

DROP VIEW  IF EXISTS increasingHDIS CASCADE ;

-- Query 7 statements
CREATE VIEW followersInEach AS (
	SELECT rid, rname, (population * rpercentage) AS cfollowers
	FROM country c JOIN religion r ON c.cid = r.cid
) ;

INSERT INTO Query7 (
	SELECT rid, rname, SUM (cfollowers) AS followers 
	FROM followersInEach 
	GROUP BY rid, rname
	ORDER BY SUM (cfollowers) DESC
) ;

DROP VIEW  IF EXISTS followersInEach CASCADE ;

-- Query 8 statements
CREATE VIEW popLangs AS (
	SELECT cid, lid, lname 
	FROM language l
	WHERE lpercentage = ( SELECT MAX(lpercentage)
		                  FROM language
		                  WHERE l.cid = cid
		                ) 
) ;

CREATE VIEW cidsLang AS (
	SELECT n.country AS c1id, n.neighbor AS c2id, p1.lname AS lname  
	FROM neighbour n, popLangs p1, popLangs p2
	WHERE (n.country = p1.cid) AND (n.neighbor = p2.cid) AND
	      (n.country != n.neighbor) AND (p1.lid = p2.lid)
) ;

INSERT INTO Query8 (
	SELECT c1.cname AS c1name, c2.cname AS c2name, cL.lname AS lname
	FROM cidsLang cL, country c1, country c2
	WHERE (cL.c1id = c1.cid) AND (cL.c2id = c2.cid)  
	ORDER BY lname ASC, c1.cname DESC
);

DROP VIEW  IF EXISTS popLangs CASCADE ;
DROP VIEW  IF EXISTS cidsLang CASCADE ;

-- Query 9 statements
CREATE VIEW oceanDepth AS (
	SELECT c.cid, o.oid, o.depth
	FROM country c, oceanAccess oA, ocean o
	WHERE (c.cid = oA.cid) AND (oA.oid = o.oid)
) ;

CREATE VIEW maxDepth AS (
	SELECT c.cid, MAX(o.depth) AS maxDepth
	FROM country c FULL JOIN oceanDepth o ON c.cid = o.cid
	GROUP BY c.cid
) ;

CREATE VIEW totalSpan AS (
	SELECT c.cname AS cname, (height + maxDepth) AS span
	FROM country c JOIN maxDepth m ON c.cid = m.cid 
) ;  

INSERT INTO Query9 (
	SELECT cname, span AS totalSpan
	FROM totalSpan 
	WHERE span = (
		SELECT MAX(span)
		FROM totalSpan 
		)
) ;

DROP VIEW  IF EXISTS oceanDepth CASCADE ;
DROP VIEW  IF EXISTS maxDepth CASCADE ;
DROP VIEW  IF EXISTS totalSpan CASCADE ;

-- Query 10 statements
CREATE VIEW Allborders AS (
	SELECT cname, SUM (length) AS borderslength
	FROM country c JOIN neighbour n ON cid = country
	GROUP BY cname 
) ;

CREATE VIEW maxborders AS (
	SELECT cname, MAX(borderslength) AS borderslength
	FROM Allborders
	GROUP BY cname
) ;

INSERT INTO Query10 (
	SELECT cname , borderslength
	FROM maxborders
	WHERE borderslength = (
		SELECT MAX(borderslength)
		FROM maxborders
		)
) ;

DROP VIEW  IF EXISTS Allborders CASCADE ;
DROP VIEW  IF EXISTS maxborders CASCADE ;

