-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

-- Get heights of all neighbouring countries.
CREATE VIEW heights AS
	SELECT n.country, n.neighbor, c.height
	FROM neighbour AS n, country AS c
	WHERE n.neighbor = c.cid;

-- Get the neighbouring country that has the highest elevation
CREATE VIEW h AS
	SELECT country, neighbor, height
	FROM heights AS h1
	WHERE height >= ALL (
		SELECT height 
		FROM heights AS h2 
		WHERE h1.country = h2.country);

INSERT INTO Query1 (
	SELECT c1.cid, c1.cname, c2.cid, c2.cname
	FROM h, country AS c1, country AS c2
	WHERE h.country = c1.cid AND h.neighbor = c2.cid
	ORDER BY c1.cname ASC
);

DROP VIEW IF EXISTS h;
DROP VIEW IF EXISTS heights;


-- Query 2 statements
INSERT INTO Query2 (
	SELECT DISTINCT c.cid, c.cname
	FROM country AS c, ((SELECT cid FROM country) EXCEPT (SELECT cid FROM oceanAccess)) AS d
	WHERE c.cid = d.cid
	ORDER BY cname ASC
);


-- Query 3 statements

-- Get all countries that are landlocked.
CREATE VIEW locked AS
	SELECT cid, cname
	FROM country
	WHERE cid NOT IN (
		SELECT cid 
		FROM oceanAccess);

-- Get all countries with neighbours who are landlocked by only 1 country.
CREATE VIEW onlyOne AS
	SELECT n.country AS cid, locked.cname
	FROM neighbour AS n, locked
	WHERE n.country = locked.cid
	GROUP BY n.country, locked.cname
	HAVING COUNT(n.neighbor) = 1;

INSERT INTO Query3 (
	SELECT o.cid, o.cname, n.neighbor, c.cname
	FROM neighbour AS n, onlyOne AS o, country AS c
	WHERE n.country = o.cid AND n.neighbor = c.cid
	ORDER BY o.cname ASC
);

DROP VIEW IF EXISTS onlyOne;
DROP VIEW IF EXISTS locked;


-- Query 4 statements

-- Get all countries that have direct or indirect access to an ocean.
CREATE VIEW access AS
	SELECT n.country, o.oid
	FROM neighbour AS n, oceanAccess AS o
	WHERE n.country = o.cid OR n.neighbor = o.cid
	GROUP BY n.country, o.oid;

INSERT INTO Query4 (
	SELECT c.cname, o.oname
	FROM access AS a, country AS c, ocean AS o
	WHERE a.country = c.cid AND a.oid = o.oid
	ORDER BY cname ASC, oname DESC
);

DROP VIEW IF EXISTS access;


-- Query 5 statements

-- Get the average hdi score for each country between 2009 and 2013 (inclusive).
CREATE VIEW hdiPeriod AS
	SELECT cid, AVG(hdi_score) AS avghdi
	FROM hdi
	WHERE year >= 2009 AND year <= 2013
	GROUP BY cid;

INSERT INTO Query5 (
	SELECT c.cid, c.cname, h.avghdi
	FROM hdiPeriod AS h, country AS c
	WHERE h.cid = c.cid
	ORDER BY h.avghdi DESC
	LIMIT 10
);

DROP VIEW IF EXISTS hdiPeriod;


-- Query 6 statements

-- Get hdi scores between 2009 and 2013 (inclusive).
CREATE VIEW hdiPeriod AS
	SELECT *
	FROM hdi
	WHERE year >= 2009 AND year <= 2013
	ORDER BY year DESC;

-- Get all country ids that didn't have an increase in hdi scores between 2009-2013.
CREATE VIEW nonincrease AS
	SELECT h1.cid
	FROM hdiPeriod AS h1, hdiPeriod AS h2
	WHERE h1.cid = h2.cid AND h1.year > h2.year AND h1.hdi_score <= h2.hdi_score;

-- Get all country ids that had an increase in hdi scores between 2009-2013.
CREATE VIEW increase AS
	SELECT h1.cid
	FROM hdiPeriod AS h1, hdiPeriod AS h2
	WHERE h1.cid = h2.cid AND h1.year > h2.year AND h1.hdi_score > h2.hdi_score;


-- Get all country ids that have hdi scores that were constantly increasing.
CREATE VIEW increasing AS
	(SELECT cid FROM increase) EXCEPT (SELECT cid FROM nonincrease);

INSERT INTO Query6 (
	SELECT c.cid, c.cname
	FROM increasing AS i, country AS c
	WHERE i.cid = c.cid
	ORDER BY c.cname ASC
);

DROP VIEW IF EXISTS increasing;
DROP VIEW IF EXISTS increase;
DROP VIEW IF EXISTS nonincrease;
DROP VIEW IF EXISTS hdiPeriod;


-- Query 7 statements

-- Assuming 0 <= rpercentage <= 1
INSERT INTO Query7 (
	SELECT rid, rname, SUM(rpercentage * population) AS followers
	FROM religion AS r, country AS c
	WHERE r.cid = c.cid
	GROUP BY rid, rname
	ORDER BY followers DESC
);


-- Query 8 statements

-- Get the most popular language in every country
CREATE VIEW mostPop AS
	SELECT cid, lid, lname, lpercentage
	FROM language AS l1
	WHERE lpercentage >= ALL (
		SELECT l2.lpercentage
		FROM language AS l2
		WHERE l1.cid = l2.cid);

CREATE VIEW neighbouring AS
	SELECT m1.cid AS cid1, m1.lname AS lname1, m2.cid AS cid2, m2.lname AS lname2
	FROM neighbour AS n, mostPop AS m1, mostPop AS m2
	WHERE n.country = m1.cid AND n.neighbor = m2.cid;

INSERT INTO Query8 (
	SELECT c1.cname, c2.cname, n.lname1
	FROM country AS c1, country AS c2, neighbouring AS n
	WHERE c1.cid = n.cid1 AND c2.cid = n.cid2 AND lname1 = lname2
	ORDER BY lname1 ASC, c1.cname DESC
);

DROP VIEW IF EXISTS neighbouring;
DROP VIEW IF EXISTS mostPop;


-- Query 9 statements

-- Get all countries that are landlocked
CREATE VIEW locked AS
	SELECT cname, height, 0 AS depth
	FROM country
	WHERE cid NOT IN (
		SELECT cid 
		FROM oceanAccess);

-- Get all countries that are not landlocked
CREATE VIEW notLocked AS
	SELECT cname, height, MAX(depth)
	FROM country AS c, oceanAccess AS oa, ocean AS o 
	WHERE c.cid = oa.cid AND oa.oid = o.oid
	GROUP BY cname, height;

CREATE VIEW totalSpan AS
	SELECT cname, (u.height + u.depth) AS totalspan
	FROM ((SELECT * FROM locked) UNION (SELECT * FROM notLocked)) AS u
	ORDER BY totalspan DESC;

INSERT INTO Query9 (
	SELECT cname, totalspan
	FROM totalSpan
	WHERE totalspan >= ALL (SELECT totalspan FROM totalSpan)
);

DROP VIEW IF EXISTS totalSpan;
DROP VIEW IF EXISTS notLocked;
DROP VIEW IF EXISTS locked;


-- Query 10 statements

-- Get all countries and their total border length
CREATE VIEW border AS
	SELECT country, SUM(length) AS borderslength
	FROM neighbour
	GROUP BY country;

INSERT INTO Query10 (
	SELECT cname, borderslength
	FROM border AS b, country AS c
	WHERE b.country = c.cid AND b.borderslength >= ALL (SELECT borderslength FROM border)
);

DROP VIEW IF EXISTS border;

