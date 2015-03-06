-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1 (
SELECT country1.cid AS c1id, country1.cname AS c1name, country2.cid AS c2id, country2.cname AS c2name
FROM a2.neighbour AS n1, a2.country as country2 
	INNER JOIN (SELECT c1.cid, MAX(c1.cname) as cname, MAX(c2.height) AS height
                    FROM a2.country AS c1, a2.country AS c2, a2.neighbour
                    WHERE c1.cid=a2.neighbour.country AND c2.cid=a2.neighbour.neighbor
                    GROUP BY c1.cid) country1
                    ON country1.height = country2.height
WHERE n1.country = country1.cid AND n1.neighbor = country2.cid
ORDER BY country1.cname ASC);

-- Query 2 statements
INSERT INTO Query2 (
SELECT c1.cid, c1.cname
FROM a2.country AS c1
WHERE c1.cid NOT IN (SELECT c2.cid
		    FROM a2.oceanaccess AS c2)
ORDER BY c1.cname ASC);

-- Query 3 statements
INSERT INTO Query3 (
SELECT c1.cid AS c1id, max(c1.cname) AS c1name, max(c2.cid) AS c2id, max(c2.cname) AS c2name
FROM a2.country AS c1, a2.country AS c2, a2.neighbour AS n1
WHERE c1.cid NOT IN (SELECT c2.cid
		FROM a2.oceanaccess AS c2) AND n1.country=c1.cid AND c2.cid=n1.neighbor
		GROUP BY c1.cid
		HAVING count(n1.neighbor) = 1
ORDER BY c1.cname ASC);

-- Query 4 statements
INSERT INTO Query4 (
(SELECT c1.cname, o1.oname
FROM a2.oceanaccess direct, a2.country c1, a2.ocean o1
WHERE direct.cid=c1.cid AND o1.oid=direct.oid)
UNION
(SELECT c1.cname, o1.oname
FROM a2.oceanaccess a1, a2.neighbour n1, a2.country c1, a2.ocean o1
WHERE n1.neighbor=a1.cid AND n1.country=c1.cid AND o1.oid=a1.oid)
ORDER BY cname ASC, oname DESC);

-- Query 5 statements
INSERT INTO Query5 (
SELECT h1.cid, max(c1.cname) as cname, sum(h1.hdi_score) as avghdi
FROM a2.hdi AS h1, a2.country as c1
WHERE 2014 > h1.year AND h1.year > 2008 AND c1.cid=h1.cid
GROUP BY h1.cid
ORDER BY sum(h1.hdi_score) DESC LIMIT 10);

-- Query 6 statements
INSERT INTO Query6 (
SELECT c1.cid as cid, c1.cname as cname
FROM a2.hdi AS h1, a2.hdi AS h2, a2.hdi AS h3, a2.hdi AS h4, a2.hdi AS h5, a2.country as c1
WHERE h1.year=2009 AND h2.year=2010 AND h3.year=2011 AND 
	h4.year=2012 AND h5.year=2013 AND
	h1.cid=h2.cid AND h2.cid=h3.cid AND h3.cid=h4.cid AND 
	h4.cid=h5.cid AND c1.cid=h1.cid AND
	h1.hdi_score < h2.hdi_score AND h2.hdi_score < h3.hdi_score AND 
	h3.hdi_score < h4.hdi_score AND h4.hdi_score < h5.hdi_score
ORDER BY c1.cname ASC);

-- Query 7 statements
INSERT INTO Query7 (
SELECT r1.rid as rid, max(r1.rname) as rname, sum(r1.rpercentage*c1.population) as followers
FROM a2.country as c1, a2.religion as r1
WHERE r1.cid=c1.cid
GROUP BY r1.rid
ORDER BY sum(r1.rpercentage*c1.population) DESC);

-- Query 8 statements
CREATE VIEW maxlan AS
SELECT lan.cid, lan.lname, lan.lid 
FROM a2.language lan
inner join(
	SELECT a2.language.cid, max(a2.language.lpercentage)
	FROM a2.language
	GROUP BY a2.language.cid) ss
	on lan.lpercentage = ss.max AND ss.cid = lan.cid;

INSERT INTO Query8 (
SELECT c1.cname AS c1name, c2.cname AS c2name, m1.lname AS lname
FROM a2.country AS c1, a2.country AS c2, a2.neighbour AS n, maxlan AS m1, maxlan AS m2
WHERE c1.cid = n.country AND c2.cid = n.neighbor AND 
	n.country = m1.cid AND n.neighbor = m2.cid AND m1.lid = m2.lid
ORDER BY m1.lname ASC, c1.cname DESC);
DROP VIEW maxlan;

-- Query 9 statements
CREATE VIEW database AS
(SELECT c1.cid, max(c1.height+o1.depth) AS span
FROM a2.ocean AS o1, a2.country AS c1, a2.oceanAccess AS access
WHERE access.cid = c1.cid AND access.oid = o1.oid
GROUP BY c1.cid)
UNION
(SELECT c1.cid, c1.height as span
FROM a2.country AS c1
WHERE c1.cid not in (SELECT a2.oceanAccess.cid FROM a2.oceanAccess));

INSERT INTO Query9(
SELECT a2.country.cname AS cname, d1.span AS totalspan
FROM a2.country, database d1
WHERE a2.country.cid = d1.cid AND d1.span in (SELECT max(database.span) FROM database));
DROP view database;

-- Query 10 statements
CREATE VIEW temp AS
SELECT n1.country, sum(n1.length) as borderslength
FROM a2.neighbour as n1
GROUP BY n1.country;

INSERT INTO Query10(
SELECT a2.country.cname AS cname, t1.borderslength AS borderslength
FROM a2.country, temp t1
WHERE a2.country.cid = t1.country AND t1.borderslength in (SELECT max(borderslength) FROM temp));
DROP view temp;

