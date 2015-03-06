-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

--Get c1name, c1id in ascending order by c1name.

CREATE VIEW c2 AS SELECT cid AS c2id, height FROM country;

-- natural join by neighbor -> nieghbour with country
CREATE VIEW c1 AS
SELECT country AS c1id, neighbor AS c2id
FROM neighbour
INNER JOIN country
ON neighbour.neighbor=country.cid;

--all neighbors and their heights
CREATE VIEW mid AS
SELECT c1id, c1.c2id, height
FROM c1
JOIN c2
ON c2.c2id=c1.c2id;

--only tallest neighbours
CREATE VIEW tallest AS
SELECT mid.c1id, c2id, height
FROM    (SELECT  c1id, MAX(height) AS high
        FROM mid
        GROUP BY c1id) AS midfinal
JOIN mid
ON ((mid.height = midfinal.high) and mid.c1id=midfinal.c1id);

--adding first country names
CREATE VIEW finally AS
SELECT c1id, cname AS c1name, c2id
FROM tallest
JOIN country
ON (c1id=cid);

--adding neighbour country name
INSERT INTO Query1 SELECT c1id, c1name, c2id, cname AS c2name
FROM finally
JOIN country
ON (c2id=cid)
ORDER BY c1name ASC;

DROP VIEW finally CASCADE;
DROP VIEW tallest CASCADE;
DROP VIEW mid CASCADE;
DROP VIEW c1 CASCADE;
DROP VIEW c2 CASCADE;

-- Query 2 statements

--easy...?
INSERT INTO Query2 SELECT cid, cname FROM country
WHERE cid not in (SELECT cid FROM oceanaccess)
ORDER BY cname ASC;

-- Query 3 statements

CREATE VIEW landlock AS
SELECT cid, cname FROM country
WHERE cid not in (SELECT cid FROM oceanaccess)
ORDER BY cname ASC;

--only countries with one neighbour
CREATE VIEW solo AS
SELECT country.cid AS c1id, cname AS c1name
FROM    (SELECT  country AS cid, COUNT(country) AS total
        FROM neighbour
        GROUP BY cid) AS solo
JOIN country
ON ((total = 1) and country.cid=solo.cid);


INSERT INTO Query3 SELECT c1id, c1name, c2id, cname AS c2name
FROM (SELECT c1id, c1name, neighbor AS c2id
	FROM solo
	JOIN neighbour
	ON (solo.c1id=neighbour.country)) AS final
JOIN country
ON (final.c2id=country.cid)
ORDER BY cname ASC;

DROP VIEW final CASCADE;
DROP VIEW solo CASCADE;
DROP VIEW landlock CASCADE;

-- Query 4 statements

--countries with direct ocean access
CREATE VIEW direct AS
SELECT cid, cname, inter.oid, oname
FROM (SELECT oceanaccess.cid, oceanaccess.oid, country.cname
	FROM oceanaccess
	JOIN country
	ON oceanaccess.cid=country.cid) AS inter
JOIN ocean
ON (inter.oid=ocean.oid);

--countries with indirect ocean access
CREATE VIEW indirect AS
SELECT cid, cname, inter2.oid, oname
FROM (SELECT inter.cid, cname, oceanaccess.oid
	FROM (SELECT * FROM country
		JOIN neighbour
		ON (country.cid=neighbour.country)) AS inter
	JOIN oceanaccess
	ON inter.neighbor=oceanaccess.cid) AS inter2
JOIN ocean
ON (inter2.oid=ocean.oid);

INSERT INTO Query4 SELECT cname, oname
FROM (SELECT * FROM direct
	UNION
	SELECT * FROM indirect) AS inter
ORDER BY cname ASC, oname DESC;

DROP VIEW indirect CASCADE;
DROP VIEW direct CASCADE;

-- Query 5 statements

--only countries with one neighbour
CREATE VIEW avgh AS
SELECT country.cid, average
FROM    (SELECT  cid, AVG(hdi_score) AS average
        FROM (
			SELECT *
			FROM hdi 
			WHERE hdi.year in (2009, 2010, 2011, 2012, 2013)
			) as hdi2
        GROUP BY cid) AS inter
JOIN country
ON country.cid=inter.cid
ORDER BY average DESC;


INSERT INTO Query5 SELECT cid, cname, average FROM
 (SELECT country.cid, cname, average FROM avgh
 JOIN country
 ON avgh.cid=country.cid) AS inter
ORDER BY average DESC
LIMIT 10;

DROP VIEW avgh CASCADE;
-- Query 6 statements

--cid of countries with at least 1 decreasing hdi_score in the 5yr span.
CREATE VIEW decrement AS
SELECT hdi.cid
FROM hdi JOIN hdi AS hdi2 ON hdi.cid = hdi2.cid
WHERE (hdi.year < hdi2.year and hdi.hdi_score >= hdi2.hdi_score);

INSERT INTO Query6 SELECT cid, cname FROM country
WHERE cid NOT IN (SELECT cid FROM decrement)
ORDER BY cname ASC;

DROP VIEW decrement CASCADE;

-- Query 7 statements	

INSERT INTO query7 
SELECT rid, rname, SUM(rpercentage * population) AS followers 
FROM country JOIN religion ON country.cid=religion.cid
GROUP BY rid, rname
ORDER BY followers DESC;

-- Query 8 statements

--countries that share language
CREATE VIEW pairs AS
SELECT lan1.cid AS c1id, lan2.cid as c2id, lan1.lname
FROM language AS lan1
JOIN language AS lan2
ON lan1.lname=lan2.lname and lan1.cid != lan2.cid;

CREATE VIEW final AS
SELECT country.cname AS c1name, c2id, lname FROM pairs JOIN country
ON country.cid=pairs.c1id;

INSERT INTO Query8 SELECT c1name, country.cname AS c2name, lname FROM final JOIN country
ON country.cid=final.c2id ORDER BY lname ASC, c1name DESC;

-- Query 9 statements

CREATE VIEW deepestocean AS
SELECT cid, MAX(depth) AS maxdepth
FROM (SELECT * FROM ocean 
JOIN oceanaccess
ON ocean.oid=oceanaccess.oid) as inter
GROUP BY cid;


INSERT INTO Query9 
SELECT country.cid, (maxdepth + height) AS totalspan FROM deepestocean 
JOIN country
ON deepestocean.cid=country.cid
ORDER BY totalspan DESC
LIMIT 1;

DROP VIEW deepestocean CASCADE;

-- Query 10 statements
INSERT INTO Query10
SELECT cname, borderslength
FROM    (SELECT  country AS cid, SUM(length) AS borderslength
        FROM neighbour
        GROUP BY cid) AS inter
JOIN country
ON inter.cid=country.cid 
ORDER BY borderslength DESC
LIMIT 1;
