-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW TallestNeighbour As
	SELECT max(height), n.country 
	FROM  country as c join neighbour as n on c.cid = n.neighbor 
	GROUP BY n.country 
	ORDER BY n.country;

CREATE VIEW  Neig AS
	SELECT *
	FROM  neighbour join country on cid = neighbor;

CREATE VIEW CountryNeig AS
	SELECT t.country, n.neighbor
	FROM TallestNeighbour as t join Neig as n on t.country = n.country and max = height;


INSERT INTO Query1 (
	SELECT  c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name
	FROM CountryNeig as cn join country as c1 on c1.cid = cn.country join country as c2 on c2.cid = cn.neighbor
	ORDER BY c1.cname

);

DROP VIEW CountryNeig CASCADE;
DROP VIEW Neig;
DROP VIEW TallestNeighbour;

-- Query 2 statements

INSERT INTO Query2 (

SELECT cid,cname 
FROM country
WHERE cid NOT IN 
   (SELECT cid
     FROM oceanAccess
     WHERE cid is NOT NULL
    )
ORDER BY cname

);

-- Query 3 statements

CREATE VIEW NeighbourCount AS
	SELECT country, count(neighbor)
	FROM neighbour
	GROUP BY country;

CREATE VIEW Surrounded AS
	SELECT cid as c1id, cname as c1name
	FROM country
	WHERE cid IN
		(
		SELECT country
		FROM NeighbourCount
		WHERE count = 1
		)
	AND cid NOT IN
		(
		SELECT cid
		FROM oceanAccess
		)
	;

CREATE VIEW c2id AS
	SELECT neighbor AS c2id
	FROM Surrounded, neighbour
	WHERE c1id = country;

CREATE VIEW c2name AS
	SELECT cname as c2name
	FROM c2id, country
	WHERE c2id = cid;

INSERT INTO Query3(
SELECT * FROM Surrounded, c2id, c2name
ORDER BY c1name ASC
);

DROP VIEW NeighbourCount CASCADE;
-- Query 4 statements

INSERT INTO Query4(
SELECT cname, oname
FROM country NATURAL JOIN ocean NATURAL JOIN oceanAccess
WHERE cid IN (SELECT cid FROM oceanAccess)

UNION

SELECT cname, oname
FROM country, neighbour, ocean, oceanAccess
WHERE country.cid = neighbour.country
AND neighbour.neighbor in (SELECT cid FROM oceanAccess)

ORDER BY cname ASC, oname DESC
);

-- Query 5 statements

INSERT INTO Query5(
SELECT avg(country.cid) as cid, cname, avg(hdi_score) as avghdi
FROM country JOIN hdi ON year >= 2009 AND year <= 2013 AND country.cid = hdi.cid
GROUP BY cname
ORDER BY avghdi DESC
Limit 10
);


-- Query 6 statements

INSERT INTO Query6 ( 
SELECT c1.cid, c1.cname
FROM country as c1, hdi as h1, hdi as h2, hdi as h3, hdi as h4, hdi as h5
WHERE c1.cid = h1.cid AND c1.cid = h2.cid AND c1.cid = h3.cid AND c1.cid = h4.cid AND c1.cid = h5.cid
AND h1.year = 2009 AND h2.year = 2010 AND h3.year = 2011 AND h4.year = 2012 AND h5.year = 2013
AND h1.hdi_score < h2.hdi_score AND h2.hdi_score < h3.hdi_score AND h3.hdi_score < h4.hdi_score
AND h4.hdi_score < h5.hdi_score
ORDER BY c1.cname  
);

-- Query 7 statements

INSERT INTO Query7(
SELECT rid, rname, ceil(population * rpercentage / 100) as followers
FROM religion NATURAL JOIN country
ORDER by ceil(population * rpercentage / 100) DESC
);



-- Query 8 statements

CREATE VIEW TopPercent as
SELECT l.cid, max(l.lpercentage) 
FROM language as l, country as c
WHERE l.cid = c.cid
GROUP BY l.cid;

CREATE VIEW TopName as
SELECT l.cid, l.lname
FROM language as l, TopPercent as t
WHERE l.cid = t.cid AND l.lpercentage = t.max;

INSERT INTO Query8 (
SELECT c1.cname as c1name, c2.cname as c2name, t1.lname
FROM TopName as t1, TopName as t2 natural join Neighbour, country as c1, country as c2
WHERE t1.cid < t2.cid AND t1.lname = t2.lname AND t1.cid = country AND t2.cid = neighbor AND t1.cid = c1.cid AND t2.cid = c2.cid 
ORDER BY lname ASC, c1name DESC
);

DROP VIEW TopPercent CASCADE;



-- Query 9 statements

INSERT INTO Query9 (

SELECT cname, depth + height as totalspan
FROM country as c, oceanaccess as oa, ocean as o
WHERE oa.oid = o.oid AND c.cid = oa.cid
	
UNION 
	
SELECT cname, height as totalspan
FROM country
	 

ORDER BY totalspan desc
LIMIT 1
);

-- Query 10 statements

INSERT INTO Query10(
SELECT cname, sum(length) as borderslength
FROM country JOIN neighbour ON cid = country 
GROUP By cname 
ORDER BY sum(length) desc limit 1  
);






