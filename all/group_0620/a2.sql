-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.


-- Query 1 statements 
CREATE VIEW n_maxh AS
SELECT max(height) as maxheight, country
FROM country, neighbour
WHERE cid = neighbor 
GROUP BY height, country;

INSERT INTO Query1(
SELECT n_maxh.country as c1id, c1.cname as c1name, ne.neighbor as c2id, c2.cname as c2name 
FROM country c1, country c2, n_maxh, neighbour ne
WHERE n_maxh.country = c1.cid 
AND ne.country = c1.cid
AND n_maxh.maxheight = c2.height 
AND c2.cid = ne.neighbor
ORDER BY c1name ASC); 


DROP VIEW n_maxh;

-- Query 2 statements 
INSERT INTO Query2(
SELECT cid, cname 	
FROM country
WHERE cid NOT IN (
	SELECT cid 
	FROM oceanAccess
	WHERE cid IS NOT NULL
	)
ORDER BY cname ASC
);

-- Query 3 statements 
CREATE VIEW LL AS
SELECT cid, cname
FROM country
WHERE cid NOT IN (
	SELECT cid 
	FROM oceanAccess
	WHERE cid IS NOT NULL
	);

CREATE VIEW good AS
SELECT country, neighbor 
FROM neighbour
WHERE country NOT IN(
	SELECT n1.country as country
	FROM neighbour n1, neighbour n2
	WHERE n1.country = n2.country
	AND n1.neighbor != n2.neighbor
	AND n1.country IS NOT NULL
	)
AND country IN(
	SELECT cid as country
	FROM LL
	where cid IS NOT NULL
	);

INSERT INTO Query3(
SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name
FROM country c1, country c2, good
WHERE country = c1.cid
AND neighbor = c2.cid
ORDER BY c1name ASC
);

DROP VIEW good;
DROP VIEW LL;

-- Query 4 statements 
CREATE VIEW n_acc AS
SELECT cname, oname
FROM neighbour, oceanAccess, country, ocean
WHERE neighbor = oceanAccess.cid
AND ocean.oid = oceanAccess.oid
AND neighbor = country.cid
AND oceanAccess.oid IS NOT NULL;

INSERT INTO Query4(
SELECT cname, oname
FROM oceanAccess, ocean, country
WHERE oceanAccess.oid = ocean.oid
AND oceanAccess.cid = country.cid
UNION
SELECT cname, oname
FROM n_acc
ORDER BY cname ASC, oname DESC
);

DROP VIEW n_acc;

-- Query 5 statements  
INSERT INTO Query5(
SELECT country.cid as cid, country.cname as cname, avg(hdi_score) as avghdi
FROM country, hdi
WHERE hdi.cid = country.cid
AND year < 2013 
AND year > 2009
GROUP BY country.cid
ORDER BY avghdi DESC
LIMIT 10
);


-- Query 6 statements
CREATE VIEW hdi_incr AS
SELECT h1.cid
FROM hdi h1, hdi h2, hdi h3, hdi h4, hdi h5
WHERE h1.year = 2009 
AND h2.year = 2010
AND h3.year = 2011
AND h4.year = 2012
AND h5.year = 2013
AND h2.hdi_score > h1.hdi_score
AND h3.hdi_score > h2.hdi_score
AND h4.hdi_score > h3.hdi_score
AND h5.hdi_score > h4.hdi_score;

INSERT INTO Query6(
SELECT country.cid, cname
FROM hdi_incr, country
WHERE country.cid = hdi_incr.cid
ORDER BY cname ASC
);

DROP VIEW hdi_incr;

-- Query 7 statements  
CREATE VIEW fols AS
SELECT rid, SUM(rpercentage * population) as followers
FROM religion, country
WHERE religion.cid = country.cid
GROUP BY rid;


INSERT INTO Query7(
Select fols.rid as rid, religion.rname as rname, followers
FROM religion, fols
WHERE fols.rid = religion.rid
ORDER BY followers DESC);

DROP VIEW fols;
-- Query 8 statements  
CREATE VIEW popl AS
SELECT cid, lname
FROM language
WHERE lpercentage IN(
	SELECT max(lpercentage)
	FROM language
	GROUP BY cid
	);

INSERT INTO Query8(
SELECT c1.cname as c1name, c2.cname as c2name, l1.lname as lname
FROM popl l1, popl l2, country c1, country c2, neighbour
WHERE country = c1.cid
AND neighbor = c2.cid
AND country = l1.cid
AND neighbor = l2.cid
AND l1.lname = l2.lname
ORDER BY lname ASC, c1name DESC);

DROP VIEW popl;


-- Query 9 statements          
CREATE VIEW eledepth AS
SELECT cname, height, max(depth) as odepth
FROM ocean, oceanAccess, country
WHERE country.cid IN( 
	SELECT cid
	FROM	oceanAccess
	)
GROUP BY country.cname, height
UNION
SELECT cname, height, sum(height-height) as odepth
FROM ocean, oceanAccess, country
WHERE country.cid NOT IN( 
	SELECT cid
	FROM	oceanAccess
	)
GROUP BY country.cname, height;

INSERT INTO Query9(
SELECT cname, sum(height-odepth) as totalspan
FROM eledepth
GROUP BY cname
ORDER BY totalspan ASC
Limit 1
);


DROP VIEW eledepth;
--Query 10 statements 
	INSERT INTO Query10(
	SELECT cname, sum(length) as borderslength 
	FROM country, neighbour
	WHERE country.cid = neighbour.country
	GROUP BY cname
	ORDER BY borderslength DESC
	LIMIT 1);

