-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.


-- Query 1 statements (done)
CREATE VIEW countryHeight(country, neighbor, height) AS
	SELECT ne.country, ne.neighbor, co.height
	FROM neighbour ne JOIN country co ON ne.neighbor = co.cid;
	
CREATE VIEW findMaxHeight(country, neighbor) AS
	SELECT ch.country, ch.neighbor
	FROM countryHeight ch
	JOIN (SELECT country, max(height) as height FROM countryHeight GROUP BY country) su
	ON ch.country = su.country AND ch.height = su.height;

INSERT INTO Query1 (
	SELECT country, (SELECT cname FROM country WHERE cid = findMaxHeight.country), 
	neighbor, (SELECT cname FROM country WHERE cid = findMaxHeight.neighbor) 
	FROM findMaxHeight
	ORDER BY country ASC
);


-- Query 2 statements (done)
INSERT INTO Query2 (
	SELECT co.cid, co.cname
	FROM country co
	WHERE co.cid NOT IN (SELECT cid FROM oceanAccess)
	ORDER BY co.cname ASC
);


-- Query 3 statements(done)
INSERT INTO Query3 (
	SELECT co.cid,	co.cname, 
	(SELECT neighbor FROM neighbour WHERE country = su.country), 
	(SELECT cname FROM country WHERE cid = (SELECT neighbor FROM neighbour WHERE country = su.country))
	FROM country co
	INNER JOIN
		(SELECT country, count(neighbor)
			FROM neighbour
			GROUP BY country
			HAVING count(*) = 1) su
	ON co.cid = su.country
	WHERE co.cid NOT IN (SELECT cid FROM oceanAccess)
	ORDER BY co.cid ASC
);


-- Query 4 statements (done)
INSERT INTO Query4 (
	SELECT co.cname AS country_name, oc.oname AS ocean_name
	FROM country co
	JOIN oceanAccess oa ON co.cid = oa.cid
	JOIN ocean oc ON oa.oid = oc.oid
	UNION
	SELECT co2.cname AS country_name, oc2.oname AS ocean_name
	FROM country co2
	JOIN neighbour ne ON ne.country = co2.cid
	JOIN oceanAccess oa2 ON ne.neighbor = oa2.cid
	JOIN ocean oc2 ON oa2.oid = oc2.oid
	ORDER BY country_name ASC, ocean_name DESC 
);



-- Query 5 statements (done)
INSERT INTO Query5 (
	SELECT hdi.cid, (SELECT cname FROM country WHERE hdi.cid = cid), avg(hdi.hdi_score) AS avghdi
	FROM hdi
	WHERE hdi.year >= 2009 AND hdi.year <= 2013
	GROUP BY hdi.cid
	ORDER BY avghdi DESC LIMIT 10
);



-- Query 6 statements (done)
INSERT INTO Query6 (
	SELECT h1.cid, (SELECT cname FROM country WHERE h1.cid = cid) as cname
	FROM hdi h1
	WHERE (SELECT hdi_score FROM hdi WHERE year = 2009 AND cid = h1.cid) < (SELECT hdi_score FROM hdi WHERE year = 2010 AND cid = h1.cid)
		AND (SELECT hdi_score FROM hdi WHERE year = 2010 AND cid = h1.cid) < (SELECT hdi_score FROM hdi WHERE year = 2011 AND cid = h1.cid)
		AND (SELECT hdi_score FROM hdi WHERE year = 2011 AND cid = h1.cid) < (SELECT hdi_score FROM hdi WHERE year = 2012 AND cid = h1.cid)
		AND (SELECT hdi_score FROM hdi WHERE year = 2012 AND cid = h1.cid) < (SELECT hdi_score FROM hdi WHERE year = 2013 AND cid = h1.cid)
	ORDER BY cname ASC
);



-- Query 7 statements(done)
INSERT INTO Query7 (
	SELECT re.rid, re.rname, sum(co.population*re.rpercentage/100) AS followers
	FROM religion re
	JOIN country co ON co.cid = re.cid
	GROUP BY re.rid, re.rname
	ORDER BY followers DESC
);


-- Query 8 statements
CREATE VIEW popLanByCountry(country, lname, lpercentage, neighbor) AS
	SELECT distinct ne.country, su2.lname, su1.lpercentage, ne.neighbor
	FROM neighbour ne
		JOIN(SELECT ne2.country as country, max(la.lpercentage) as lpercentage
			FROM neighbour ne2 JOIN language la ON ne2.country = la.cid
			GROUP BY ne2.country) su1
			JOIN(SELECT ne3.country as country, la2.lname as lname, la2.lpercentage as lpercentage FROM neighbour ne3 JOIN language la2 ON ne3.country = la2.cid) su2
			ON su1.country = su2.country AND su1.lpercentage = su2.lpercentage
		ON ne.country = su1.country
	ORDER BY ne.country;
		
CREATE VIEW popLanByNeighbor(country, neighbor, lname, lpercentage) AS
	SELECT distinct ne.country, su1.neighbor, su2.lname, su1.lpercentage
	FROM neighbour ne
		JOIN(SELECT ne2.neighbor as neighbor, max(la.lpercentage) as lpercentage
			FROM neighbour ne2 JOIN language la ON ne2.neighbor = la.cid
			GROUP BY ne2.neighbor) su1
			JOIN(SELECT ne3.neighbor as neighbor, la2.lname as lname, la2.lpercentage as lpercentage FROM neighbour ne3 JOIN language la2 ON ne3.neighbor = la2.cid) su2
			ON su1.neighbor = su2.neighbor AND su1.lpercentage = su2.lpercentage
		ON ne.neighbor = su1.neighbor
	ORDER BY ne.country;
	
CREATE VIEW bothTables(country, lname, lpercentage, neighbor, lname2, lpercentag2) AS
	SELECT distinct pop1.country, pop1.lname, pop1.lpercentage, pop1.neighbor, pop2.lname, pop2.lpercentage
	FROM popLanByCountry pop1
	JOIN popLanByNeighbor pop2 ON pop1.neighbor = pop2.neighbor
	ORDER BY pop1.country;
	
INSERT INTO Query8 (
	SELECT (SELECT cname FROM country WHERE cid = ta.country) as c1name, 
	(SELECT cname FROM country WHERE cid = ta.neighbor), ta.lname as lname
	FROM bothTables ta
	WHERE ta.lname = ta.lname2 
	ORDER BY lname ASC, c1name DESC
); 


-- Query 9 statements (done)
CREATE VIEW AllList(cname, height, oid, depth) AS
	SELECT co.cname, co.height, oc.oid, oc.depth
	FROM country co
	JOIN oceanAccess oa ON co.cid = oa.cid
	JOIN ocean oc ON oc.oid = oa.oid;

CREATE VIEW findMAX(cname, height, depth) AS
	SELECT cname, height, max(depth)
	FROM ALLlist
	GROUP BY cname, height;
	
CREATE VIEW noOceanAccess(cname, height) AS
	SELECT co.cname, co.height
	FROM country co
	WHERE co.cid NOT IN (SELECT cid FROM oceanAccess);
	

INSERT INTO Query9 (
	SELECT cname, (height + depth) as totalspan
	FROM findMAx
	UNION 
	SELECT cname, height FROM noOceanAccess
);



-- Query 10 statements
CREATE VIEW CountryList(cname, length) AS
	SELECT co.cname, sum(ne.length) AS length
	FROM country co
	JOIN neighbour ne ON co.cid = ne.country
	GROUP BY co.cname;

INSERT INTO Query10 (
	SELECT cname, max(length) AS boarderlength
	FROM CountryList
	GROUP BY cname
);


DROP VIEW findMaxHeight;
DROP VIEW countryHeight;
DROP VIEW bothTables;
DROP VIEW popLanByCountry;
DROP VIEW popLanByNeighbor;
DROP VIEW findMAX;
DROP VIEW AllList;
DROP VIEW noOceanAccess;
DROP VIEW CountryList;