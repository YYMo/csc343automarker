-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

DELETE FROM query1;
DELETE FROM query2;
DELETE FROM query3;
DELETE FROM query4;
DELETE FROM query5;
DELETE FROM query6;
DELETE FROM query7;
DELETE FROM query8;
DELETE FROM query9;
DELETE FROM query10;

-- Query 1 statements
-- For each contry, fiend its neighbor country with the highest elevation point.
-- Report the id and name of the country and the id and name of its neighboring country.
INSERT INTO query1(
	SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name
	FROM( 
		SELECT c1.cid as cid, c1.cname as cname, max(c2.height) as maxneighbourheight
		FROM neighbour, country c1, country c2
		WHERE neighbour.country = c1.cid
		AND neighbour.neighbor = c2.cid
		GROUP BY c1.cid) as c1, neighbour, country c2
	WHERE c1.maxneighbourheight = c2.height
	AND neighbour.country = c1.cid
	AND neighbour.neighbor = c2.cid
	ORDER BY c1name
);

-- Query 2 statements
-- Find the landlocked countries. Report the id(s) and name(s) of the landlocked countries
INSERT INTO query2(
	SELECT cid, cname
	FROM country
	WHERE cid NOT IN (SELECT cid from oceanAccess)
	ORDER BY cname
);


-- Query 3 statements
-- Find the landlocked countries which are surrounded by exactly one country.
-- Report the id and name of the landlocked country, followed by the id and name of the country that surrounds it.
INSERT INTO query3(
	SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name
	FROM (
		SELECT cid, cname
		FROM country, neighbour
		WHERE cid NOT IN (SELECT cid from oceanAccess)
		AND country.cid = neighbour.country
		GROUP BY cid
		HAVING COUNT(neighbour.neighbor) = 1) as c1, neighbour, country as c2
	WHERE c1.cid = neighbour.country
	AND neighbour.neighbor = c2.cid
	ORDER BY c1name
);


-- Query 4 statements
-- Find the accessible ocean(s) of each country.
-- Report the name of the country and the name of the accessible ocean(s).
INSERT INTO query4(
	SELECT country.cname as cname, ocean.oname as oname
	FROM (
		SELECT  neighbour.country as cid, oid
		FROM oceanAccess, neighbour
		WHERE neighbour.neighbor = oceanAccess.cid
		UNION
		SELECT cid,oid
		FROM oceanAccess
		) as result, country, ocean
	WHERE country.cid = result.cid
	AND ocean.oid = result.oid
	ORDER BY cname, oname DESC
);


-- Query 5 statements
-- Find the top-10 countries with the highest average Human Development Index(HDI) over the 5-year period of 2009-2013.
INSERT INTO query5(
	SELECT country.cid as cid, country.cname as cname, avg.avghdi as avghdi
	FROM (
		SELECT hdi.cid as cid, avg(hdi.hdi_score) as avghdi
		FROM hdi
		WHERE hdi.year >= 2009
		AND hdi.year <= 2013
		GROUP BY hdi.cid) as avg, country
	WHERE avg.cid = country.cid
	ORDER BY avghdi DESC
	LIMIT 10
);


-- Query 6 statements
-- Find the countries for which their Human Development Index(HDI) is constantly increasing over the 5-year period of 2009-2013.
INSERT INTO query6(
	SELECT DISTINCT country.cid as cid, country.cname as cname
	FROM hdi, country
	WHERE hdi.cid NOT IN (
		SELECT DISTINCT h1.cid as cid
		FROM hdi as h1, hdi as h2
		WHERE h1.cid = h2.cid
		AND h1.year >= 2009
		AND h1.year < 2013
		AND h2.year > 2009
		AND h2.year <= 2013
		AND h1.year < h2.year
		AND h1.hdi_score >= h2.hdi_score
		ORDER BY h1.cid)
	AND hdi.cid = country.cid
	ORDER BY cname
);


-- Query 7 statements
-- Find the total number of people in the world that follow each religion.
-- Report the id of the religion, the name of the religion and the respective number of people that follow it.
INSERT INTO query7(
	SELECT r2.rid as rid, r2.rname as rname, r1.followers as followers
	FROM (
		SELECT religion.rid as rid, round(sum(religion.rpercentage * country.population)) as followers
		FROM religion, country
		WHERE religion.cid = country.cid
		GROUP BY religion.rid) as r1, (
		SELECT distinct religion.rid as rid, religion.rname as rname
		FROM religion) as r2
	WHERE r1.rid = r2.rid
	ORDER BY followers DESC
);


-- Query 8 statements
-- Find all the pairs of neighboring countries that have the same most popular language.
INSERT INTO query8(
	SELECT c1.cname as c1name, c2.cname as c2name, l.lname
	FROM(
		SELECT neighbour.country as country, neighbour.neighbor as neighbor, l2.lname as lname
		FROM(
			SELECT language.cid as cid, language.lpercentage as lpercentage, language.lid as lid
			FROM (
				SELECT language.cid as cid, max(language.lpercentage)
				FROM language
				GROUP BY language.cid)
			as l, language
			WHERE l.cid =  language.cid
			AND l.max = language.lpercentage
		) as l1,(
			SELECT language.cid as cid, language.lid as lid, language.lname as lname
			FROM (
			        SELECT language.cid as cid, max(language.lpercentage)
			        FROM language
			        GROUP BY language.cid)
			as l, language
			WHERE l.cid = language.cid
			AND l.max = language.lpercentage
		) as l2, neighbour
		WHERE l1.cid = neighbour.country
		AND l2.cid = neighbour.neighbor
		AND l1.lid = l2.lid) as l, country as c1, country as c2
	WHERE l.country = c1.cid
	AND l.neighbor = c2.cid
	ORDER BY lname ASC, c1name DESC
);


-- Query 9 statements
-- Find the country with the larger difference between the country's highest elevation point and the depth of its deepest ocean.
INSERT INTO query9(
	SELECT country.cname as cname, o.depth + country.height as totalspan
	FROM (
		(SELECT oceanAccess.cid as cid, max(ocean.depth) as depth
		FROM oceanAccess, ocean
		WHERE ocean.oid = oceanAccess.oid
		GROUP BY oceanAccess.cid)
		UNION
		(SELECT cid, 0
		FROM country
		WHERE cid NOT IN (SELECT cid FROM oceanAccess)
		)) as o, country
	WHERE country.cid = o.cid
	ORDER BY totalspan DESC
	LIMIT 1
);


-- Query 10 statements
-- Find the country with the longest total border length.
-- Report the country and the total length of its borders.
INSERT INTO query10(
	SELECT country.cname as cname, t.borderslength as borderslength
	FROM (
		SELECT max(borderslength)
		FROM (
			SELECT sum(neighbour.length) as borderslength
			FROM neighbour
			GROUP BY country
		) as total_length) as m, (
		SELECT country as cid, sum(neighbour.length) as borderslength
		FROM neighbour
		GROUP BY country) as t, country
	WHERE m.max = t.borderslength
	AND country.cid = t.cid
);

