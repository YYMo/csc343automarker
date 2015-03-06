-- Add below your SQL statements.
-- You can create intermediate views (AS needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" commAND in psql to execute the SQL commANDs in this file.

-- Query 1 statements
 --[5 marks] For each country, find its neighbor country with the highest elevation point.
 --Report the id AND name of the country AND the id AND name of its neighboring country.

-- SELECT country.cid AS c1id, country.cname AS c1name, neighbour.neighbor AS c2cid , _______ AS c2name --name of neighbour. how get?
-- FROM country, neighbor
-- WHERE c1id = neighbour.country
-- 	AND height >= (SELECT height --subquery = height of all c1name's neighbors
-- 					FROM neighbor
-- 					WHERE country = c1id)
-- GROUP BY c1name
-- ORDER BY c1name ASC
----- sol 2
CREATE VIEW pairs AS
	SELECT neighbour.country AS c1id, cid AS c2id, country.cname AS c2name, country.height AS neighb_height
	FROM neighbour, country
	WHERE neighbour.neighbor = cid;

CREATE VIEW only_highest AS
	SELECT p1.*
	FROM pairs p1 LEFT OUTER JOIN pairs p2
	ON (p1.neighb_height < p2.neighb_height AND p1.c1id = p2.c1id)
	WHERE p2.neighb_height is NULL; -- = NULL ?

INSERT INTO Query1 (
	SELECT h.c1id AS c1id, cname AS c1name, h.c2id AS c2id, h.c2name	AS c2name
	-- SELECT h.c1id, cname AS c1name, h.c2id, h.c2name -- can I shorten to this?
	FROM only_highest h, country
	WHERE h.c1id = cid
	ORDER BY c1name ASC
	);

DROP VIEW only_highest CASCADE;
DROP VIEW pairs CASCADE;

-- Query 2 statements
-- [5 marks] Find the lANDlocked countries.
-- A lANDlocked country is a country entirely enclosed by lAND (e.g., SwitzerlAND).
-- Report the id(s) AND name(s) of the lANDlocked countries.
-- Output Table: Query2
-- Attributes: cid (lANDlocked country id) [INTEGER]
-- cname (lANDlocked country name) [VARCHAR(20)]
-- ￼order by: cname ASC

INSERT INTO Query2 (
	SELECT cid, cname
	FROM country
	WHERE cid NOT IN (SELECT cid -- SELECT *?
					  FROM oceanAccess
					)
	ORDER BY cname ASC
	);

-- solution 2

-- INSERT INTO Query2
-- SELECT cid, cname
-- FROM lANDlocked l, country c
-- WHERE l.cid = c.cid;

-- Query 3 statements
-- [5 marks] Find the lANDlocked countries which are surrounded by exactly one country.
-- Report the id AND name of the lANDlocked country, followed by the id AND name of the country that surrounds it.
-- Output Table: Query3
-- Attributes: c1id (lANDlocked country id) [INTEGER]
-- 			c1name (lANDlocked country name) [VARCHAR(20)]
-- 			c2id (surrounding country id)  [INTEGER]
-- 			c2name (surrounding country name) [VARCHAR(20)]
-- Order by: c1name
-- ASC

CREATE VIEW lANDlocked AS
	SELECT Query2.cname AS c1name, Query2.cid AS c1id, country.cname AS c2name, neighbor AS c2id
	FROM   Query2,country, neighbour
	WHERE Query2.cid = neighbour.country
		AND country.cid = neighbor;

INSERT INTO Query3 (
	SELECT lANDlocked.c1id AS c1id,c1name, c2id, c2name
    FROM lANDlocked, (SELECT c1id
    					FROM lANDlocked
    					GROUP BY c1id
    					HAVING count(c1id) = 1) AS neighbors
  	WHERE neighbors.c1id = lANDlocked.c1id
);

DROP VIEW lANDlocked;

-- CREATE VIEW lANDlocked AS
-- 	SELECT cid AS llcid, cname AS llcname
-- 	FROM country
-- 	WHERE cid NOT IN (SELECT cid -- SELECT *?
-- 					FROM oceanAccess
-- 					);

-- CREATE VIEW one_neighbor AS
-- 	SELECT c1.cid AS nCountrycid, n1.neighbor AS nNeighborcid
-- 	FROM country c1, neighbour n1
-- 	WHERE cid = n1.country
-- 		AND NOT EXISTS (SELECT *
-- 						FROM neighbour n2
-- 						WHERE c1.cid = n2.country
-- 							AND n1.neighbor = n2.neighbour);


-- INSERT INTO Query3 (
-- 	SELECT ll.llcid AS c1cid, ll.llcname AS c1name, 1n.nCountrycid AS c2cid, country.cname AS c2name,
-- 	FROM one_neighbor 1n, lANDlocked ll, country
-- 	WHERE country.cid = nNeighborcid AND nCountrycid = llcid
-- 	ORDER BY c1name ASC
-- );

-- DROP VIEW one_neighbor CASCADE;
-- DROP VIEW lANDlocked CASCADE;

-- MY FIRST ATTEMPT
-- SELECT country.cid AS cid, country.cname AS cname, neightbour.neighbor AS nid, ___ AS nname -- need neighbors name
-- FROM country
-- INNER JOIN neightbour
-- ON cid = neighbour.country
-- WHERE
-- 	AND COUNT(nid) = 1 -- NUMBER OF NEIGHBORING COUNTRY TUPLES IN neighbor
-- 	AND cid NOT IN (SELECT cid -- SELECT *?
-- 					FROM oceanAccess
-- 				)
-- GROUP BY cname
-- ORDER BY cname ASC


--- sol 2

-- CREATE VIEW singleton AS
-- SELECT country, neighbor
-- FROM neighbor n1
-- WHERE not exists
-- (SELECT country, neighbor
-- FROM neighbor n2
-- WHERE n1.country=n2.country AND n1.neighbor<>n2.neighbor);

-- INSERT INTO Query3
-- SELECT *
-- FROM singleton s, lANDlocked l
-- WHERE s.country=l.country;

-- DROP VIEW singleton CASCADE;
--

-- Query 4 statements
-- [5 marks] Find the accessible ocean(s) of each country.
-- An ocean is accessible by a country if either the country itself hAS a coAStline on that ocean (direct access to the ocean)
-- or the country is neighboring another country that hAS a coAStline on that ocean (indirect access).
-- Report the name of the country AND the name of the accessible ocean(s).
-- Output Table: Query4
-- Attributes: cname (country name) [VARCHAR(20)]
-- 			oname (ocean name) [VARCHAR(20)]
-- Order by: 	cname ASC,
-- 			oname DESC

-- (country in Query4) or (neighbors with (country in Query4))

-- 2nd try f this
-- CREATE VIEW indirect_only AS
-- 	SELECT neighbour.country AS cid, oceanAccess.oid AS oid
-- 	FROM oceanAccess, neighbour
-- 	WHERE neighbour.neighbor = oceanAccess.cid
-- 		AND neighbour.neighbor IN (SELECT cid FROM oceanAccess)
-- 		AND neighbour.country NOT IN (SELECT cid FROM oceanAccess);

-- INSERT INTO Query4 (
-- 	(SELECT country.cname AS cname, ocean.oname AS oname -- Either indirect access
-- 	FROM indirect_only, country, ocean
-- 	WHERE ocean.oid = indirect_only.oid
-- 		AND country.cid = indirect_only.cid
-- 	-- GROUP BY indirect_only.cid
-- 	)
-- 	UNION
-- 	(SELECT country.cname AS cname, ocean.oname -- Or direct access
-- 	FROM oceanAccess, ocean
-- 	WHERE ocean.cid = oceanAccess.cid
-- 		AND country.cid = oceanAccess.cid
-- 	);
-- 	ORDER BY cname ASC, oname DESC
-- )

-- DROP VIEW indirect_only CASCADE;

-- 3rd :(
	-- check this on over.
CREATE VIEW ocean_acc AS
	SELECT cname, oname
	FROM oceanAccess,country,ocean
     	WHERE oceanAccess.cid = country.cid AND ocean.oid= oceanAccess.oid;


create view indirect_acc AS
	SELECT cname, oname
	FROM neighbour,oceanAccess, country, ocean
	WHERE neighbor= oceanAccess.cid AND country = country.cid AND oceanAccess.oid= ocean.oid;


INSERT INTO QUERY4 (SELECT * FROM indirect_acc
		     union
		     SELECT * FROM ocean_acc
		     ORDER BY cname ASC,oname DESC);

DROP VIEW ocean_acc;
DROP VIEW indirect_acc;

-- Query 5 statements
-- [5 marks] Find the top 10 countries with the highest average Human Development Index (HDI)
-- over the 5-year period of 2009-2013 (inclusive).

-- Output Table: Query5
-- Attributes: cid (country id)
-- 			cname (country name)
-- 			avghdi countrys average HDI
-- Order by: avghdi

INSERT INTO Query5 (
	SELECT country.cid AS cid, country.cname AS cname, AVG(hdi_score) AS avghdi
	FROM hdi, country
	WHERE country.cid = hdi.cid
		AND hdi.year >= 2009
		AND hdi.year <= 2013
	GROUP BY hdi.cid, country.cid
	ORDER BY avghdi DESC
	LIMIT 10);

-- Query 6 statements

-- Find the countries for which their Human Development Index (HDI) is constantly increasing
-- over the 5-year period of 2009-2013 (inclusive).
-- Constantly increasing means that from year to year there is a positive change (increASe) in the country’s HDI.
-- Output Table: Query6
-- Attributes: cid (country id) [INTEGER]
				-- cname
-- ￼Order by:
-- cname (country name) [VARCHAR(20)] cname ASC

-- first try - something wrong!
-- CREATE VIEW valid_year AS
-- 	SELECT *
-- 	FROM hdi
-- 	WHERE year >= 2009
-- 		AND year <= 2013;

-- CREATE VIEW increasing AS
-- 	SELECT v1.cid as cid
-- 	FROM valid_year v1,
-- 	valid_year v2,
-- 	valid_year v3,
-- 	valid_year v4,
-- 	valid_year v5
-- 	WHERE   v1.cid = v2.cid
-- 		AND v2.cid = v3.cid
-- 		AND v3.cid = v4.cid
-- 		AND v4.cid = v5.cid
-- 		AND v1.year = 2009
-- 		AND v2.year = 2010
-- 		AND v2.year = 2011
-- 		AND v4.year = 2012
-- 		AND v5.year = 2013
-- 		AND (v1.hdi_score <= v2.hdi_score)
-- 		AND (v2.hdi_score <= v3.hdi_score)
-- 		AND (v3.hdi_score <= v4.hdi_score)
-- 		AND (v4.hdi_score <= v5.hdi_score);

-- INSERT INTO Query6 (
-- 	SELECT country.cname AS cname, country.cid AS cid
-- 	FROM country, increasing
-- 	WHERE country.cid = increasing.cid
-- 	ORDER BY country.cname
-- );

-- DROP VIEW valid_year CASCADE;
-- DROP VIEW increasing CASCADE;


--TRY #2!
-- CREATE VIEW year1 AS
-- 	SELECT *
-- 	FROM hdi
-- 	WHERE year = 2008;

-- CREATE VIEW year2 AS
-- 	SELECT hdi.hdi_score AS hdi_score, hdi.cid AS cid
-- 	FROM hdi, year1
-- 	WHERE hdi.year = 2011
-- 		AND hdi.hdi_score >= year1.hdi_score
-- 		AND hdi.cid = year1.cid;

-- CREATE VIEW year3 AS
-- 	SELECT hdi.hdi_score AS hdi_score, hdi.cid AS cid
-- 	FROM hdi, year2
-- 	WHERE hdi.year = 2012
-- 		AND hdi.hdi_score >= year2.hdi_score
-- 		AND hdi.cid = year2.cid;

-- CREATE VIEW year4 AS
-- 	SELECT hdi.hdi_score AS hdi_score, hdi.cid AS cid
-- 	FROM hdi, year3
-- 	WHERE hdi.year = 2012
-- 		AND hdi.hdi_score >= year3.hdi_score
-- 		AND hdi.cid = year3.cid;

-- CREATE VIEW year5 AS
-- 	SELECT hdi.hdi_score AS hdi_score, hdi.cid AS cid
-- 	FROM hdi, year4
-- 	WHERE hdi.year = 2013
-- 		AND hdi.hdi_score >= year4.hdi_score
-- 		AND hdi.cid = year4.cid;

-- INSERT INTO Query6 (
--     SELECT country.cname AS cname, country.cid AS cid
--     FROM country, year5
--     WHERE year5.cid = country.cid
--     ORDER BY country.cname
--     );

-- DROP VIEW year5 CASCADE;
-- DROP VIEW year4 CASCADE;
-- DROP VIEW year3 CASCADE;
-- DROP VIEW year2 CASCADE;
-- DROP VIEW year1 CASCADE;


-- try #3

CREATE VIEW year2009 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2009;

CREATE VIEW year2010 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2010;

CREATE VIEW year2011 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2011;

CREATE VIEW year2012 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2012;

CREATE VIEW year2013 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2013;

INSERT INTO Query6 (
	SELECT cid, cname
	FROM country, (SELECT year2009.cid as increasing_cid
					FROM year2009, year2010, year2011, year2012, year2013
					WHERE year2009.hdi_score < year2010.hdi_score
					AND year2009.cid = year2010.cid
					AND (year2010.hdi_score < year2011.hdi_score)
					AND year2010.cid = year2011.cid
					AND (year2011.hdi_score < year2012.hdi_score)
					AND year2011.cid = year2012.cid
					AND (year2012.hdi_score < year2013.hdi_score)
					AND year2012.cid = year2013.cid
					GROUP BY year2009.cid) as hdi_increase
	WHERE country.cid = increasing_cid
	ORDER BY cname
);

DROP VIEW year2009;
DROP VIEW year2010;
DROP VIEW year2011;
DROP VIEW year2012;
DROP VIEW year2013;

-- Query 7 statements

-- 5
-- marks
-- ]
-- Find the total number of people in the world that follow each religion.
-- Report the id of the religion, the name of the religion AND the respective
-- number of people that follow it .

-- Output Table: Query7 Attributes:
-- rid ( religion id) [INTEGER]
-- r name ( religion name) [VARCHAR(20)]
-- followers (number of followers) [INTEGER]
-- Order by: followers DESC

CREATE VIEW followers AS
	SELECT r.rid AS rid, r.rname AS rname,
			sum(r.rpercentage * c.population) AS followers
	FROM country c, religion r
	WHERE r.cid = c.cid
	GROUP BY rid, rname;

INSERT INTO Query7 (
	SELECT rid, rname, followers
	FROM followers
	ORDER BY followers DESC
);
DROP VIEW followers CASCADE;

-- Query8 statements
-- 5 marks ] Find all the pairs of neighboring countries that have the same most
-- popular language. For example, <Canada, USA , English> is one example
-- tuple because in both countries, English is the most popular language;
-- <Chile, Argentina, Spanish> can be another tuple , AND so on. Report the
-- names of the countries AND the name of the language.
-- Output Table: Query 8 Attributes:
-- c1name ( country name ) [VARCHAR(20)]
-- c2name ( neighboring country name) [VARCHAR(20)]
-- lname ( language name ) [VARCHAR(20)] Order by:
-- lname ASC, c1name DESC

CREATE VIEW most_pop_lan AS
	SELECT c.cid AS cid, c.cname AS cname, l.lname AS lname
	FROM country c, language l
	WHERE c.cid = l.cid
		AND l.lpercentage >= ALL (SELECT l2.lpercentage
									FROM language l2
									WHERE l2.cid = c.cid);

INSERT INTO Query8 (
	SELECT m1.cname AS c1name, m2.cname AS c2name, m1.lname AS lname
	FROM most_pop_lan m1, most_pop_lan m2, neighbour
	WHERE m1.cid <> m2.cid
		AND m1.cid = neighbour.country
		AND m2.cid = neighbour.neighbor
		AND m1.lname = m2.lname
	ORDER BY lname ASC, c1name DESC
);

DROP VIEW most_pop_lan CASCADE;

-- Query 9 statements


CREATE VIEW deepest_ocean AS
	SELECT cid, depth
	FROM ocean o1, oceanAccess
	WHERE o1.oid = oceanAccess.oid
		AND o1.depth >= ALL (SELECT depth
							FROM ocean o2, oceanAccess oa
							WHERE o2.oid = oa.oid
								AND oa.cid = oceanAccess.cid);

CREATE VIEW highest_coastal AS
	SELECT c.cid AS cid, depth + height AS height
	FROM deepest_ocean d, country c
	WHERE d.cid = c.cid;

CREATE VIEW highest_landlocked AS
	SELECT country.cid AS cid, country.height AS height
	FROM Query2 ll, country
	WHERE ll.cid = country.cid;


CREATE VIEW both_heights AS
	SELECT cid, height
	FROM
	(SELECT cid, height FROM highest_landlocked
	UNION
	SELECT cid, height FROM highest_coastal AS hc) AS both_h;

INSERT INTO Query9 (
	SELECT country.cname as cname, b1. height AS height
	FROM country, both_heights b1
	WHERE country.cid = b1.cid
		AND b1.height >= ALL (SELECT height
							FROM both_heights b2)
);

-- Query 10 statements

CREATE VIEW borders AS
	SELECT neighbour.country as cid, sum(length) AS length
	FROM neighbour
	GROUP BY neighbour.country;

INSERT INTO Query10 (
	SELECT country.cname, borders.length
	FROM borders, country
	WHERE borders.cid = country.cid
		AND borders.length >= ALL (SELECT length
								FROM borders b2)
);
DROP VIEW borders;