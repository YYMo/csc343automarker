-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

INSERT INTO Query1 (

-- Assuming if there are more thann one country of equal (max) height are allowed in the set
SELECT c2.cid, c2.cname, country.cid, country.cname
        FROM country
        JOIN neighbour n ON country.cid = n.neighbor
	JOIN country c2 ON c2.cid = n.country 
        JOIN (
		/* Pull the country and the height of its highest neighbour */
		SELECT n.country country, max(country.height) height
       		FROM country 
        	JOIN neighbour n ON country.cid = n.neighbor 
		GROUP BY n.country
	) t2 
	ON t2.country = n.country AND t2.height = country.height
ORDER BY c2.cname ASC
);

-- Query 2 statements

-- Assuming landlocked country is the one that does
-- not have access to any ocean:

CREATE OR REPLACE VIEW q2_result AS
SELECT DISTINCT cid, cname 
FROM country 
WHERE cid NOT IN
(
	SELECT cid FROM oceanaccess
);

INSERT INTO Query2 (

SELECT * FROM q2_result
ORDER BY q2_result.cname ASC
);

-- Query 3 statements

INSERT INTO Query3 (

SELECT c1.cid, c1.cname, c2.cid, c2.cname 
FROM country c1
JOIN 
(
	/* Get all countries which have negbour count of 1 (and their neighbours) */
	SELECT country, neighbor FROM neighbour WHERE country IN
	(
		SELECT country 
		FROM neighbour
		GROUP BY country
		HAVING COUNT(neighbor) = 1
	)
) q2
ON q2.country = c1.cid
JOIN country c2
ON q2.neighbor = c2.cid
WHERE c1.cid IN	--Make sure country does not have border with ocean (landlocked) or is from set in Q2 only
(   
    SELECT cid FROM q2_result
)
ORDER BY c1.cname ASC
);

-- This view is no longer needed
DROP VIEW q2_result;

-- Query 4 statements

INSERT INTO Query4 (

SELECT country.cname, ocean.oname 
FROM oceanaccess
JOIN country
ON country.cid = oceanaccess.cid
JOIN ocean
ON ocean.oid = oceanaccess.oid

UNION

SELECT country.cname, ocean.oname
FROM neighbour
JOIN oceanaccess
ON oceanaccess.cid = neighbour.country
JOIN ocean
ON ocean.oid = oceanaccess.oid
JOIN country 
ON country.cid = neighbor
WHERE neighbour.country IN 
(
        SELECT cid 
        FROM oceanaccess
)
ORDER BY cname ASC, oname DESC 
);

-- Query 5 statements

INSERT INTO Query5 (

SELECT c.cid, c.cname, q.score
FROM country AS c
JOIN
(
SELECT cid, AVG(hdi_score) as score
FROM hdi
WHERE year >= 2009 AND year <= 2013 /* TODO: in a separate subquery ? */
GROUP BY cid
ORDER BY score desc
LIMIT 10
) q
 ON q.cid = c.cid 
 ORDER BY q.score DESC
);

-- Query 6 statements

INSERT INTO Query6 (

SELECT cid, cname
FROM country 
WHERE cid =
(
	SELECT hdi1.cid FROM
	hdi hdi1
	JOIN hdi hdi2 
	ON hdi1.cid = hdi2.cid
	JOIN hdi hdi3 
	ON hdi1.cid = hdi3.cid
	JOIN hdi hdi4 
	ON hdi1.cid = hdi4.cid
	JOIN hdi hdi5
	ON hdi1.cid = hdi5.cid
	WHERE 
		    hdi1.hdi_score < hdi2.hdi_score
		AND hdi2.hdi_score < hdi3.hdi_score
		AND hdi3.hdi_score < hdi4.hdi_score
		AND hdi4.hdi_score < hdi5.hdi_score
		AND hdi1.year >= 2009 
	AND hdi2.year > hdi1.year 
	AND hdi3.year > hdi2.year
	AND hdi4.year > hdi3.year
	AND hdi5.year > hdi4.year
	AND hdi5.year <= 2013
)
ORDER BY cname ASC
);

-- Query 7 statements

INSERT INTO Query7 (

SELECT DISTINCT r.rid, religion.rname, r.religion_total
FROM religion
JOIN
(
SELECT religion.rid as rid, SUM (religion.rpercentage * country.population) as religion_total
FROM country
JOIN religion 
ON religion.cid = country.cid
GROUP BY religion.rid 
) r
ON religion.rid = r.rid
ORDER BY r.religion_total DESC
);

-- Query 8 statements

CREATE OR REPLACE VIEW country_pop_lang AS
SELECT lid, q.cid AS cid, lname
FROM language
JOIN
(
	SELECT cid, max(lpercentage) as lpercentage
	FROM language
	GROUP BY cid
) q
ON  q.cid = language.cid 
AND q.lpercentage = language.lpercentage;


INSERT INTO Query8 (

SELECT DISTINCT c1.cname, c2.cname, l.lname
FROM country_pop_lang l1
CROSS JOIN country_pop_lang l2
JOIN neighbour
ON	neighbour.country= l1.cid
 AND neighbour.neighbor = l2.cid
JOIN language AS l
ON l.lid = l1.lid
JOIN country AS c1
ON c1.cid = l1.cid
JOIN country AS c2
ON c2.cid = l2.cid 
WHERE l1.lid = l2.lid
ORDER BY l.lname ASC, c1.cname DESC
);

-- This view is no longer needed
DROP VIEW country_pop_lang;

-- Query 9 statements

INSERT INTO Query9 (

SELECT cname, d.diff
FROM
(
	SELECT cid, MAX(diff) AS diff
	FROM
	(
		/* Selects for countries with dir access */
		SELECT country.cid AS cid, (ocean.depth+country.height) AS diff 
		FROM oceanaccess
		JOIN country
		ON country.cid = oceanaccess.cid
		JOIN ocean
		ON ocean.oid = oceanaccess.oid

		UNION

		SELECT country.cid AS cid , country.height AS diff
		FROM country
		WHERE cid NOT IN 
		(
			SELECT cid FROM oceanaccess
		)
	) q
	GROUP BY cid
) d
JOIN country
ON d.cid = country.cid

);

-- Query 10 statements

INSERT INTO Query10 (

SELECT country.cname, total_border
FROM
(
	SELECT country AS cid, SUM(length) as total_border
	FROM neighbour
	GROUP BY country
) q
JOIN country
ON country.cid = q.cid
ORDER BY total_border DESC
LIMIT 1

);
