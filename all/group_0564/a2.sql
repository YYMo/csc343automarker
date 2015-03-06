-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW high AS
	SELECT country, neighbor
	FROM country, neighbour n1
	WHERE
		country.cid = n1.neighbor AND
		height >= ALL
			(SELECT height FROM country JOIN neighbour ON cid = neighbor
			WHERE neighbour.country = n1.country);

INSERT INTO Query1 (
	SELECT c1.cid c1id, c1.cname c1name, c2.cid c2id, c2.cname c2name
	FROM country c1, country c2, high
	WHERE
		high.country  = c1.cid AND
		high.neighbor = c2.cid
	ORDER BY c1.cname
);

DROP VIEW high;


-- Query 2 statements
INSERT INTO Query2 (
	SELECT country.cid, cname
	FROM
		((SELECT cid FROM country) EXCEPT (SELECT cid FROM oceanAccess)) ids
		JOIN country ON ids.cid = country.cid
	ORDER BY cname
);


-- Query 3 statements
INSERT INTO Query3 (
	SELECT c1.cid c1id, c1.cname c1name, c2.cid c2id, c2.cname c2name
	FROM
		(
			(
				(SELECT cid FROM country)
				EXCEPT
				(SELECT cid FROM oceanAccess)
			)
			INTERSECT
			(
				SELECT country cid
				FROM neighbour
				GROUP BY country
				HAVING COUNT(neighbour) = 1
			)
		) ids,
		neighbour, country c1, country c2
	WHERE
		ids.cid = neighbour.country AND
		neighbour.country = c1.cid AND
		neighbour.neighbor = c2.cid
	ORDER BY c1.cname
);


-- Query 4 statements
INSERT INTO Query4 (
	SELECT * FROM (
		(
			SELECT country.cname cname, oname
			FROM neighbour, oceanAccess, country, ocean
			WHERE
				country.cid = neighbour.country AND
				oceanAccess.oid = ocean.oid AND
				(neighbour.country = oceanAccess.cid OR
				neighbour.neighbor = oceanAccess.cid)
		) UNION (
			SELECT country.cname cname, oname
			FROM country, ocean, oceanAccess
			WHERE
				oceanAccess.oid = ocean.oid AND
				country.cid = oceanAccess.cid
		)
	) ans
	ORDER BY cname, oname DESC
);


-- Query 5 statements
INSERT INTO Query5 (
	SELECT country.cid cid, country.cname cname, AVG(hdi_score) avghdi
	FROM hdi JOIN country ON hdi.cid = country.cid
	WHERE year >= 2009 AND year <= 2013
	GROUP BY country.cid, country.cname
	ORDER BY AVG(hdi_score) DESC
	LIMIT 10
);


-- Query 6 statements
INSERT INTO Query6 (
	SELECT * FROM (
		(
			SELECT country.cid cid, country.cname cname
			FROM country, hdi h1, hdi h2
			WHERE
				country.cid = h1.cid AND
				h1.cid = h2.cid AND
				h1.year >= 2009 AND h1.year <= 2013 AND
				h2.year >= 2009 AND h2.year <= 2013 AND
				h1.year > h2.year AND
				h1.hdi_score > h2.hdi_score
		) EXCEPT (
			SELECT country.cid cid, country.cname cname
			FROM country, hdi h1, hdi h2
			WHERE
				country.cid = h1.cid AND
				h1.cid = h2.cid AND
				h1.year >= 2009 AND h1.year <= 2013 AND
				h2.year >= 2009 AND h2.year <= 2013 AND
				h1.year > h2.year AND
				h1.hdi_score <= h2.hdi_score
		)
	) ans
	ORDER BY cname
);


-- Query 7 statements
INSERT INTO Query7 (
	SELECT rid, rname, SUM(population * rpercentage) followers
	FROM country JOIN religion ON country.cid = religion.cid
	GROUP BY rid, rname
	ORDER BY SUM(population * rpercentage) DESC
);


-- Query 8 statements
CREATE VIEW pop_lang AS
	SELECT cid, lid, lname
	FROM language lang1
	WHERE lang1.lpercentage >= ALL
		(SELECT lpercentage FROM language lang2 WHERE lang1.cid = lang2.cid)
;

INSERT INTO Query8 (
	SELECT c1.cname c1name, c2.cname c2name, plang1.lname lname
	FROM country c1, country c2, neighbour, pop_lang plang1, pop_lang plang2
	WHERE
		c1.cid = neighbour.country AND 
		c2.cid = neighbour.neighbor AND
		c1.cid = plang1.cid AND 
		c2.cid = plang2.cid AND
		plang1.lid = plang2.lid
	ORDER BY plang1.lname, c1.cname DESC
);

DROP VIEW pop_lang;


-- Query 9 statements
CREATE VIEW landlocked AS
	SELECT cname, height totalspan
	FROM
		((SELECT cid FROM country)
		EXCEPT
		(SELECT cid FROM oceanAccess)) ids
		JOIN country ON ids.cid = country.cid;

CREATE VIEW withocean AS
	SELECT country.cname cname, (height + depth) totalspan
	FROM country, oceanAccess, ocean
	WHERE country.cid = oceanAccess.cid AND oceanAccess.oid = ocean.oid
;

INSERT INTO Query9 (
	SELECT cname, totalspan
	FROM ((SELECT * FROM landlocked) UNION (SELECT * FROM withocean)) ans
	ORDER BY totalspan DESC
	LIMIT 1
);

DROP VIEW withocean;
DROP VIEW landlocked;


-- Query 10 statements
INSERT INTO Query10 (
	SELECT country.cname cname, SUM(neighbour.length) borderslength
	FROM country JOIN neighbour ON country.cid = neighbour.country
	GROUP BY neighbour.country, country.cname
	ORDER BY SUM(neighbour.length) DESC
	LIMIT 1
);


