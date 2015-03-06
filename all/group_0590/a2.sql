-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

set search_path to a2;
-- Query 1 statements
DROP VIEW IF EXISTS neighbour_height;
DELETE FROM Query1;

CREATE VIEW neighbour_height AS 
	(SELECT c1.cid, c1.cname, 
		c2.cid as c2_cid, c2.cname as c2_cname, c2.height 
	FROM 
		neighbour join 
		country c1 on c1.cid = neighbour.country join 
		country c2 on c2.cid = neighbour.neighbor
	);

INSERT INTO Query1 (
	SELECT neighbour_height.cid, cname, c2_cid, c2_cname 
	FROM 
		neighbour_height, 
		(SELECT cid, MAX(height) as max_height FROM neighbour_height GROUP BY cid) max_height 
	WHERE 
		neighbour_height.cid = max_height.cid AND 
		neighbour_height.height = max_height 
	ORDER BY cname
);

DROP VIEW neighbour_height;

-- Query 2 statements
DELETE FROM Query2;
INSERT INTO Query2 (
	SELECT cid,cname 
	FROM country 
	WHERE cid NOT IN (SELECT cid FROM oceanAccess)
	ORDER BY cname
);


-- Query 3 statements
DELETE FROM Query3;
INSERT INTO Query3 (
	SELECT c1.cid, c1.cname, c2.cid, c2.cname 
	FROM 
		neighbour n JOIN 
		country c1 ON n.country = c1.cid JOIN 
		country c2 ON n.neighbor = c2.cid 
	WHERE c1.cid IN (
		SELECT cid 
		FROM country 
		WHERE cid NOT IN (
			SELECT cid FROM oceanAccess
		) AND (
			SELECT COUNT(*) FROM neighbour WHERE cid = country) = 1) 
	ORDER BY c1.cname
);


-- Query 4 statements
DELETE FROM Query4;
INSERT INTO Query4 (
	SELECT cname, oname 
	FROM 
		(
			(SELECT cid, oid FROM oceanAccess) 
			UNION 
			(SELECT neighbour.country, oceanAccess.oid FROM neighbour JOIN oceanAccess ON oceanAccess.cid = neighbour.neighbor)
		) r JOIN 
		country ON r.cid = country.cid JOIN 
		ocean ON r.oid = ocean.oid 
	ORDER BY cname, oname DESC
);


-- Query 5 statements
DROP VIEW IF EXISTS avg_hdi_score;
DELETE FROM Query5;
CREATE VIEW avg_hdi_score AS (
	SELECT cid, AVG(hdi_score) as avg 
	FROM hdi 
	WHERE year >= 2009 AND year <= 2013 GROUP BY cid
);
INSERT INTO Query5 (
	SELECT country.cid, cname, avg 
	FROM 
		avg_hdi_score JOIN 
		country ON country.cid = avg_hdi_score.cid 
	ORDER BY avg DESC LIMIT 10
);
DROP VIEW avg_hdi_score; 


-- Query 6 statements
DELETE FROM Query6;
INSERT INTO Query6 (
	SELECT country.cid, cname 
	FROM 
		country JOIN (
			SELECT h1.cid 
			FROM hdi h1, hdi h2 
			WHERE 
				h1.cid = h2.cid AND 
				h1.year = h2.year+1 AND 
				h1.year>=2010 AND h1.year<=2013 AND 
				h1.hdi_score-h2.hdi_score > 0 
			GROUP BY h1.cid 
			HAVING COUNT(*) = 4
		) re ON 
		re.cid = country.cid 
	ORDER BY cname
);


-- Query 7 statements
DELETE FROM Query7;
INSERT INTO Query7 (
	SELECT rid, rname, followers 
	FROM 
		(SELECT DISTINCT rid,rname FROM religion) rr NATURAL JOIN 
		(SELECT rid, SUM(population * rpercentage / 100) as followers 
		FROM religion NATURAL JOIN country 
		GROUP BY rid) calc 
	ORDER BY followers DESC
);


-- Query 8 statements
DELETE FROM Query8;
DROP VIEW IF EXISTS pop_lan;

CREATE VIEW pop_lan AS (
	SELECT language.cid, lid, lname 
	FROM 
		language, 
		(
			SELECT cid, MAX(lpercentage) 
			FROM language 
			GROUP BY cid
		) max 
	WHERE 
		language.cid = max.cid AND 
		language.lpercentage = max.max
);

INSERT INTO Query8(
	SELECT c1.cname, c2.cname, p1.lname 
	FROM 
		neighbour JOIN 
		pop_lan p1 ON neighbour.country = p1.cid JOIN 
		pop_lan p2 ON neighbour.neighbor = p2.cid JOIN 
		country c1 ON c1.cid = p1.cid 
		JOIN country c2 ON c2.cid = p2.cid 
	WHERE p1.lid = p2.lid 
	ORDER BY p1.lname, c1.cname DESC
);

DROP VIEW pop_lan;

-- Query 9 statements
DELETE FROM Query9;
DROP VIEW IF EXISTS span;

CREATE VIEW span AS (
    (SELECT cid, height as span 
        FROM country 
        WHERE cid NOT IN (SELECT cid FROM oceanAccess)) 
    UNION  
    (SELECT cid, MAX(height+depth) as span 
    FROM 
        country NATURAL JOIN 
        ocean NATURAL JOIN oceanAccess 
    GROUP BY cid)
);
INSERT INTO Query9(
	SELECT cname, span 
	FROM 
		country NATURAL JOIN (
            SELECT * FROM span WHERE span = (SELECT MAX(span) FROM span)
        ) re
);

-- Query 10 statements
DELETE FROM Query10;
DROP VIEW IF EXISTS borderlength;
CREATE VIEW borderlength AS (
	SELECT country, SUM(length) 
	FROM neighbour 
	GROUP BY country
);

INSERT INTO Query10(
	SELECT country.cname, sum 
	FROM 
		borderlength JOIN 
		country ON borderlength.country = country.cid 
	WHERE sum >= ALL(SELECT sum FROM borderlength)
);
DROP VIEW borderlength;
