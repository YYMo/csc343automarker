-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
DELETE FROM query1;

CREATE VIEW neighbors AS
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name,
c2.height AS height
FROM (country JOIN neighbour ON (cid=country)) c1
JOIN country c2 ON (c2.cid=c1.neighbor)
ORDER BY c1name ASC;

INSERT INTO query1
SELECT c1id, c1name, c2id, c2name
FROM neighbors n1
WHERE height >= ALL (
        SELECT height
        FROM neighbors n2
        WHERE n1.c1id=n2.c1id
);

DROP VIEW neighbors;



-- Query 2 statements
DELETE FROM query2;

CREATE VIEW seasideCountries AS
SELECT DISTINCT cid
FROM country JOIN oceanAccess USING (cid);

INSERT INTO query2
SELECT cid, cname
FROM country
WHERE cid <> ALL (
	SELECT * 
	FROM seasideCountries
)
ORDER BY cname ASC;

DROP VIEW seasideCountries;





-- Query 3 statements
DELETE FROM query3;

CREATE VIEW seasideCountries AS
SELECT DISTINCT cid
FROM country JOIN oceanAccess USING (cid);

CREATE VIEW  oneNeighbor AS
(SELECT country AS cid
FROM neighbour)
	EXCEPT
(SELECT n1.country AS cid
FROM neighbour n1, neighbour n2
WHERE n1.country=n2.country AND NOT n1.neighbor=n2.neighbor)
;


INSERT INTO query3
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name
FROM 	(
		country 
			JOIN 
		( (SELECT *
		FROM oneNeighbor)
			EXCEPT	
		(SELECT * 
		FROM seasideCountries) ) subcountries
		USING (cid) 
	) c1 
		JOIN 
	(neighbour JOIN country c2 ON (cid=neighbor))
	ON (c1.cid=country)
ORDER BY c1name ASC;

DROP VIEW oneNeighbor;
DROP VIEW seasideCountries;


-- Query 4 statements
DELETE FROM query4;

CREATE VIEW directaccess AS
SELECT cid, cname, oname
FROM (country JOIN oceanAccess USING (cid)) JOIN ocean USING (oid);

CREATE VIEW indirectaccess AS
SELECT country.cid AS cid, country.cname AS cname, oname
FROM (directaccess JOIN neighbour ON (cid=country)) 
	JOIN country ON (neighbor=country.cid);

INSERT INTO query4
SELECT DISTINCT cname, oname
FROM 	( (SELECT * 
	FROM directaccess)
		UNION
	(SELECT *
	FROM indirectaccess) ) a
ORDER BY cname ASC, oname DESC;

DROP VIEW indirectaccess;
DROP VIEW directaccess;




-- Query 5 statements
DELETE FROM query5;

INSERT INTO query5
SELECT cid, cname, AVG(hdi_score) AS avghdi
FROM hdi JOIN country USING (cid)
WHERE year<=2013 AND year>=2009
GROUP BY cid, cname
ORDER BY avghdi DESC
LIMIT 10;



-- Query 6 statements
DELETE FROM query6;

CREATE VIEW last5years AS 
SELECT *
FROM hdi 
WHERE year<=2013 AND year>=2009;

CREATE VIEW nonincreasing AS
SELECT h1.cid AS cid
FROM last5years h1, last5years h2
WHERE h1.cid = h2.cid AND h1.year < h2.year AND h1.hdi_score >= h2.hdi_score;

INSERT INTO query6
SELECT cid, cname
FROM 	( (SELECT cid FROM last5years)
		EXCEPT
	(SELECT cid FROM nonincreasing) ) a
	JOIN country USING (cid)
ORDER BY cname ASC;

DROP VIEW nonincreasing;
DROP VIEW last5years;




-- Query 7 statements
DELETE FROM query7;

INSERT INTO query7
SELECT rid, rname, SUM(rpercentage*population) AS followers
FROM religion JOIN country USING (cid)
GROUP BY rid, rname
ORDER BY followers DESC;



-- Query 8 statements
DELETE FROM query8;

CREATE VIEW notmostspoken AS 
SELECT DISTINCT cid, l1.lid AS lid, l1.lname AS lname, l1.lpercentage AS lpercentage
FROM language l1 JOIN language l2 USING (cid)
WHERE l1.lpercentage < l2.lpercentage;

CREATE VIEW mostspoken AS
SELECT *
FROM 
	( (SELECT * FROM language)
		EXCEPT
	(SELECT * FROM notmostspoken) ) most;

INSERT INTO query8
SELECT c1.cname AS c1name, c2.cname AS c2name, m1.lname AS lname
FROM ( (mostspoken m1 JOIN mostspoken m2 ON (m1.lid=m2.lid AND m1.cid <> m2.cid)) 
	JOIN country c1 ON (c1.cid=m1.cid) )
	JOIN country c2 ON (c2.cid=m2.cid);


DROP VIEW mostspoken;
DROP VIEW notmostspoken;




-- Query 9 statements
DELETE FROM query9;

CREATE VIEW spans AS
SELECT cname, COALESCE(height+depth, height) AS totalspan
FROM (country LEFT JOIN oceanaccess USING (cid) ) 
	LEFT JOIN ocean USING (oid);

INSERT INTO query9
SELECT *
FROM spans
WHERE totalspan >= ALL (
	SELECT totalspan
	FROM spans);

DROP VIEW spans;



-- Query 10 statements
DELETE FROM query10;

CREATE VIEW borders AS
SELECT cname, SUM(length) AS borderlength
FROM country JOIN neighbour ON (cid=country)
GROUP BY cname;

INSERT INTO query10
SELECT *
FROM borders
WHERE borderlength >= ALL (
	SELECT borderlength
	FROM borders);

DROP VIEW borders;


