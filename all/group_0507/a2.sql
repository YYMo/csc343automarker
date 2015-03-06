-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW neighbourHeights AS
SELECT country, neighbor, height
FROM country JOIN neighbour ON neighbor = cid;

CREATE VIEW shortNeighbour AS
SELECT nh1.country AS country, nh1.neighbor AS neighbor, nh1.height AS height
FROM neighbourHeights nh1, neighbourHeights nh2 
WHERE nh1.country = nh2.country AND nh1.height < nh2.height;

CREATE VIEW tallestNeighbour AS
SELECT country, neighbor
FROM neighbour
EXCEPT (SELECT country, neighbor 
		FROM shortNeighbour);

INSERT INTO Query1 (SELECT tn.country AS c1id, c1.cname AS c1name, tn.neighbor AS c2id, c2.cname AS c2name
				   	FROM tallestNeighbour tn, country c1, country c2
				   	WHERE tn.country = c1.cid AND tn.neighbor = c2.cid
				   	ORDER BY c1name ASC);

DROP VIEW tallestNeighbour;
DROP VIEW shortNeighbour;
DROP VIEW neighbourHeights;


-- Query 2 statements
INSERT INTO Query2 (SELECT cid, cname
		    FROM country
		    WHERE cid NOT IN (SELECT cid
		      			  FROM oceanAccess)
		    ORDER BY cname ASC);


-- Query 3 statements
CREATE VIEW surroundedByOne AS
SELECT country, neighbor
FROM neighbour
WHERE country NOT IN (SELECT country
		      FROM neighbour
		      GROUP BY country HAVING COUNT(neighbor) <> 1);

INSERT INTO Query3 (SELECT Q2.cid AS c1id, Q2.cname AS c1name, s.neighbor AS c2id, c.cname AS c2name
 		    FROM Query2 Q2, surroundedByOne s, country c
		    WHERE Q2.cid=s.country AND c.cid=s.neighbor
		    ORDER BY c1name ASC);

DROP VIEW surroundedByOne;

-- Query 4 statements
CREATE VIEW oceanNeighbour AS
SELECT cname, oname
FROM (ocean NATURAL JOIN oceanAccess) o, (neighbour JOIN country ON cid = neighbour.neighbor)
WHERE o.cid=neighbour.country;

INSERT INTO Query4 (SELECT cname, oname	
		    FROM oceanNeighbour
		    	UNION
		    SELECT cname, oname
		    FROM country NATURAL JOIN oceanAccess NATURAL JOIN ocean
		    ORDER BY cname ASC, oname DESC);

DROP VIEW oceanNeighbour;


-- Query 5 statements
INSERT INTO Query5 (SELECT hdi.cid AS cid, country.cname AS cname, AVG(hdi_score) AS avghdi
		    FROM hdi, country
		    WHERE (hdi.year BETWEEN 2009 AND 2013) AND country.cid = hdi.cid
		    GROUP BY hdi.cid, cname
		    ORDER BY avghdi DESC
		    LIMIT 10);


-- Query 6 statements
CREATE VIEW incrHDI AS
SELECT h1.cid as cid
FROM hdi AS h1, hdi AS h2, hdi AS h3, hdi AS h4, hdi AS h5
WHERE h1.cid=h2.cid AND h2.cid=h3.cid AND h3.cid=h4.cid AND h4.cid=h5.cid AND 
	h1.year=2009 AND h2.year=2010 AND h3.year=2011 AND h4.year=2012 AND h5.year=2013 AND
	h1.hdi_score<h2.hdi_score AND h2.hdi_score<h3.hdi_score AND h3.hdi_score<h4.hdi_score AND h4.hdi_score<h5.hdi_score;

INSERT INTO Query6 (SELECT cid, cname
		    FROM incrHDI NATURAL JOIN country
		    ORDER BY cname ASC);

DROP VIEW incrHDI;


-- Query 7 statements
INSERT INTO Query7 (SELECT rid, rname, SUM(rpercentage * population) AS followers
		    FROM country NATURAL JOIN religion
		    GROUP BY rid, rname
		    ORDER BY followers DESC);


-- Query 8 statements
CREATE VIEW popLanguagePercentage AS
SELECT cid, MAX(lpercentage) AS lpercentage 
FROM language
GROUP BY cid;

CREATE VIEW popLanguageName AS
SELECT cname, cid, lname
FROM language NATURAL JOIN popLanguagePercentage NATURAL JOIN country;

INSERT INTO Query8 (SELECT p1.cname AS c1name, p2.cname AS c2name, p2.lname AS lname
					FROM neighbour, popLanguageName p1, popLanguageName p2
					WHERE neighbour.country=p1.cid AND neighbour.neighbor=p2.cid AND p1.lname=p2.lname
					ORDER BY lname ASC, c1name DESC);

DROP VIEW popLanguageName;
DROP VIEW popLanguagePercentage;

-- Query 9 statements
CREATE VIEW withOcean AS
SELECT cname, (height + depth) AS totalspan
FROM (ocean NATURAL JOIN oceanAccess) NATURAL JOIN country;

CREATE VIEW noOcean AS
SELECT cname, height as totalspan
FROM country
WHERE cid NOT IN (SELECT cid
		      FROM oceanAccess);

INSERT INTO Query9 (SELECT *
		    	    FROM (SELECT * FROM withOcean UNION SELECT * FROM noOcean) x
		    		ORDER BY totalspan DESC
		    		LIMIT 1);

DROP VIEW withOcean;
DROP VIEW noOcean;


-- Query 10 statements
INSERT INTO Query10 (SELECT cname, SUM(length) AS borderslength
		     FROM country JOIN neighbour ON neighbour.country=country.cid
		     GROUP BY cname
		     ORDER BY borderslength DESC
		     LIMIT 1);


