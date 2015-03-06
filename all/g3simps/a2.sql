-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW neighboursheight(c1id, mheight) AS
	SELECT country, MAX(height)
	FROM neighbour, country
	WHERE neighbor = cid
	GROUP BY country;

INSERT INTO query1(SELECT c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name
			FROM country c1, neighbour, country c2, neighboursheight
			WHERE c1.cid = c1id AND c1id = country AND neighbor = c2.cid AND c2.height = mheight
			ORDER BY c1name
		   );

DROP VIEW neighboursheight;

-- Query 2 statements
CREATE VIEW landlocked(c1id, c1name) AS
	SELECT cid, cname
	FROM country
	WHERE cid NOT IN (SELECT cid FROM oceanAccess);

INSERT INTO query2 (SELECT * FROM landlocked);

-- Query 3 statements
CREATE VIEW single(c1id, c1name, total) AS
	SELECT cid, cname, COUNT(neighbor)
	FROM country, neighbour
	WHERE cid = country
	GROUP BY cid, cname
	HAVING COUNT(neighbor) = 1;

INSERT INTO query3 ( SELECT c1id, c1name, cid as c2id, cname as c2name
			FROM neighbour, country, (SELECT c1id, c1name FROM single INTERSECT SELECT c1id, c1name FROM landlocked)singleland
			WHERE c1id = country AND cid = neighbor
		    );
DROP VIEW single;
DROP VIEW landlocked;

-- Query 4 statements
INSERT INTO query4 (
	SELECT cname, oname
	FROM(
		SELECT cname, oname 
		FROM oceanAccess NATURAL JOIN country NATURAL JOIN ocean

		UNION

		SELECT c.cname as cname, oname
		FROM country c, neighbour, oceanAccess o NATURAL JOIN ocean
		WHERE c.cid = neighbour.country AND
			c.cid NOT IN (SELECT cid FROM oceanAccess) AND
			neighbour.neighbor IN (SELECT cid FROM oceanAccess) AND
			 neighbour.neighbor = o.cid 
	     )countriesOcean
	ORDER BY cname ASC, oname DESC
	);


-- Query 5 statements
	INSERT INTO query5(
		SELECT cid, cname, AVG(hdi_score) AS avghdi
		FROM hdi NATURAL JOIN country
		WHERE year >= 2009 AND year <= 2013
		GROUP BY cid, cname
		ORDER BY avghdi DESC LIMIT 10
	);


-- Query 6 statements
CREATE VIEW oneyear(cid, cname, one) AS
	SELECT cid, cname, hdi_score
	FROM hdi NATURAL JOIN country
	WHERE year = 2009;

CREATE VIEW twoyear(cid, cname, two) AS
        SELECT cid, cname, hdi_score
        FROM hdi NATURAL JOIN country
        WHERE year = 2010;

CREATE VIEW threeyear(cid, cname, three) AS
        SELECT cid, cname, hdi_score
        FROM hdi NATURAL JOIN country
        WHERE year = 2011;

CREATE VIEW fouryear(cid, cname, four) AS
        SELECT cid, cname, hdi_score
        FROM hdi NATURAL JOIN country
        WHERE year = 2012;

CREATE VIEW fiveyear(cid,cname,five) AS
        SELECT cid, cname, hdi_score
        FROM hdi NATURAL JOIN country
        WHERE year = 2013;

INSERT INTO query6 (SELECT cid, cname
			FROM oneyear NATURAL JOIN twoyear NATURAL JOIN threeyear NATURAL JOIN fouryear NATURAL JOIN fiveyear
			WHERE five > four AND four > three AND three > two AND two > one
			ORDER BY cname
		     );
DROP VIEW oneyear;
DROP VIEW twoyear;
DROP VIEW threeyear;
DROP VIEW fouryear;
DROP VIEW fiveyear;


-- Query 7 statements
	INSERT INTO query7(
		SELECT DISTINCT rid, rname, SUM(rpercentage*population) AS followers
		FROM religion NATURAL JOIN country
		GROUP BY rid, rname
		ORDER BY followers DESC
	);


-- Query 8 statements



-- Query 9 statements
	INSERT INTO query9(
		SELECT DISTINCT cname, MAX(span) AS totalspan
		FROM (SELECT cname, MAX(height+depth) as span
			FROM country NATURAL JOIN oceanAccess NATURAL JOIN ocean
			GROUP BY cid, cname

			UNION

			SELECT cname, max(height) AS span
			FROM country
			WHERE cid NOT IN (SELECT cid FROM oceanAccess)
			GROUP BY cname
		     )spanning
		GROUP BY cname
		);

-- Query 10 statements
CREATE VIEW totalborder(cname, span) AS
	SELECT cname, SUM(length)
	FROM neighbour, country
	WHERE country > neighbor AND cid = country
	GROUP BY cname;

INSERT INTO query10(SELECT cname, span AS borderslength
			FROM totalborder
			WHERE span >= ALL (SELECT span FROM totalborder)
		  );

DROP VIEW totalborder;
