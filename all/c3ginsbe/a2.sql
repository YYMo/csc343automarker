-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW pairs AS (
	SELECT C1.cid AS c1id, C1.cname AS c1name, C2.cid AS c2id, C2.cname AS c2name, c2.height AS c2height
	FROM a2.country C1, a2.country C2
);

CREATE VIEW maxNeighbourHeight AS (
	SELECT c1id AS country, max(c2height) AS height
	FROM pairs JOIN a2.neighbour ON (c1id = neighbour.country AND c2id = neighbour.neighbor)
	GROUP BY c1id
);

CREATE VIEW final AS(
	SELECT c1id, c1name, c2id, c2name
	FROM pairs JOIN a2.neighbour ON (c1id = country AND c2id = neighbor)
	WHERE c2height = (SELECT height FROM maxNeighbourHeight WHERE maxNeighbourHeight.country = c1id)
	ORDER BY c1name ASC
);

INSERT INTO QUERY1 (SELECT * FROM final);

DROP VIEW final;
DROP VIEW maxNeighbourHeight;
DROP VIEW pairs;

-- Query 2 statements

INSERT INTO QUERY2 (
	SELECT cid, cname
        FROM country
        WHERE cid NOT IN (SELECT cid FROM oceanAccess)
);

-- Query 3 statements

CREATE VIEW landlocked AS(
        SELECT cid, cname
        FROM a2.country
        WHERE cid NOT IN (SELECT cid FROM a2.oceanAccess)
);

CREATE VIEW twoPlusNeighbours AS(
	SELECT N1.country AS cid
	FROM a2.neighbour N1, a2.neighbour N2
	WHERE N1.country = N2.country AND N1.neighbor != N2.neighbor
);

CREATE VIEW zeroNeighbours AS(
	(SELECT cid FROM a2.country) EXCEPT
	(SELECT country AS cid FROM a2.neighbour)
);

CREATE VIEW oneNeighbour AS(
	((SELECT country AS cid FROM a2.neighbour) EXCEPT
	(SELECT * FROM twoPlusNeighbours)) EXCEPT
	(SELECT * FROM zeroNeighbours)
);

CREATE VIEW lLoN AS(
	SELECT landlocked.cid AS c1id, landlocked.cname AS c1name, oneNeighbour.cid AS c2id, country.cname AS c2name
	FROM landlocked, oneNeighbour, a2.neighbour, a2.country
	WHERE landlocked.cid = oneNeighbour.cid AND
		oneNeighbour.cid = a2.neighbour.country AND
		a2.neighbour.neighbor = country.cid
);

INSERT INTO Query3 (SELECT * FROM lLoN);

DROP VIEW lLoN;
DROP VIEW oneNeighbour;
DROP VIEW zeroNeighbours;
DROP VIEW twoPlusNeighbours;
DROP VIEW landlocked;

-- Query 4 statements

CREATE VIEW coastal  AS(
	(
	SELECT cid, oid
	FROM a2.oceanAccess
	)
	UNION
	(
	SELECT neighbor AS cid, oceanAccess.oid AS oid
	FROM a2.neighbour, a2.oceanAccess
	WHERE neighbour.neighbor = oceanAccess.cid
	)
);

CREATE VIEW final AS(
	SELECT country.cname AS cname, ocean.oname AS oname
	FROM coastal, a2.ocean, a2.country 
	WHERE coastal.cid = country.cid AND coastal.oid = ocean.oid
	ORDER BY cname ASC, oname DESC
);

INSERT INTO QUERY4 (SELECT * FROM final);

DROP VIEW final;
DROP VIEW coastal;

-- Query 5 statements

INSERT INTO Query5 (
	SELECT country.cid, cname, AVG(hdi_score) AS avghdi
	FROM a2.hdi, a2.country
	WHERE hdi.cid = country.cid AND hdi.year >= 2009 AND hdi.year <= 2013
	GROUP BY country.cid
	ORDER BY avghdi DESC
	LIMIT 10
);

-- Query 6 statements

CREATE VIEW score2009 AS(
	SELECT cid, hdi_score
	FROM a2.hdi
	WHERE year = 2009
);

CREATE VIEW score2010 AS(
	SELECT cid, hdi_score
	FROM a2.hdi
	WHERE year = 2010
);

CREATE VIEW score2011 AS(
	SELECT cid, hdi_score
	FROM a2.hdi
	WHERE year = 2011
);

CREATE VIEW score2012 AS(
	SELECT cid, hdi_score
	FROM a2.hdi
	WHERE year = 2012
);

CREATE VIEW score2013 AS(
	SELECT cid, hdi_score
	FROM a2.hdi
	WHERE year = 2013
);

CREATE VIEW increasing AS(
	SELECT s9.cid
	FROM score2009 s9, score2010 s10, score2011 s11, score2012 s12, score2013 s13
	WHERE s9.cid = s10.cid AND
		s10.cid = s11.cid AND
		s11.cid = s12.cid AND
		s12.cid = s13.cid AND
		s9.hdi_score < s10.hdi_score AND
		s10.hdi_score < s11.hdi_score AND
		s11.hdi_score < s12.hdi_score AND
		s12.hdi_score < s13.hdi_score
);

INSERT INTO Query6 (
	SELECT country.cid, cname
	FROM increasing, a2.country
	WHERE increasing.cid = country.cid
	ORDER BY cname ASC
);

DROP VIEW increasing;
DROP VIEW score2009;
DROP VIEW score2010;
DROP VIEW score2011;
DROP VIEW score2012;
DROP VIEW score2013;

-- Query 7 statements

CREATE VIEW countryReligion AS(
	SELECT country.cid, religion.rid, religion.rname, religion.rpercentage * country.population AS people
	FROM a2.country, a2.religion
	WHERE country.cid = religion.cid
);

CREATE VIEW religionFollowers AS (
	SELECT rid, SUM(people) AS followers
	FROM countryReligion
	GROUP BY rid
);

INSERT INTO Query7 (
	SELECT DISTINCT(R.rid), R.rname, followers
	FROM religionFollowers RF JOIN religion R ON RF.rid = R.rid
	ORDER BY followers DESC
);

DROP VIEW religionFollowers;
DROP VIEW countryReligion;

-- Query 8 statements

CREATE VIEW maxLanguagePercentage AS(
	SELECT C.cid, cname, max(lpercentage) AS percentage
	FROM a2.language L, a2.country C
	WHERE L.cid = C.cid
	GROUP BY C.cid
);

CREATE VIEW firstLanguage AS(
	SELECT M.cid, M.cname, L.lname
	FROM a2.language L, maxLanguagePercentage M
	WHERE L.cid = M.cid AND L.lpercentage = M.percentage
);

INSERT INTO QUERY8 (
	SELECT L1.cname AS c1name, L2.cname AS c2name, L1.lname AS lname
	FROM a2.neighbour, firstLanguage AS L1, firstLanguage AS L2
	WHERE neighbour.country = L1.cid AND neighbour.neighbor = L2.cid AND L1.lname = L2.lname
	ORDER BY lname ASC, c1name DESC
);

DROP VIEW firstLanguage;
DROP VIEW maxLanguagePercentage;

-- Query 9 statements

CREATE VIEW landLockedSpan AS (
	SELECT cname, height AS totalspan
	FROM a2.country
	WHERE cid NOT IN (SELECT cid FROM a2.oceanAccess)
);

CREATE VIEW coastalSpan AS (
	SELECT cname, height + depth AS totalspan
	FROM a2.country, a2.oceanAccess, a2.ocean
	WHERE country.cid = oceanAccess.cid AND oceanAccess.oid = ocean.oid
);

CREATE VIEW span AS(
	SELECT *
	FROM ((SELECT * FROM landLockedSpan) UNION (SELECT * FROM coastalSpan)) AS alias
);

INSERT INTO QUERY9 (
	SELECT cname, totalspan
	FROM span
	WHERE totalspan >= ALL (SELECT totalspan FROM span)
);

DROP VIEW span;
DROP VIEW coastalSpan;
DROP VIEW landLockedSpan;

-- Query 10 statements

CREATE VIEW totalBorder AS (
	SELECT country AS cid, SUM(length) as border
	FROM a2.neighbour
	GROUP BY country
);

INSERT INTO QUERY10 (
	SELECT country.cid, border
	FROM totalBorder, a2.country
	WHERE totalBorder.cid = country.cid AND border >= ALL (SELECT border FROM totalBorder)
);

DROP VIEW totalBorder;
