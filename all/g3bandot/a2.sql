-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
-- table of country (as cid), its neighbour (as neighbor), and the neighbours height (as height)
CREATE VIEW TableCountrysAndHeight AS
	(SELECT cid, neighbor, height FROM (
		-- table of all the countries (in cid) and its neighbours (in neighbor)
		(SELECT country as cid, neighbor
		FROM neighbour) AS TableCountrysAndNeighbour
		NATURAL JOIN
		-- table of all countries (in neighbor) and its height (as height)
		(SELECT cid as neighbor, height
		FROM country) AS TableCountry))
;

-- table of countries (as cid) and its neighbor country's height with the highest elevation point (as height)
CREATE VIEW TableCountryAndNeighbourHeight AS
	SELECT cid, max(height) AS height FROM TableCountrysAndHeight GROUP BY cid
;

-- height with the highest elevation point (as height)
CREATE VIEW Answer_1 AS
	SELECT country.cid, cname, TableCountryAndNeighbourHeight.height FROM (
		country
		JOIN
		TableCountryAndNeighbourHeight
		ON country.cid = TableCountryAndNeighbourHeight.cid
	)
;

-- ANSWER
INSERT INTO Query1 (
	SELECT Answer_1.cid AS c1id, Answer_1.cname AS c1name, country.cid AS c2id, country.cname AS c2name FROM(
		country
		JOIN
		Answer_1
		ON country.height = Answer_1.height
	)
	ORDER BY c1id ASC
);

DROP VIEW IF EXISTS TableCountrysAndHeight CASCADE;
DROP VIEW IF EXISTS TableCountryAndNeighbourHeight CASCADE;
DROP VIEW IF EXISTS Answer_1 CASCADE;



-- Query 2 statements
-- table of all countries (as cid) that are landlocked
CREATE VIEW TableLandlocked AS
	SELECT cid FROM country
	EXCEPT
	SELECT DISTINCT cid FROM oceanAccess
;

-- ANSWER
INSERT INTO Query2 (
	SELECT cid, cname FROM(
		country
		NATURAL JOIN
		TableLandlocked
	)
	ORDER BY cname ASC
);

DROP VIEW IF EXISTS TableLandlocked CASCADE;



-- Query 3 statements
-- table of all the countries (as cid) and their name (cname)
-- such that they are landlocked and have exactly one neighbour
CREATE VIEW OneNeighbourAndLandlocked AS
	-- landlocked countries from previous query: Query 2
	SELECT cid, cname FROM (
		country
		NATURAL JOIN
		(SELECT cid FROM country
		EXCEPT
		SELECT DISTINCT cid FROM oceanAccess) AS TableLandlocked
	)
	INTERSECT
	-- countries with exactly one neighbour (as cid)
	SELECT cid, cname FROM (
		(SELECT country AS cid FROM neighbour GROUP BY country HAVING COUNT(*) = 1) AS TableCountryOneNeighbour
		NATURAL JOIN
		country
	)
;

-- table OneNeighbourLandlocked with additional infromation of each country's neighbour (as c2id)
CREATE VIEW Answer_1 AS
	SELECT cid AS c1id, cname AS c1name, neighbor AS c2id FROM (
		neighbour
		JOIN
		OneNeighbourAndLandlocked
		ON cid = country)
;

-- ANSWER
INSERT INTO Query3 (
	SELECT c1id, c1name, c2id, cname AS c2name FROM (
		country
		JOIN
		Answer_1
		ON c2id = cid)
	ORDER BY c1name ASC
);

DROP VIEW IF EXISTS OneNeighbourAndLandlocked CASCADE;
DROP VIEW IF EXISTS Answer_1 CASCADE;



-- Query 4 statements
-- table of all countries (as cname) that have direct access to an ocean (as oname)
CREATE VIEW DirectAccess AS
	SELECT cname, oname FROM (
		ocean
		NATURAL JOIN
		(SELECT cname, oid FROM (
			(SELECT * FROM oceanAccess) AS TableOceanAccess
			NATURAL JOIN
			(SELECT * FROM country) AS TableCountry
		)) AS DirectAccessNoOceanName
	)
;

-- table of all countries (as cid) that have direct access to an ocean
CREATE VIEW DirectAccessCID AS
	SELECT cid FROM (
		DirectAccess
		NATURAL JOIN
		country
	)
;

-- table of all countries (as neighbor) that have indirect access through a country (as cid)
CREATE VIEW IndirectAccess_1 AS
	SELECT neighbor, cid FROM (
		DirectAccessCID
		JOIN
		neighbour
		ON country = cid
	)
;

-- table of all countries (as neighbor) and their name (cname)
-- such that they have indirect access throught a country (as cid)
CREATE VIEW IndirectAccess_2 AS
	SELECT neighbor, cname, IndirectAccess_1.cid FROM (
		IndirectAccess_1
		JOIN
		country
		ON neighbor = country.cid
	)
;

-- table of all countries (as cname) that have indirect access to an ocean (as oname)
CREATE VIEW IndirectAccess AS
	SELECT cname, oname FROM (
		(SELECT cname, oid FROM (
			IndirectAccess_2
			NATURAL JOIN
			oceanAccess
		)) AS NameAndOID
		NATURAL JOIN
		ocean
	)
;

-- ANSWER
INSERT INTO Query 4 (
	SELECT * FROM DirectAccess
	UNION
	SELECT * FROM IndirectAccess
	ORDER BY cname ASC, oname DESC
);


DROP VIEW IF EXISTS DirectAccess CASCADE;
DROP VIEW IF EXISTS DirectAccessCID CASCADE;
DROP VIEW IF EXISTS IndirectAccess_1 CASCADE;
DROP VIEW IF EXISTS IndirectAccess_2  CASCADE;
DROP VIEW IF EXISTS IndirectAccess CASCADE;



-- Query 5 statements
-- every country (as cid) and their average hdi (as avghdi)
CREATE VIEW Answer_1 AS
	SELECT cid, AVG(hdi_score) AS avghdi
	FROM hdi
	WHERE year in (2009, 2010, 2011, 2012, 2013) GROUP BY cid
	ORDER BY avghdi DESC
	-- since we only want the top 10
	LIMIT 10
;

-- ANSWER
INSERT INTO Query5 (
	SELECT cid, cname, avghdi FROM (
		Answer_1
		NATURAL JOIN
		country
	)
	ORDER BY avghdi DESC
);


DROP VIEW IF EXISTS Answer_1 CASCADE;



-- Query 6 statements
-- table of every country (as cid) and their hdi from year 2009 to 2013 (as hdi_score<year>)
CREATE VIEW CountryFiveYearHDI AS
	SELECT * FROM (
		(SELECT cid, hdi_score AS hdi_score2009 FROM hdi WHERE year = 2009) AS TableHdiYear2009
		NATURAL JOIN
		(SELECT cid, hdi_score AS hdi_score2010 FROM hdi WHERE year = 2010) AS TableHdiYear2010
		NATURAL JOIN
		(SELECT cid, hdi_score AS hdi_score2011 FROM hdi WHERE year = 2011) AS TableHdiYear2011
		NATURAL JOIN
		(SELECT cid, hdi_score AS hdi_score2012 FROM hdi WHERE year = 2012) AS TableHdiYear2012
		NATURAL JOIN
		(SELECT cid, hdi_score AS hdi_score2013 FROM hdi WHERE year = 2013) AS TableHdiYear2013
	)
;

-- table of countries with increasing hdi from 2009 to 2013 (as cid)
CREATE VIEW IncreasingHDI AS
	SELECT cid FROM CountryFiveYearHDI 
	WHERE (hdi_score2013 > hdi_score2012 AND hdi_score2012 > hdi_score2011 AND hdi_score2011> hdi_score2010 
	AND hdi_score2010 >  hdi_score2009)
;

-- ANSWER
INSERT INTO Query6 (
	SELECT cid, cname FROM (
		country
		NATURAL JOIN
		IncreasingHDI
	) ORDER BY cname ASC
);

DROP VIEW IF EXISTS CountryFiveYearHDI CASCADE;
DROP VIEW IF EXISTS IncreasingHDI CASCADE;





-- Query 7 statements
-- ANSWER
INSERT INTO Query7 (
	SELECT rid, rname, CAST(SUM((population * rpercentage)) AS INT) AS followers FROM (
		country
		NATURAL JOIN
		religion
	)
	GROUP BY rid, rname
	ORDER BY followers DESC
);



-- Query 8 statements
-- table of all country (as cid) and the percentage of their most spoken language (as lpercentage)
CREATE VIEW MostSpokenPercentage AS
	SELECT cid, max(lpercentage) AS lpercentage FROM language GROUP BY cid
;

-- table of all country (as cid) and their most popular language (as lname)
CREATE VIEW CountryMostLanguage AS
	SELECT language.cid, lname FROM (
		MostSpokenPercentage
		JOIN
		language
		ON MostSpokenPercentage.lpercentage = language.lpercentage AND MostSpokenPercentage.cid = language.cid
	)
;

-- table of all country (as cid), their name, (as cname), and their most popular language (as lname)
CREATE VIEW CountryNameMostLanguage AS
	SELECT cid, cname, lname FROM (
		CountryMostLanguage
		NATURAL JOIN
		country
	)
;

-- table of all country (as cid), their name, (as cname), their most popular language (as lname), and their neighbour (as neighbor)
CREATE VIEW CountryNameMostLanguageNeighbour AS
	SELECT cid, cname, lname, neighbor FROM (
		CountryNameMostLanguage
		JOIN
		neighbour
		ON cid = country
	)
;

-- table of all country (as cn1ame), their most popular language (as c1lname), their neighbour (as neighbor)
-- and their neighbour's most popualr language (c2lname)
CREATE VIEW Answer_1 AS
	SELECT cname AS c1name,  CountryNameMostLanguageNeighbour.lname AS c1lname, 
	neighbor, CountryMostLanguage.lname AS c2lname FROM (
		CountryMostLanguage
		JOIN 
		CountryNameMostLanguageNeighbour
		ON CountryMostLanguage.cid = CountryNameMostLanguageNeighbour.neighbor
	)
;

-- table of all country (as c1name), their most popular language (as c1lname), 
-- their neighbour (as c2name) and their neighbour's most popualr language (c2lname)
CREATE VIEW Answer_1Formatted AS
	SELECT c1name, c1lname, cname AS c2name, c2lname FROM (
		Answer_1
		JOIN
		country
		ON neighbor = cid
	)
;

-- ANSWER
INSERT INTO Query8 (
	SELECT c1name, c2name, c1lname AS lname 
	FROM Answer_1Formatted 
	WHERE c1lname = c2lname 
	ORDER BY lname ASC, c1name DESC
);

DROP VIEW IF EXISTS MostSpokenPercentage CASCADE;
DROP VIEW IF EXISTS CountryMostLanguage CASCADE;
DROP VIEW IF EXISTS CountryNameMostLanguage CASCADE;
DROP VIEW IF EXISTS CountryNameMostLanguageNeighbour CASCADE;
DROP VIEW IF EXISTS Answer_1 CASCADE;
DROP VIEW IF EXISTS Answer_1Formatted CASCADE;


-- Query 9 statements
-- table of all the landlocked countries (as cname) from Query 2 with an additional attribute totalspan (0 + height)
CREATE VIEW LandlockedSpan AS
	SELECT cname, height AS totalspan FROM (
		country
		NATURAL JOIN
		(SELECT cid FROM country
		EXCEPT
		SELECT DISTINCT cid FROM oceanAccess) AS TableLandlocked
	)
;

-- table of all non-landlocked countries (as cname) and the depth of deepest ocean (as depth)
CREATE VIEW NonLandlockedDepth AS
	SELECT cname, max(depth) AS depth FROM (
		country
		NATURAL JOIN
		oceanAccess
		NATURAL JOIN 
		ocean
	) GROUP BY cname
;

-- table of all non-landlocked countries (as cname) and their total difference (as totalspan)
CREATE VIEW NonLandlockedSpan AS
	SELECT cname, (height + depth) AS totalspan FROM (
		country
		NATURAL JOIN
		NonLandlockedDepth
	)
;

-- ASNWER
INSERT INTO Query9 (
	SELECT * FROM LandlockedSpan
	UNION
	SELECT * FROM NonLandlockedSpan
	ORDER BY totalspan DESC LIMIT 1
);

DROP VIEW IF EXISTS LandlockedSpan CASCADE;
DROP VIEW IF EXISTS NonLandlockedDepth CASCADE;
DROP VIEW IF EXISTS NonLandlockedSpan CASCADE;



—- Query 10 statements
-- table of all country (as cid) and their total border length (as borderslength)
CREATE VIEW CountryBorderLength AS
	SELECT country AS cid, SUM(length) AS borderslength  FROM neighbour GROUP BY country
;

-- table of all country (as cname) and their total border length (as borderslength)
CREATE VIEW Answer_1 AS
	SELECT cname, borderslength FROM (
		CountryBorderLength
		NATURAL JOIN
		country
	)
;

-- ANSWER
INSERT INTO Query10 (
	SELECT * FROM Answer_1 ORDER BY borderslength DESC LIMIT 1
);

DROP VIEW IF EXISTS CountryBorderLength CASCADE;
DROP VIEW IF EXISTS Answer_1 CASCADE;