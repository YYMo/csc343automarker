-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW HighestNeighbour AS
	SELECT country, max(height) as height
	FROM neighbour, country
	WHERE cid = neighbor
	GROUP BY country;
INSERT INTO Query1(
	SELECT c1.cid, c1.cname, c2.cid, c2.cname
	FROM HighestNeighbour, country c1, neighbour, country c2
	WHERE c1.cid = HighestNeighbour.country and c1.cid = neighbour.country and c2.cid = neighbour.neighbor and c2.height = HighestNeighbour.height
	ORDER BY c1.cname ASC
	);
DROP VIEW HighestNeighbour;

-- Query 2 statements
CREATE VIEW LandLocked AS
	(SELECT cid FROM country) EXCEPT (SELECT cid FROM oceanaccess);
INSERT INTO Query2(
	SELECT LandLocked.cid, cname
	FROM LandLocked, country
	WHERE LandLocked.cid = country.cid
	ORDER BY cname ASC
	);
DROP VIEW LandLocked;

-- Query 3 statements
CREATE VIEW MoreThanOne AS
	SELECT Query2.cid, Query2.cname
	FROM Query2, neighbour n1, neighbour n2
	WHERE Query2.cid = n1.country and Query2.cid = n2.country and n1.neighbor <> n2.neighbor;
CREATE VIEW OnlyOne AS
	(SELECT * FROM Query2) EXCEPT (SELECT * FROM MoreThanOne);
INSERT INTO Query3(
	SELECT OnlyOne.cid, OnlyOne.cname, neighbor, country.cname
	FROM OnlyOne, neighbour, country
	WHERE OnlyOne.cid = neighbour.country and neighbour.neighbor = country.cid
	ORDER BY OnlyOne.cname ASC
	);
DROP VIEW OnlyOne;
DROP VIEW MoreThanOne;

-- Query 4 statements
CREATE VIEW IndirectAccess AS
	SELECT country.cid, oceanaccess.oid
	FROM country, neighbour, oceanaccess
	WHERE country.cid = neighbour.country and neighbour.neighbor = oceanaccess.cid;
INSERT INTO Query4(
	SELECT cname, oname
	FROM ((SELECT * FROM oceanaccess) UNION (SELECT * FROM IndirectAccess)) AS accessible, country, ocean
	WHERE country.cid = accessible.cid and ocean.oid = accessible.oid
	ORDER BY cname ASC, oname DESC
	);
DROP VIEW IndirectAccess;

-- Query 5 statements
CREATE VIEW AverageHDI AS
	SELECT cid, avg(hdi_score) as avghdi
	FROM hdi
	WHERE year >= 2009 and year <=2013
	GROUP BY cid;
INSERT INTO Query5(
	SELECT country.cid, country.cname, AverageHDI.avghdi
	FROM AverageHDI, country
	WHERE AverageHDI.cid = country.cid
	ORDER BY avghdi DESC
	LIMIT 10
	);
DROP VIEW AverageHDI;

-- Query 6 statements
CREATE VIEW Increasing1 AS
	SELECT h1.cid
	FROM HDI AS h1, HDI AS h2
	WHERE h1.cid = h2.cid AND h1.year = 2009 AND h2.year = 2010 AND h1.hdi_score < h2.hdi_score;
CREATE VIEW Increasing2 AS
	SELECT h1.cid
	FROM HDI AS h1, HDI AS h2
	WHERE h1.cid = h2.cid AND h1.year = 2010 AND h2.year = 2011 AND h1.hdi_score < h2.hdi_score;
CREATE VIEW Increasing3 AS
	SELECT h1.cid
	FROM HDI AS h1, HDI AS h2
	WHERE h1.cid = h2.cid AND h1.year = 2011 AND h2.year = 2012 AND h1.hdi_score < h2.hdi_score;
CREATE VIEW Increasing4 AS
	SELECT h1.cid
	FROM HDI AS h1, HDI AS h2
	WHERE h1.cid = h2.cid AND h1.year = 2012 AND h2.year = 2013 AND h1.hdi_score < h2.hdi_score;
CREATE VIEW Increasing AS
	(SELECT * FROM Increasing1) INTERSECT
	(SELECT * FROM Increasing2) INTERSECT
	(SELECT * FROM Increasing3) INTERSECT
	(SELECT * FROM Increasing4);
INSERT INTO Query6(
	SELECT country.cid, country.cname
	FROM Increasing, country
	WHERE Increasing.cid = country.cid
	ORDER BY cname ASC
	);
DROP VIEW Increasing;
DROP VIEW Increasing1;
DROP VIEW Increasing2;
DROP VIEW Increasing3;
DROP VIEW Increasing4;

-- Query 7 statements
INSERT INTO Query7(
	SELECT rid, rname, SUM(rpercentage * population) AS followers
	FROM religion, country
	WHERE religion.cid = country.cid
	GROUP BY rid, rname
	ORDER BY followers DESC
	);

-- Query 8 statements
CREATE VIEW TopLanguage AS
	SELECT country.cid, lid, lname
	FROM country, language
	WHERE country.cid = language.cid AND language.lpercentage = (SELECT MAX(lpercentage) FROM language WHERE language.cid = country.cid);
INSERT INTO Query8(
	SELECT C1.cname, C2.cname, L1.lname
	FROM country AS C1, country AS C2, TopLanguage AS L1, TopLanguage AS L2, neighbour
	WHERE C1.cid = neighbour.country AND C2.cid = neighbour.neighbor AND
		C1.cid = L1.cid AND C2.cid = L2.cid AND L1.lid = L2.lid
	ORDER BY L1.lname ASC, C1.cname DESC
	);
DROP VIEW TopLanguage;
	
-- Query 9 statements
CREATE VIEW spans AS
	SELECT country.cid, country.cname,(height + max(depth)) AS totalspan
	FROM ocean, oceanaccess, country
	WHERE ocean.oid = oceanaccess.oid AND country.cid = oceanaccess.cid
	GROUP BY country.cid;
CREATE VIEW LandLocked AS
	(SELECT cid FROM country) EXCEPT (SELECT cid FROM oceanaccess);
CREATE VIEW spansLandlocked AS
	SELECT country.cid, country.cname, height AS totalspan
	FROM country, LandLocked
	WHERE country.cid = LandLocked.cid;	
CREATE VIEW spansAll AS 
	(select * from spans) UNION (select * from  spansLandlocked);
INSERT INTO Query9(
	SELECT cname, totalspan
	FROM spansAll
	WHERE totalspan = (SELECT MAX(totalspan) from spansAll)
	);
DROP VIEW spansAll;
DROP VIEW spansLandlocked;
DROP VIEW LandLocked;
DROP VIEW spans;

-- Query 10 statements
CREATE VIEW borderlength AS
	SELECT country.cid, country.cname, SUM(neighbour.length) AS length
	FROM country, neighbour
	WHERE country.cid = neighbour.country
	GROUP BY country.cid;
INSERT INTO Query10(
	SELECT cname, length
	FROM borderlength
	WHERE length = (SELECT MAX(length) from borderlength)
	);
DROP VIEW borderlength;