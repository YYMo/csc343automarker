-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements --
CREATE VIEW CountryNeighbour AS
SELECT country, neighbor, height
FROM country, neighbour
WHERE country.cid = neighbour.neighbor;
 
CREATE VIEW CountryHeights AS
SELECT country, max(height) AS MaxHeight
FROM CountryNeighbour
GROUP BY country;

CREATE VIEW CountryHeightsWithName AS 
SELECT cid AS c1id, cname AS c1name, MaxHeight
FROM CountryHeights, country
WHERE CountryHeights.country = country.cid;
 
CREATE VIEW NeighbourName AS
SELECT c1id, c1name, country.cid AS c2id, country.cname AS c2name
FROM CountryHeightsWithName, country
WHERE CountryHeightsWithName.MaxHeight = country.height;

INSERT INTO Query1 (
SELECT c1id, c1name, c2id, c2name
FROM NeighbourName, neighbour
WHERE NeighbourName.c1id = neighbour.country AND c2id = neighbour.neighbor
ORDER BY c1name ASC
);

DROP VIEW NeighbourName;
DROP VIEW CountryHeightsWithName;
DROP VIEW CountryHeights;
DROP VIEW CountryNeighbour;

-- Query 2 statements --
CREATE VIEW landlocked AS 
SELECT cid 
FROM country 
EXCEPT (SELECT cid FROM oceanAccess);

INSERT INTO Query2 (cid, cname) 
SELECT country.cid, country.cname 
FROM country JOIN landlocked ON country.cid = landlocked.cid 
ORDER BY country.cname ASC;

DROP VIEW landlocked;

-- Query 3 statements --
CREATE VIEW CountryWithOcean AS
SELECT country.cid AS cid, country.cname AS cname
FROM country, oceanAccess
WHERE country.cid = oceanAccess.cid ;
 
CREATE VIEW Landlocked AS
SELECT c3.cid AS cid, c3.cname AS cname
FROM ((SELECT cid, cname FROM country AS c1) EXCEPT (SELECT cid, cname FROM CountryWithOcean AS c2)) AS c3;
 
CREATE VIEW OnlyOneNeighbour AS 
SELECT country 
FROM neighbour
GROUP BY country
HAVING COUNT(neighbour.neighbor) = 1;

CREATE VIEW NeighbourCid AS 
SELECT neighbour.country AS cid, neighbour.neighbor AS neighbor
FROM OnlyOneNeighbour, neighbour
WHERE OnlyOneNeighbour.country = neighbour.country;

CREATE VIEW NeighbourName AS 
SELECT NeighbourCid.cid AS cid, country.cid AS nCid, country.cname AS nName
FROM country, NeighbourCid
WHERE NeighbourCid.neighbor = country.cid;

INSERT INTO Query3(
SELECT Landlocked.cid AS c1id, Landlocked.cname AS c1name, NeighbourName.nCid AS c2id, NeighbourName.nName AS c2name
FROM Landlocked, NeighbourName
WHERE Landlocked.cid = NeighbourName.cid
ORDER BY c1name ASC
);

DROP VIEW NeighbourName;
DROP VIEW NeighbourCid;
DROP VIEW OnlyOneNeighbour;
DROP VIEW Landlocked;
DROP VIEW CountryWithOcean;

-- Query 4 statements --
CREATE VIEW directAccess AS 
SELECT country.cname, ocean.oname 
FROM country JOIN oceanAccess ON country.cid = oceanAccess.cid JOIN ocean ON oceanAccess.oid = ocean.oid;

CREATE VIEW indirectAccess AS 
SELECT country.cname, ocean.oname 
FROM oceanAccess JOIN neighbour ON oceanAccess.cid = neighbour.country JOIN country ON neighbour.neighbor = country.cid JOIN ocean ON oceanAccess.oid = ocean.oid ;

INSERT INTO Query4 (cname, oname) 
SELECT cname, oname 
FROM directAccess UNION (SELECT cname, oname 
						 FROM indirectAccess 
						 ORDER BY cname ASC, oname DESC);

DROP VIEW indirectAccess;
DROP VIEW directAccess;

-- Query 5 statements --
INSERT INTO Query5 (cid, cname, avghdi) 
SELECT country.cid, country.cname, AVG(hdi_score) AS avghdi 
FROM hdi JOIN country ON hdi.cid = country.cid 
WHERE year BETWEEN 2009 AND 2013 
GROUP BY country.cid 
ORDER BY avghdi DESC 
LIMIT 10;

-- Query 6 statements --
CREATE VIEW hdi2009 AS SELECT cid, hdi_score, year FROM hdi WHERE year = 2009;
CREATE VIEW hdi2010 AS SELECT cid, hdi_score, year FROM hdi WHERE year = 2010;
CREATE VIEW hdi2011 AS SELECT cid, hdi_score, year FROM hdi WHERE year = 2011;
CREATE VIEW hdi2012 AS SELECT cid, hdi_score, year FROM hdi WHERE year = 2012;
CREATE VIEW hdi2013 AS SELECT cid, hdi_score, year FROM hdi WHERE year = 2013;

CREATE VIEW all_Queries AS 
SELECT hdi2009.cid AS cid, hdi2009.year as q2009, hdi2010.year AS q2010, hdi2011.year AS q2011, hdi2012.year AS q2012, hdi2013.year AS q2013 
FROM hdi2009 JOIN hdi2010 ON hdi2009.cid = hdi2010.cid JOIN hdi2011 ON hdi2009.cid = hdi2011.cid JOIN hdi2012 ON hdi2009.cid = hdi2012.cid JOIN hdi2013 ON hdi2009.cid = hdi2013.cid;

INSERT INTO Query6 (cid, cname) 
SELECT country.cid, country.cname 
FROM country JOIN all_Queries ON country.cid = all_Queries.cid 
WHERE (all_Queries.q2009 < all_Queries.q2010) AND 
       (all_Queries.q2010 < all_Queries.q2011) AND
       (all_Queries.q2011 < all_Queries.q2012) AND 
       (all_Queries.q2012< all_Queries.q2013);

DROP VIEW all_Queries;
DROP VIEW hdi2013;
DROP VIEW hdi2012;
DROP VIEW hdi2011;
DROP VIEW hdi2010;
DROP VIEW hdi2009;

-- Query 7 statements--
CREATE VIEW ReligionTotal AS 
SELECT religion.rid AS rid, SUM(country.population * religion.rpercentage / 100) AS followers
FROM country JOIN religion ON country.cid = religion.cid
GROUP BY religion.rid;

INSERT INTO Query7(
SELECT distinct(religion.rid), religion.rname, followers
FROM religion, ReligionTotal
WHERE religion.rid = ReligionTotal.rid
ORDER BY followers DESC
);

DROP VIEW ReligionTotal;

-- Query 8 statements --
CREATE VIEW CountryLanguage AS
SELECT country.cid AS cid, language.lname AS lname, language.lpercentage AS lpercentage
FROM country, language
WHERE country.cid = language.cid;

CREATE VIEW GroupLanguage AS
SELECT cid, max(lpercentage) AS MaxPercentage
FROM CountryLanguage
GROUP BY cid;

CREATE VIEW MaxLanguage AS
SELECT GroupLanguage.cid AS cid, language.lname AS lname 
FROM GroupLanguage, language
WHERE GroupLanguage.MaxPercentage = language.lpercentage AND GroupLanguage.cid = language.cid;

CREATE VIEW languageNeighbour1 AS 
SELECT MaxLanguage.cid AS cid, MaxLanguage.lname AS lnameCountry, neighbour.neighbor AS neighbor
FROM MaxLanguage, neighbour
WHERE MaxLanguage.cid = neighbour.country;

CREATE VIEW languageNeighbour2 AS 
SELECT languageNeighbour1.cid AS cid, languageNeighbour1.lnameCountry AS lnameCountry, languageNeighbour1.neighbor AS neighbor, MaxLanguage.lname AS lnameNeighbour
FROM languageNeighbour1, MaxLanguage
WHERE languageNeighbour1.neighbor = MaxLanguage.cid;

CREATE VIEW sameLanguage AS
SELECT languageNeighbour2.cid AS cid, languageNeighbour2.lnameCountry AS lnameCountry, languageNeighbour2.neighbor AS cid2
FROM languageNeighbour2
WHERE lnameCountry = lnameNeighbour;

CREATE VIEW getCname AS 
SELECT country.cname AS c1name, sameLanguage.lnameCountry AS lname, sameLanguage.cid2 AS cid
FROM sameLanguage, country
WHERE sameLanguage.cid = country.cid;

CREATE VIEW getCname2 AS 
SELECT country.cname AS c2name, getCname.lname AS lname, getCname.c1name AS c1name
FROM getCname, country 
WHERE getCname.cid = country.cid;

INSERT INTO Query8(
SELECT c1name, c2name, lname 
FROM getCname2
);

DROP VIEW getCname2;
DROP VIEW getCname;
DROP VIEW sameLanguage;
DROP VIEW languageNeighbour2;
DROP VIEW languageNeighbour1;
DROP VIEW MaxLanguage;
DROP VIEW GroupLanguage;
DROP VIEW CountryLanguage;

-- Query 9 statements --
CREATE VIEW oceanDepth AS 
SELECT oceanAccess.cid AS cid, ocean.depth AS depth 
FROM oceanAccess, ocean
WHERE oceanAccess.oid = ocean.oid;

CREATE VIEW countryOcean AS
SELECT country.cname AS cname, (country.height + oceanDepth.depth) AS depth
FROM oceanDepth, country
WHERE oceanDepth.cid = country.cid;

CREATE VIEW maxDepth AS 
SELECT cname, max(depth) AS totalspan 
FROM countryOcean 
GROUP BY cname;

INSERT INTO Query9(
SELECT cname, totalspan 
FROM maxDepth
ORDER BY totalspan DESC 
LIMIT 1 
);

DROP VIEW maxDepth;
DROP VIEW countryOcean;
DROP VIEW oceanDepth;


-- Query 10 statements --
CREATE VIEW totalBorder AS 
SELECT country, SUM(length) AS borderslength
FROM neighbour
GROUP BY country 
ORDER BY borderslength DESC
LIMIT 1;

INSERT INTO Query10(
SELECT country.cname AS cname, totalBorder.borderslength AS borderslength
FROM totalBorder, country
WHERE country.cid = totalBorder.country
);

DROP VIEW totalBorder;