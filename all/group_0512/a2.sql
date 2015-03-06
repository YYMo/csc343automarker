-- Query 1 statements

-- Get all countries and their neighbours together.
CREATE VIEW CountryNeighbour AS 
SELECT * FROM country JOIN neighbour ON cid = neighbor;

-- Get the entire tuple where a countries neighbour's height is the maximum height
-- of all of that countries neighbour's heights.
CREATE VIEW MaxNeighbour AS
SELECT InfoCountryNeighbour.country,
InfoCountryNeighbour.neighbor, InfoCountryNeighbour.height 
FROM CountryNeighbour InfoCountryNeighbour 
INNER JOIN (SELECT country, max(height) maxheight 
                FROM CountryNeighbour 
                GROUP BY country) MaxNeighbour 
ON InfoCountryNeighbour.country = MaxNeighbour.country 
AND InfoCountryNeighbour.height = MaxNeighbour.maxheight;

-- Limit the country table to only having information about cid and name.
-- This allows us to easily look up the name of the country when
-- we have the cid.
CREATE VIEW CountryNameTable AS 
SELECT cid, cname 
FROM country;

-- Pair the two countries together, basically rename cids and don't SELECT height.
CREATE VIEW MaxNeighbourPair AS 
SELECT country AS c1id, neighbor AS c2id 
FROM MaxNeighbour;

-- Get the name of the country with c1id.
CREATE VIEW MaxNeighbourPairOneName AS 
SELECT c1id, cname as c1name, c2id 
FROM CountryNameTable JOIN MaxNeighbourPair ON c1id = cid;

-- Get the name of the country with c2id.
CREATE VIEW MaxNeighbourPairTwoNames AS 
SELECT c1id, c1name, c2id, cname as c2name 
FROM CountryNameTable JOIN MaxNeighbourPairOneName ON c2id = cid;

-- Order by c1name ASC.
CREATE VIEW Answer1 AS 
SELECT * 
FROM MaxNeighbourPairTwoNames 
ORDER BY c1name ASC;

INSERT INTO Query1 (SELECT * FROM Answer1);

-- DROP all created VIEWs.
DROP VIEW IF EXISTS Answer1 CASCADE;
DROP VIEW IF EXISTS MaxNeighbourPairTwoNames CASCADE;
DROP VIEW IF EXISTS MaxNeighbourPairOneName CASCADE;
DROP VIEW IF EXISTS CountryNameTable CASCADE;
DROP VIEW IF EXISTS MaxNeighbourPair CASCADE;
DROP VIEW IF EXISTS MaxNeighbour CASCADE;
DROP VIEW IF EXISTS CountryNeighbour CASCADE;

-- Query 2 statements

-- Get landlocked countries by selecting countries that do not
-- have access to an ocean.
CREATE VIEW LandLockedCountries AS
SELECT cid, cname 
FROM country 
WHERE cid NOT IN (SELECT cid FROM oceanAccess) ORDER BY cname ASC;

INSERT INTO Query2 (SELECT * FROM LandLockedCountries);

-- DROP all created VIEWs.
DROP VIEW IF EXISTS LandLockedCountries CASCADE;

-- Query 3 statements

-- Get all landlocked countries.
CREATE VIEW LandLockedCountries AS
SELECT cid, cname 
FROM country 
WHERE cid NOT IN (SELECT cid FROM oceanAccess) ORDER BY cname ASC;

-- Get all countries with only 1 neighbour.
CREATE VIEW CountriesWithOneNeighbour AS
SELECT country as c1id, COUNT(neighbor)
FROM neighbour
GROUP BY country
HAVING COUNT(neighbour) = 1;

-- Get that country/neighbour pair.
CREATE VIEW CountriesWithThatOneNeighbour AS
SELECT country as c1id, neighbor as c2id
FROM neighbour
WHERE country IN (SELECT c1id FROM CountriesWithOneNeighbour);

-- Get all landlocked countries with one neighbour.
CREATE VIEW LandlockedCountriesOneNeighbour AS
SELECT * FROM CountriesWithThatOneNeighbour
WHERE c1id IN (SELECT cid FROM LandLockedCountries);

-- Limit the country table to only having information about cid and name.
-- This allows us to easily look up the name of the country when
-- we have the cid.
CREATE VIEW CountryNameTable AS 
SELECT cid, cname 
FROM country;

-- Get the name of the country with c2id.
CREATE VIEW LonelyLandlockedCountryPairOneName AS 
SELECT c1id, cname AS c1name, c2id 
FROM CountryNameTable 
JOIN LandlockedCountriesOneNeighbour ON c1id = cid;

-- Get the name of the country with c2id.
CREATE VIEW LonelyLandlockedCountryPairTwoNames AS 
SELECT c1id, c1name, c2id, cname as c2name 
FROM CountryNameTable 
JOIN LonelyLandlockedCountryPairOneName ON c2id = cid;

-- Prepare for insertion.
CREATE VIEW Answer3 AS
SELECT * FROM LonelyLandlockedCountryPairTwoNames 
ORDER BY c1name ASC;

INSERT INTO Query3 (SELECT * FROM Answer3);

-- DROP all created VIEWs.
DROP VIEW IF EXISTS Answer3 CASCADE;
DROP VIEW IF EXISTS LonelyLandlockedCountryPairTwoNames CASCADE;
DROP VIEW IF EXISTS LonelyLandlockedCountryPairOneName CASCADE;
DROP VIEW IF EXISTS LandlockedCountriesOneNeighbour CASCADE;
DROP VIEW IF EXISTS CountryNameTable CASCADE;
DROP VIEW IF EXISTS CountriesWithThatOneNeighbour CASCADE;
DROP VIEW IF EXISTS CountriesWithOneNeighbour CASCADE;
DROP VIEW IF EXISTS LandLockedCountries CASCADE;
--...

-- Query 4 statements

CREATE VIEW CountriesWithOceans AS
SELECT cname, oname 
FROM country, oceanAccess, ocean 
WHERE country.cid IN (SELECT cid FROM oceanAccess) 
        AND ocean.oid = oceanAccess.oid
        AND country.cid = oceanaccess.cid;

CREATE VIEW CountriesThatBorderCountriesWithOceans AS
SELECT cname, oname 
FROM neighbour, country, oceanAccess, ocean 
WHERE neighbour.country = country.cid AND neighbor = oceanAccess.cid AND oceanAccess.oid = ocean.oid;

CREATE VIEW AllCountriesThatCanAccessOceans AS
(SELECT * FROM CountriesWithOceans) UNION
 (SELECT * FROM CountriesThatBorderCountriesWithOceans) 
 ORDER BY cname ASC, oname DESC;

INSERT INTO Query4 (SELECT * FROM AllCountriesThatCanAccessOceans);

-- DROP all created VIEWs.
DROP VIEW IF EXISTS AllCountriesThatCanAccessOceans CASCADE;
DROP VIEW IF EXISTS CountriesThatBorderCountriesWithOceans CASCADE;
DROP VIEW IF EXISTS CountriesWithOceans CASCADE;

-- Query 5 statements

CREATE VIEW CountryHDIBetween2009and2013 AS
SELECT hdi.cid, cname, year, hdi_score 
FROM hdi, country
WHERE year BETWEEN 2009 AND 2013 AND hdi.cid = country.cid;

CREATE VIEW CountryHDIAverageBetween2009and2013 AS 
SELECT cid, cname, avg(hdi_score) 
FROM CountryHDIBetween2009and2013 
GROUP BY cid, cname
ORDER BY avg(hdi_score) DESC LIMIT 10;

INSERT INTO Query5 (SELECT * FROM CountryHDIAverageBetween2009and2013);

-- DROP all created VIEWs.
DROP VIEW IF EXISTS CountryHDIAverageBetween2009and2013 CASCADE;
DROP VIEW IF EXISTS CountryHDIBetween2009and2013 CASCADE;

-- Query 6 statements

-- Get 2009 hdi scores for all countries with a 2009 score.
CREATE VIEW CountryHDI2009 AS 
SELECT cid, year, hdi_score AS hdi_score_2009 
FROM hdi 
WHERE year = 2009;

-- Get 2010 hdi scores for all countries with a 2010 score.
CREATE VIEW CountryHDI2010 AS 
SELECT cid, year, hdi_score AS hdi_score_2010 
FROM hdi 
WHERE year = 2010;

-- Get 2011 hdi scores for all countries with a 2011 score.
CREATE VIEW CountryHDI2011 AS 
SELECT cid, year, hdi_score AS hdi_score_2011 
FROM hdi 
WHERE year = 2011;

-- Get 2012 hdi scores for all countries with a 2012 score.
CREATE VIEW CountryHDI2012 AS 
SELECT cid, year, hdi_score AS hdi_score_2012 
FROM hdi 
WHERE year = 2012;

-- Get 2013 hdi scores for all countries with a 2013 score.
CREATE VIEW CountryHDI2013 AS 
SELECT cid, year, hdi_score AS hdi_score_2013 
FROM hdi 
WHERE year = 2013;

-- Append 2010 scores to 2009 scores.
CREATE VIEW CountryHDI9_10 AS 
SELECT CountryHDI2009.cid, hdi_score_2009, hdi_score_2010 
FROM CountryHDI2009 JOIN CountryHDI2010 
ON CountryHDI2009.cid = CountryHDI2010.cid;

-- Append 2011 scores to 2009/2010 scores.
CREATE VIEW CountryHDI9_10_11 AS 
SELECT CountryHDI9_10.cid, hdi_score_2009, hdi_score_2010, hdi_score_2011 
FROM CountryHDI9_10 JOIN CountryHDI2011
ON CountryHDI9_10.cid = CountryHDI2011.cid;

-- Append 2012 scores to 2009/2010/2011 scores.
CREATE VIEW CountryHDI9_10_11_12 AS 
SELECT CountryHDI9_10_11.cid, hdi_score_2009, hdi_score_2010, hdi_score_2011, hdi_score_2012 
FROM CountryHDI9_10_11 JOIN CountryHDI2012 
ON CountryHDI9_10_11.cid = CountryHDI2012.cid;

-- Append 2013 scores to 2009/2010/2011/2012 scores.
CREATE VIEW CountryHDI9_10_11_12_13 AS 
SELECT CountryHDI9_10_11_12.cid, hdi_score_2009, 
       hdi_score_2010, hdi_score_2011, 
       hdi_score_2012, 
       hdi_score_2013 
FROM CountryHDI9_10_11_12 JOIN CountryHDI2013
ON CountryHDI9_10_11_12.cid = CountryHDI2013.cid;

-- Even though both names are hideous, rename.
CREATE VIEW CountryHDI2009_2013 AS 
SELECT * 
FROM CountryHDI9_10_11_12_13;

-- Select countries whose score's are constantly increasing over the 5 year span.
CREATE VIEW CountryConstantIncrease as 
SELECT * 
FROM CountryHDI2009_2013
WHERE hdi_score_2009 < hdi_score_2010
AND   hdi_score_2010 < hdi_score_2011
AND   hdi_score_2011 < hdi_score_2012
AND   hdi_score_2012 < hdi_score_2013;

-- Prepare answer, get country name and order by correctly.
CREATE VIEW Answer6 AS
SELECT country.cid, cname 
FROM CountryConstantIncrease 
JOIN country ON CountryConstantIncrease.cid = country.cid
ORDER BY cname ASC;

INSERT INTO Query6 (SELECT * FROM Answer6);

-- DROP all created VIEWs.
DROP VIEW IF EXISTS Answer6 CASCADE;
DROP VIEW IF EXISTS CountryConstantIncrease CASCADE;
DROP VIEW IF EXISTS CountryHDI9_10_11_12_13 CASCADE;
DROP VIEW IF EXISTS CountryHDI9_10_11_12 CASCADE;
DROP VIEW IF EXISTS CountryHDI9_10_11 CASCADE;
DROP VIEW IF EXISTS CountryHDI9_10 CASCADE;
DROP VIEW IF EXISTS CountryHDI2013 CASCADE;
DROP VIEW IF EXISTS CountryHDI2012 CASCADE;
DROP VIEW IF EXISTS CountryHDI2011 CASCADE;
DROP VIEW IF EXISTS CountryHDI2010 CASCADE;
DROP VIEW IF EXISTS CountryHDI2009 CASCADE;

-- Query 7 statements

-- Get a country's cid and population.
CREATE VIEW Population AS 
SELECT cid, population 
FROM country;

-- Get a countries population divided by religion.
CREATE VIEW ReligionPopulation AS 
SELECT Religion.cid, rid, rname, rpercentage, population 
FROM Religion JOIN Population ON Religion.cid = Population.cid;

-- Get the sum of each religious population.
CREATE VIEW ReligionPopulationSum AS 
SELECT rid, rname, SUM(rpercentage * population) AS followers 
FROM Religion JOIN Population ON Religion.cid = population.cid 
GROUP BY rid, rname;

-- Prepare for insertion.
CREATE VIEW Answer7 AS
SELECT *
FROM ReligionPopulationSum
ORDER BY followers DESC;

INSERT INTO Query7 (SELECT * FROM Answer7);

-- DROP all created VIEWs.
DROP VIEW IF EXISTS Answer7 CASCADE;
DROP VIEW IF EXISTS ReligionPopulationSum CASCADE;
DROP VIEW IF EXISTS ReligionPopulation CASCADE;
DROP VIEW IF EXISTS Population CASCADE;

-- Query 8 statements

-- Get a country paired with its languages.
CREATE VIEW CountryLanguage AS 
SELECT country.cid, cname, lid, lname, lpercentage 
FROM country, language 
WHERE country.cid = language.cid;

-- Choose its most popular language.
CREATE VIEW CountryPopularLanguage AS 
SELECT cid, cname, lid, lname, lpercentage 
FROM CountryLanguage l1 
WHERE NOT EXISTS 
   (SELECT lpercentage 
        FROM CountryLanguage l2 
        WHERE l1.cname = l2.cname AND l1.lpercentage < l2.lpercentage);

-- Pair up all countries based on their languages- NOTE: Does not
-- take neighbourhood into consideration yet.
CREATE VIEW CountryLanguagePairs AS 
SELECT e1.cid AS cid1, 
       e2.cid AS cid2, 
       e1.cname AS c1name, 
       e2.cname AS c2name, e1.lname 
FROM CountryPopularLanguage e1, CountryPopularLanguage e2 
WHERE e1.lid = e2.lid AND e1.cname != e2.cname;

-- Choose those countries that are neighbo(u)rs.
CREATE VIEW NeighbourLanguagePairs AS
SELECT * 
FROM CountryLanguagePairs 
WHERE cid2 
IN (SELECT neighbor FROM neighbour where country = CountryLanguagePairs.cid1);

-- Prepare for insertion.
CREATE VIEW Answer8 AS
SELECT c1name, c2name, lname
FROM NeighbourLanguagePairs
ORDER BY lname ASC, c1name DESC;

INSERT INTO Query8 (SELECT * FROM Answer8);

-- DROP all created VIEWs.
DROP VIEW IF EXISTS Answer8 CASCADE;
DROP VIEW IF EXISTS NeighbourLanguagePairs CASCADE;
DROP VIEW IF EXISTS CountryLanguagePairs CASCADE;
DROP VIEW IF EXISTS CountryPopularLanguage CASCADE;
DROP VIEW IF EXISTS CountryLanguage CASCADE;

-- Query 9 statements

-- Get each country with the depth of its deepest ocean.
create view CountryWithMaxOceans as 
select cid, max(depth) as odepth 
from oceanAccess, ocean where oceanAccess.oid = ocean.oid group by cid;

-- Get countries with no ocean, mimic CountryWithMaxOcean, and set odepth to 0.
CREATE VIEW CountryWithNoOcean AS
SELECT cid, 0 AS odepth FROM country 
WHERE cid NOT IN (SELECT cid FROM oceanAccess);

-- Put both previous tables together.
CREATE VIEW CountryWithMaxDepth AS 
(SELECT * FROM CountryWithNoOcean) 
 UNION (SELECT * FROM CountryWithMaxOceans);

-- Get depth and height for each country.
CREATE VIEW CountryWithDepthAndHeight AS
SELECT country.cid, cname, odepth, height
FROM CountryWithMaxDepth JOIN country
ON CountryWithMaxDepth.cid = country.cid;

-- Get the span of depth and height for each country
-- NOTE: Take into consideration the countries height- a country could be
-- 'below' sea level. Perhaps if Atlantis was a country.
CREATE VIEW span AS
SELECT cname, abs(height + odepth) as totalspan
FROM CountryWithDepthAndHeight;

-- Prepare for insertion.
CREATE VIEW Answer9 AS
SELECT * 
FROM span ORDER BY totalspan DESC LIMIT 1;

INSERT INTO Query9 (SELECT * FROM Answer9);

-- DROP all created VIEWs.
DROP VIEW IF EXISTS Answer9 CASCADE;
DROP VIEW IF EXISTS span CASCADE;
DROP VIEW IF EXISTS CountryWithDepthAndHeight CASCADE;
DROP VIEW IF EXISTS CountryWithMaxDepth CASCADE;
DROP VIEW IF EXISTS CountryWithNoOcean CASCADE;
DROP VIEW IF EXISTS CountryWithMaxOceans CASCADE;

-- Query 10 statements

-- Get each country's border's sum.
CREATE VIEW CountryBorderLength AS 
SELECT country, sum(length) as borderslength
FROM neighbour GROUP BY country;

-- Get the country with the maximum border length.
CREATE VIEW CountryBorderLengthMax AS 
SELECT * 
FROM CountryBorderLength 
ORDER BY borderslength DESC LIMIT 1;

-- Get the name of the country.
CREATE VIEW MaxBorderWithName AS
SELECT country.cname, borderslength
FROM CountryBorderLengthMax JOIN country 
ON CountryBorderLengthMax.country = country.cid;

-- Prepare for insertion.
INSERT INTO Query10 (SELECT * FROM MaxBorderWithName);

-- DROP all created VIEWs.
DROP VIEW IF EXISTS MaxBorderWithName CASCADE;
DROP VIEW IF EXISTS CountryBorderLengthMax CASCADE;
DROP VIEW IF EXISTS CountryBorderLength CASCADE;


