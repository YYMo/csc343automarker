-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1 (
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name, c2.height
FROM country c1, country c2, neighbour,
   (SELECT ct1id, MAX(height) AS max_height
      FROM
      (SELECT ct1.cid as ct1id, ct2.height
         FROM country ct1, country ct2, neighbour
         WHERE ct1.cid=neighbour.country AND ct2.cid=neighbour.neighbor
         ORDER BY ct1id ASC) AS country_neighbour_height

   GROUP BY ct1id) AS country_neighbours_max_height
WHERE c1.cid=ct1id AND c1.cid=neighbour.country AND c2.cid=neighbour.neighbor AND c2.height=max_height
ORDER BY c1id ASC);



-- Query 2 statements
INSERT INTO Query2 (
SELECT cid, cname FROM country WHERE cid NOT IN (SELECT cid from oceanAccess));


-- Query 3 statements

--country with only one neighbour
CREATE VIEW OneNeighbouCountry AS
   SELECT country
   FROM neighbour
   GROUP BY country
   HAVING COUNT(neighbor)=1;

CREATE VIEW OceanAccessCountryWithOneNeighbour AS
   SELECT DISTINCT cid
   FROM oceanAccess,OneNeighbouCountry
   WHERE cid=country;

CREATE VIEW CountryNeighbour AS
   SELECT c1.cid AS c1id,c1.cname AS c1name, c2.cid AS c2id,c2.cname AS c2name
   FROM neighbour,country c1, country c2
   WHERE c1.cid=neighbour.country AND c2.cid=neighbour.neighbor;

INSERT INTO Query3 (
SELECT cn.c1id, cn.c1name, cn.c2id, cn.c2name
   FROM OceanAccessCountryWithOneNeighbour oc, CountryNeighbour cn
   WHERE oc.cid=cn.c1id
   ORDER BY c1name ASC);




-- Query 4 statements

CREATE VIEW CountryNeighbour AS
   SELECT c1.cid AS c1id,c1.cname AS c1name, c2.cid AS c2id,c2.cname AS c2name
   FROM neighbour,country c1, country c2
   WHERE c1.cid=neighbour.country AND c2.cid=neighbour.neighbor;

CREATE VIEW CountryOcean AS
   SELECT country.cid AS ccid, country.cname AS ccname, ocean.oid AS ooid, ocean.oname AS ooname
   FROM country, ocean, oceanAccess
   WHERE country.cid=oceanAccess.cid AND oceanAccess.oid=ocean.oid;

CREATE VIEW CountryNeighbourOcean AS
   SELECT c1id, c1name AS cname, co1.ooname AS oname, c2id,c2name, co2.ooname AS noname
   FROM CountryNeighbour, CountryOcean co1, CountryOcean co2
   WHERE CountryNeighbour.c1id=co1.ccid AND CountryNeighbour.c2id=co2.ccid;
INSERT INTO Query4 (
SELECT cname, noname as oname FROM CountryNeighbourOcean
UNION
SELECT cname, oname AS oname FROM CountryNeighbourOcean
ORDER BY cname);

-- Query 5 statements
INSERT INTO Query5 (
SELECT hdi.cid AS cid, country.cname AS cname, AVG(hdi.hdi_score) AS avghdi
FROM hdi,country
WHERE hdi.cid=country.cid AND hdi.year>=2009 AND hdi.year<=2013
GROUP BY hdi.cid,country.cname
ORDER BY avghdi
LIMIT 10);

-- Query 6 statements
INSERT INTO Query6 (
SELECT cid, cname
FROM country
WHERE cid IN
   (SELECT hdi1.cid AS cid
   FROM country, hdi hdi1, hdi hdi2
   WHERE hdi1.cid=country.cid AND hdi2.cid=country.cid  AND hdi1.hdi_score>hdi2.hdi_score AND hdi1.year>hdi2.year AND hdi1.year>=2009 and hdi1.year<=2013 AND hdi2.year>=2009 AND hdi2.year<=2013
EXCEPT
SELECT hdi1.cid AS cid
FROM country, hdi hdi1, hdi hdi2
WHERE hdi1.cid=country.cid AND hdi2.cid=country.cid  AND hdi1.hdi_score<=hdi2.hdi_score AND hdi1.year>hdi2.year AND hdi1.year>=2009 and hdi1.year<=2013 AND hdi2.year>=2009 AND hdi2.year<=2013));


-- Query 7 statements

CREATE VIEW WorldReligionPopulation AS
   SELECT country.cname, rid, rname, rpercentage,country.population,country.population*rpercentage/100 AS rpopulation
   FROM religion, country
   WHERE religion.cid=country.cid
   ORDER BY rname;
INSERT INTO Query7 (
SELECT rid, rname, SUM(rpopulation) AS followers
FROM WorldReligionPopulation
GROUP BY rid, rname
ORDER BY followers DESC);


-- Query 8 statements

CREATE VIEW MostPopularLanguageByCID AS
   SELECT language.cid, language.lid ,language.lname,language.lpercentage
   FROM language,
      (SELECT cid,max(lpercentage) AS mostPopularLanguagePercentage
      FROM language
      GROUP BY cid
      ORDER BY cid) AS countryMostPopularLanguagePercentage
   WHERE countryMostPopularLanguagePercentage.mostPopularLanguagePercentage=language.lpercentage AND language.cid=countryMostPopularLanguagePercentage.cid;
INSERT INTO Query8 (
SELECT c1.cname AS c1name,c2.cname AS c2name, m1.lname AS lname
FROM MostPopularLanguageByCID m1,MostPopularLanguageByCID m2, country c1, country c2, neighbour
WHERE m1.cid=c1.cid AND m2.cid=c2.cid AND m1.lid=m2.lid AND neighbour.country=c1.cid AND neighbour.neighbor=c2.cid
ORDER BY m1.lname ASC, c1.cname);


-- Query 9 statements

CREATE VIEW maxDepthBycid AS
   SELECT oceanAccess.cid AS cid, MAX(depth) AS maxDepth
   FROM oceanAccess, ocean
   WHERE oceanAccess.oid=ocean.oid
   GROUP BY cid
   ORDER BY cid;

CREATE VIEW landlockedCountry AS
   SELECT cid
   FROM country
   WHERE cid NOT IN (SELECT cid FROM oceanAccess);

INSERT INTO Query9 (
SELECT country.cid, country.height+maxDepthBycid.maxDepth AS totalspan
FROM country,maxDepthBycid
WHERE country.cid=maxDepthBycid.cid
UNION
SELECT country.cid, country.height AS totalspan
FROM country,landlockedCountry
WHERE country.cid=landlockedCountry.cid
ORDER BY cid);

-- Query 10 statements

CREATE VIEW countryBorderLength AS
   SELECT country, SUM(length) AS borderslength
   FROM neighbour
   GROUP BY country
   ORDER BY country;
INSERT INTO Query10 (
SELECT cname, borderslength
FROM country, countryBorderLength,
   (SELECT MAX(borderslength) AS maxborderslength FROM countryBorderLength) AS max
WHERE country.cid=countryBorderLength.country AND countryBorderLength.borderslength=max.maxborderslength);









