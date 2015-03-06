-- Query 1 statements
CREATE VIEW neighbouringCountriesWithElevation AS
  SELECT n.country AS c1id,
         c1.height AS height1,
         n.neighbor AS c2id,
         c2.height AS height2
         FROM neighbour n, country c1, country c2
         WHERE n.country = c1.cid AND n.neighbor = c2.cid;
 
CREATE VIEW neighbouringCountryIdsWithHighestElevation AS
  SELECT x.c1id, c2id FROM
    (SELECT c1id, MAX(height2) AS maxHeight
                               FROM neighbouringCountriesWithElevation
                               GROUP BY c1id) AS x,
    neighbouringCountriesWithElevation AS n
    WHERE x.c1id = n.c1id AND x.maxHeight = n.height2;
 
CREATE VIEW Result AS
  SELECT c1id, c1.cname AS c1name, c2id, c2.cname AS c2name FROM
    neighbouringCountryIdsWithHighestElevation n, country c1, country c2
    WHERE n.c1id = c1.cid AND n.c2id = c2.cid;
 
INSERT INTO Query1 (SELECT * FROM Result);
 
DROP VIEW Result CASCADE;
DROP VIEW neighbouringCountryIdsWithHighestElevation CASCADE;
DROP VIEW neighbouringCountriesWithElevation CASCADE;


-- Query 2 statements
CREATE VIEW AllCIDS AS
SELECT cid
FROM country;

CREATE VIEW NotLandlockedCIDS AS
SELECT DISTINCT cid
FROM oceanAccess;

CREATE VIEW LandlockedCIDS AS
SELECT * FROM AllCIDS
EXCEPT
SELECT * FROM NotLandlockedCIDS;

CREATE VIEW Result2 AS
SELECT country.cid, country.cname
FROM LandlockedCIDS NATURAL JOIN country
ORDER BY country.cname ASC;

INSERT INTO Query2 (SELECT * FROM Result2);

DROP VIEW Result2 CASCADE;

-- Query 3 statements
CREATE VIEW landlockedCountries AS
  SELECT cid FROM Query2;
 
CREATE VIEW neighbouringLandLockedCountries AS
  SELECT n.country AS c1id,
         n.neighbor AS c2id
         FROM neighbour n, country c1, country c2
         WHERE n.country = c1.cid AND n.neighbor = c2.cid;
 
         
CREATE VIEW landlockedCountriesSurroundedByOneCountry AS
  SELECT c1id, MAX(c2id) AS c2id FROM neighbouringLandLockedCountries
        GROUP BY c1id HAVING COUNT(c2id) = 1 ORDER BY c1id;
 
CREATE VIEW result3 AS
  SELECT c1id, c1.cname AS c1name, c2id, c2.cname AS c2name
        FROM landlockedCountriesSurroundedByOneCountry l, country c1, country c2
        WHERE c1id = c1.cid AND c2id = c2.cid;
 
INSERT INTO Query3 (SELECT * FROM result3);
 
DROP VIEW result3 CASCADE;
DROP VIEW landlockedCountriesSurroundedByOneCountry CASCADE;
DROP VIEW neighbouringLandLockedCountries CASCADE;
DROP VIEW landlockedCountries CASCADE;


-- Query 4 statements
CREATE VIEW CIDToOcean AS
SELECT cid, oname
FROM ocean INNER JOIN oceanAccess
ON ocean.oid = oceanAccess.oid;

CREATE VIEW NeighbourToOcean AS
SELECT country, oname
FROM neighbour INNER JOIN CIDToOcean
ON neighbour.neighbor = CIDToOcean.cid;

CREATE VIEW AllOceanAccess AS
SELECT *
FROM NeighbourToOcean
        UNION
SELECT *
FROM CIDToOcean;

CREATE VIEW Result4 AS
SELECT cname, oname
FROM country INNER JOIN AllOceanAccess
ON country.cid = AllOceanAccess.country
ORDER BY cname ASC, oname ASC;

INSERT INTO Query4 (SELECT * FROM Result4);

DROP VIEW Result4 CASCADE;

-- Query 5 statements
CREATE VIEW HDIForTimePeriod AS
  SELECT cid, hdi_score, year FROM
    hdi WHERE year <= 2013 AND year >= 2009;
 
CREATE VIEW Top10HighestHDI AS
  SELECT cid, AVG(hdi_score) AS avghdi FROM
    HDIForTimePeriod GROUP BY cid
    ORDER BY AVG(hdi_score) DESC LIMIT 10;
 
 
CREATE VIEW result5 AS
  SELECT t.cid, cname, avghdi FROM
    Top10HighestHDI t, country c WHERE
    c.cid = t.cid;
 
INSERT INTO Query5 (SELECT * FROM result5);
 
DROP VIEW result5 CASCADE;
DROP VIEW Top10HighestHDI CASCADE;
DROP VIEW HDIForTimePeriod CASCADE;


-- Query 6 statements
CREATE VIEW H2009 AS
SELECT cid, year, hdi_score
FROM hdi
WHERE year = 2009;

CREATE VIEW H2010 AS
SELECT cid, year, hdi_score
FROM hdi
WHERE year = 2010;

CREATE VIEW H2011 AS
SELECT cid, year, hdi_score
FROM hdi
WHERE year = 2011;

CREATE VIEW H2012 AS
SELECT cid, year, hdi_score
FROM hdi
WHERE year = 2012;

CREATE VIEW H2013 AS
SELECT cid, year, hdi_score
FROM hdi
WHERE year = 2013;

CREATE VIEW H2014 AS
SELECT cid, year, hdi_score
FROM hdi
WHERE year = 2014;

CREATE VIEW H2009to2010 AS
SELECT H2009.cid AS cid, H2010.hdi_score AS two
FROM H2009 INNER JOIN H2010
ON H2009.cid = H2010.cid
WHERE H2009.hdi_score < H2010.hdi_score;

CREATE VIEW H2009to2011 AS
SELECT H2009to2010.cid AS cid, H2011.hdi_score AS three
FROM H2009to2010 INNER JOIN H2011
ON H2009to2010.cid = H2011.cid
WHERE H2009to2010.two < H2011.hdi_score;

CREATE VIEW H2009to2012 AS
SELECT H2009to2011.cid AS cid, H2012.hdi_score AS four
FROM H2009to2011 INNER JOIN H2012
ON H2009to2011.cid = H2012.cid
WHERE H2009to2011.three < H2012.hdi_score;

CREATE VIEW H2009to2013 AS
SELECT H2009to2012.cid AS cid, H2013.hdi_score AS five
FROM H2009to2012 INNER JOIN H2013
ON H2009to2012.cid = H2013.cid
WHERE H2009to2012.four < H2013.hdi_score;

CREATE VIEW Result6 AS
SELECT country.cid AS cid, country.cname AS cname
FROM H2009to2013 INNER JOIN country
ON H2009to2013.cid = country.cid
ORDER BY country.cname ASC;

INSERT INTO Query6 (SELECT * FROM Result6);

DROP VIEW Result6 CASCADE;

-- Query 7 statements
CREATE VIEW CountryReligions AS
  SELECT rid, rpercentage, population
    FROM country c, religion l
    WHERE c.cid = l.cid;
 
CREATE VIEW almostResult7 AS
  SELECT rid, sum((rpercentage / 100.0) * population) AS followers FROM
    CountryReligions GROUP BY rid;
 
CREATE VIEW result7 AS
  SELECT DISTINCT a.rid, rname, followers
    FROM almostResult7 a, religion r
    WHERE a.rid = r.rid;
 
INSERT INTO Query7 (SELECT * FROM result7);
 
DROP VIEW result7 CASCADE;
DROP VIEW almostResult7 CASCADE;
DROP VIEW CountryReligions CASCADE;


-- Query 8 statements
CREATE VIEW MostPopularLanguage AS
SELECT l2.cid, l2.lname
FROM (SELECT cid, MAX(lpercentage) AS maxlang
          FROM language GROUP BY cid) AS l1 INNER JOIN language AS l2
          ON l1.cid = l2.cid AND
          l1.maxlang = l2.lpercentage;

CREATE VIEW PairsOfPopular AS
SELECT l1.cid AS cid1, l2.cid AS cid2, l1.lname
FROM MostPopularLanguage AS l1, MostPopularLanguage AS l2
WHERE l1.lname = l2.lname AND l1.cid != l2.cid;

CREATE VIEW PopularNeighbours AS
SELECT cid1, cid2, lname
FROM PairsOfPopular, neighbour
WHERE (cid1 = country AND cid2 = neighbor);

CREATE VIEW DistinctCountryPairs AS
SELECT c1.cid AS cid1, c2.cid AS cid2, 
                c1.cname AS c1name, c2.cname AS c2name
FROM Country AS c1 INNER JOIN Country AS c2
ON (c1.cid != c2.cid);

CREATE VIEW Result8 AS
SELECT c1name, c2name, lname
FROM PopularNeighbours AS p INNER JOIN DistinctCountryPairs AS c
ON (c.cid1 = p.cid1 AND c.cid2 = p.cid2) OR
        (c.cid1 = p.cid2 AND c.cid2 = p.cid1);

INSERT INTO Query8 (SELECT * FROM Result8);

DROP VIEW Result8 CASCADE;

-- Query 9 statements
CREATE VIEW CountriesWithoutDirectAccessToAnOcean AS
  SELECT cid FROM country EXCEPT SELECT cid FROM oceanAccess;
 
CREATE VIEW CountryHeightsNoOcean AS
  SELECT c.cid, height AS totalspan FROM
    country c, CountriesWithoutDirectAccessToAnOcean o
    WHERE c.cid = o.cid;
 
CREATE VIEW CountryWithOceanDepth AS
  SELECT cid, oa.oid, depth FROM
    oceanAccess oa, ocean oc
    WHERE oa.oid = oc.oid;
 
CREATE VIEW CountryWithMaxOceanDepth AS
  SELECT cid, max(depth) AS maxdepth FROM CountryWithOceanDepth
    GROUP BY cid;
 
CREATE VIEW CountryHeightsWithOcean AS
  SELECT c.cid, maxdepth + height AS totalspan FROM
    CountryWithMaxOceanDepth co, country c
    WHERE co.cid = c.cid;
 
CREATE VIEW CountriesWithSpans AS
  SELECT cid, totalspan FROM CountryHeightsNoOcean
    UNION
  SELECT cid, totalspan FROM CountryHeightsWithOcean;
 
CREATE VIEW result9 AS
  SELECT c.cid, totalspan
  FROM (SELECT cid, MAX(totalspan) AS maxtotalspan
        FROM CountriesWithSpans
        GROUP BY cid
        ORDER BY maxtotalspan DESC
        LIMIT 1) AS x
    INNER JOIN CountriesWithSpans c
    ON x.cid = c.cid AND x.maxtotalspan = c.totalspan;
 
INSERT INTO Query9 (SELECT * FROM result9);
 
DROP VIEW result9 CASCADE;
DROP VIEW CountriesWithSpans CASCADE;
DROP VIEW CountryHeightsWithOcean CASCADE;
DROP VIEW CountryWithMaxOceanDepth CASCADE;
DROP VIEW CountryWithOceanDepth CASCADE;
DROP VIEW CountryHeightsNoOcean CASCADE;
DROP VIEW CountriesWithoutDirectAccessToAnOcean;
 


-- Query 10 statements
CREATE VIEW LengthSums AS
SELECT country, SUM(length) l
FROM neighbour
GROUP BY country;

CREATE VIEW MaxLength AS
SELECT country, l
FROM LengthSums
WHERE l = (
                        SELECT MAX(l)
                        FROM LengthSums
                        );

CREATE VIEW Result10 AS
SELECT country.cname, MaxLength.l
FROM country INNER JOIN MaxLength
ON country.cid = MaxLength.country;

INSERT INTO Query10 (SELECT * FROM Result10);

DROP VIEW Result10 CASCADE;