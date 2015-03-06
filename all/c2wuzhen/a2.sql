-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW NeighborElevations (c1id, c2id, c2name, c2height) AS
SELECT country, cid, cname, height
FROM country JOIN neighbour ON cid=neighbor;

CREATE VIEW MaxNeighborElevation (c1id, c2id, c2name) AS
(SELECT c1id, c2id, c2name
FROM NeighborElevations)
EXCEPT
(SELECT n1.c1id, n1.c2id, n1.c2name
FROM NeighborElevations n1, NeighborElevations n2
WHERE n1.c1id = n2.c1id AND n1.c2height < n2.c2height);

CREATE VIEW Answer (c1id, c1name, c2id, c2name) AS
SELECT c1id, cname, c2id, c2name
FROM MaxNeighborElevation JOIN country ON cid=c1id
ORDER BY cname;

INSERT INTO Query1(SELECT * FROM Answer);
DROP VIEW IF EXISTS NeighborElevations CASCADE;
DROP VIEW IF EXISTS MaxNeighborElevation CASCADE;
DROP VIEW IF EXISTS Answer CASCADE;


-- Query 2 statements
CREATE VIEW LandlockedCid AS
(SELECT cid FROM country)
EXCEPT
(SELECT cid FROM oceanAccess);

CREATE VIEW ANSWER(cid, cname) AS
SELECT cid, cname 
FROM country NATURAL JOIN LandlockedCid
ORDER BY cname;

INSERT INTO Query2(Select * FROM Answer);
DROP VIEW IF EXISTS LandlockedCid CASCADE;
DROP VIEW IF EXISTS Answer CASCADE;


-- Query 3 statements
CREATE VIEW OneNeighborCid AS
SELECT country FROM neighbour
GROUP BY country HAVING count(neighbor)=1;

CREATE VIEW OneNeighborInfo(c1id, c2id, c2name) AS
SELECT neighbour.country, neighbor, cname
FROM country, neighbour, a2.OneNeighborCid
WHERE cid=neighbour.neighbor AND neighbour.country = OneNeighborCid.country;

CREATE VIEW LandlockedCid AS
(SELECT cid FROM country)
EXCEPT
(SELECT cid FROM oceanAccess);

CREATE VIEW OneNeighborLandlocked(c1id, c2id, c2name) AS
SELECT c1id, c2id, c2name
FROM OneNeighborInfo JOIN LandlockedCid ON c1id=cid;

CREATE VIEW Answer(c1id, c1name, c2id, c2name) AS
SELECT c1id, cname, c2id, c2name
FROM OneNeighborLandlocked JOIN country on c1id=cid
ORDER BY cname;

INSERT INTO Query3(SELECT * FROM Answer);
DROP VIEW IF EXISTS OneNeighborCid CASCADE;
DROP VIEW IF EXISTS OneNeighborInfo CASCADE;
DROP VIEW IF EXISTS LandlockedCid CASCADE;
DROP VIEW IF EXISTS OneNeighborLandlocked CASCADE;
DROP VIEW IF EXISTS Answer CASCADE;


-- Query 4 statements
CREATE VIEW IndirectAccess(cid, oid) AS
SELECT country, oid
FROM neighbour,oceanAccess
WHERE cid=neighbor;

CREATE VIEW AccessibleOceanId AS
(SELECT * FROM oceanAccess) UNION (SELECT * FROM IndirectAccess);

CREATE VIEW Answer(cname, oname) AS
SELECT cname, oname
FROM country, AccessibleOceanId, ocean
WHERE country.cid=AccessibleOceanId.cid AND AccessibleOceanId.oid = ocean.oid
ORDER BY cname ASC, oname DESC;

INSERT INTO Query4(SELECT * FROM a2.Answer);
DROP VIEW IF EXISTS IndirectAccess CASCADE;
DROP VIEW IF EXISTS AccessibleOceanId CASCADE;
DROP VIEW IF EXISTS Answer CASCADE;


-- Query 5 statements
CREATE VIEW FiveYearHDI AS
SELECT * FROM hdi WHERE year>=2009 and year<=2013;

CREATE VIEW AverageHDI(cid, avghdi) AS
SELECT cid, AVG(hdi_score)
FROM FiveYearHDI
GROUP BY cid;

CREATE VIEW Answer(cid, cname, avghdi) AS
SELECT country.cid, cname, avghdi
FROM AverageHDI JOIN country ON country.cid = AverageHDI.cid
ORDER BY avghdi DESC
LIMIT 10;

INSERT INTO Query5(SELECT * FROM Answer);
DROP VIEW IF EXISTS FiveYearHDI CASCADE;
DROP VIEW IF EXISTS AverageHDI CASCADE;
DROP VIEW IF EXISTS Answer CASCADE;


-- Query 6 statements
CREATE VIEW NineHDI AS
SELECT cid, hdi_score FROM hdi WHERE year=2009;

CREATE VIEW TenHDI AS
SELECT cid, hdi_score FROM hdi WHERE year=2010;

CREATE VIEW ElevenHDI AS
SELECT cid, hdi_score FROM hdi WHERE year=2011;

CREATE VIEW TwelveHDI AS
SELECT cid, hdi_score FROM hdi WHERE year=2012;

CREATE VIEW ThirteenHDI AS
SELECT cid, hdi_score FROM hdi WHERE year=2013;

CREATE VIEW ConstantIncreaseHDI(cid) AS
SELECT c1.cid
FROM NineHDI c1, TenHDI c2, ElevenHDI c3, TwelveHDI c4, ThirteenHDI c5
WHERE c1.cid=c2.cid AND c2.cid=c3.cid AND c3.cid=c4.cid AND c4.cid=c5.cid
AND c1.hdi_score<c2.hdi_score AND c2.hdi_score<c3.hdi_score AND
c3.hdi_score < c4.hdi_score AND c4.hdi_score < c5.hdi_score;

CREATE VIEW Answer(cid, cname) AS
SELECT country.cid, cname
FROM ConstantIncreaseHDI JOIN country ON country.cid=ConstantIncreaseHDI.cid
ORDER BY cname;

INSERT INTO Query6(Select * FROM Answer);
DROP VIEW IF EXISTS NineHDI CASCADE;
DROP VIEW IF EXISTS TenHDI CASCADE;
DROP VIEW IF EXISTS ElevenHDI CASCADE;
DROP VIEW IF EXISTS TwelveHDI CASCADE;
DROP VIEW IF EXISTS ThirteenHDI CASCADE;
DROP VIEW IF EXISTS ConstantIncreaseHDI CASCADE;
DROP VIEW IF EXISTS Answer CASCADE;


-- Query 7 statements
CREATE VIEW ReligionByCountry(cid, rid, rname, num_ppl)AS
SELECT country.cid, rid, rname, population*rpercentage/100
FROM religion JOIN country ON country.cid = religion.cid;

CREATE VIEW Answer(rid, rname, followers) AS
SELECT rid, rname, sum(num_ppl) AS totalfollowers
FROM ReligionByCountry
GROUP BY rid, rname
ORDER BY totalfollowers DESC;

INSERT INTO Query7(SELECT * FROM Answer);
DROP VIEW IF EXISTS ReligionByCountry CASCADE;
DROP VIEW IF EXISTS Answer CASCADE;


-- Query 8 statements
CREATE VIEW MaxPercentLang (cid, max_percent) AS
SELECT cid, max(lpercentage)
FROM language
GROUP BY cid;

CREATE VIEW MostPopularLang (cid, lid, lname) AS
SELECT language.cid, lid, lname
FROM language, MaxPercentLang
WHERE language.cid = MaxPercentLang.cid AND lpercentage = max_percent;

CREATE VIEW NeighborMostPopularLang (c1id, c2name, c2lid, c2lname) AS
SELECT country, cname, lid, lname
FROM country, neighbour, MostPopularLang
WHERE country.cid = neighbor AND neighbor = MostPopularLang.cid;

CREATE VIEW Answer (c1name, c2name, lname) AS
SELECT cname, c2name, lname
FROM NeighborMostPopularLang, country, MostPopularLang
WHERE country.cid=c1id AND MostPopularLang.cid=c1id AND lid=c2lid
ORDER BY lname ASC, cname DESC;

INSERT INTO Query8(SELECT * FROM Answer);
DROP VIEW IF EXISTS MaxPercentLang CASCADE;
DROP VIEW IF EXISTS MostPopularLang CASCADE;
DROP VIEW IF EXISTS NeighborMostPopularLang CASCADE;
DROP VIEW IF EXISTS Answer CASCADE;


-- Query 9 statements
CREATE VIEW LandlockedCid AS
(SELECT cid FROM country)
EXCEPT
(SELECT cid FROM oceanAccess);

CREATE VIEW LandlockedTotalSpan(cname, totalspan) AS
SELECT cname, height
FROM LandlockedCid, country
WHERE LandlockedCid.cid = country.cid;

CREATE VIEW OceanAccessTotalSpan(cname, totalspan) AS
Select cname, height+depth
FROM country, ocean, oceanAccess
WHERE country.cid = oceanAccess.cid AND oceanAccess.oid = ocean.oid;

CREATE VIEW TotalSpanAllCountries (cname, totalspan) AS
(SELECT * FROM LandlockedTotalSpan) 
UNION
(SELECT * FROM OceanAccessTotalSpan);

CREATE VIEW Answer (cname, totalspan) AS
SELECT * FROM TotalSpanAllCountries
ORDER BY totalspan DESC LIMIT 1;

INSERT INTO Query9(SELECT * FROM Answer);
DROP VIEW IF EXISTS LandlockedCid CASCADE;
DROP VIEW IF EXISTS LandlockedTotalSpan CASCADE;
DROP VIEW IF EXISTS OceanAccessTotalSpan CASCADE;
DROP VIEW IF EXISTS TotalSpanAllCountries CASCADE;
DROP VIEW IF EXISTS Answer CASCADE;


-- Query 10 statements
CREATE VIEW CountryBorderLength(country, borderlength) AS
SELECT country, sum(length)
FROM neighbour
GROUP BY country;

CREATE VIEW Answer(cname, borderlength) AS
Select cname, borderlength
FROM country JOIN CountryBorderLength ON cid = country
ORDER BY borderlength DESC
LIMIT 1;

INSERT INTO Query10(SELECT * FROM Answer);
DROP VIEW IF EXISTS CountryBorderLength CASCADE;
DROP VIEW IF EXISTS Answer CASCADE;

 

