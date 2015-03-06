-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW Neighbours AS
SELECT country.cid AS c1id, country.cname AS c1name, neighbour.neighbor AS neighbor
FROM country, neighbour
WHERE country.cid = neighbour.country;

CREATE VIEW Height AS
SELECT c1id, c1name, country.cid AS c2id, country.cname AS c2name, height
FROM Neighbours, country
WHERE neighbor = country.cid;

CREATE VIEW MaxHeight AS
SELECT c1id, MAX(height) AS height
FROM Height
GROUP BY c1id;

INSERT INTO Query1(
SELECT Height.c1id, c1name, c2id, Height.c2name
FROM Height, MaxHeight
WHERE Height.c1id = MaxHeight.c1id AND Height.height = MaxHeight.height
ORDER BY c1name);

DROP VIEW IF EXISTS MaxHeight CASCADE;
DROP VIEW IF EXISTS Height CASCADE;
DROP VIEW IF EXISTS Neighbours CASCADE;

-- Query 2 statements
INSERT INTO Query2(
SELECT country.cid AS cid, country.cname AS cname
FROM country
WHERE country.cid NOT IN (SELECT oceanAccess.cid FROM oceanAccess)
ORDER BY country.cname);

-- Query 3 statements
CREATE VIEW Landlocked AS
SELECT country.cid AS c1id, country.cname AS c1name
FROM country
WHERE country.cid NOT IN (SELECT oceanAccess.cid FROM oceanAccess);

CREATE VIEW OneNeighbour AS
SELECT neighbour.country AS c1id, COUNT(neighbour.neighbor)
FROM neighbour
GROUP BY neighbour.country
HAVING COUNT(neighbour.neighbor) = 1;

CREATE VIEW GetOneNeighbour AS
SELECT c1id, neighbour.neighbor AS c2id
FROM OneNeighbour, neighbour
WHERE c1id = neighbour.country;

CREATE VIEW FilterLandlocked AS
SELECT LandLocked.c1id, c1name, c2id
FROM Landlocked, GetOneNeighbour
WHERE LandLocked.c1id = GetOneNeighbour.c1id;

INSERT INTO Query3(
SELECT c1id, c1name, c2id, country.cname AS c2name
FROM FilterLandlocked, country
WHERE c2id = country.cid
ORDER BY c1name);

DROP VIEW IF EXISTS FilterLandlocked CASCADE;
DROP VIEW IF EXISTS GetOneNeighbour CASCADE;
DROP VIEW IF EXISTS OneNeighbour CASCADE;
DROP VIEW IF EXISTS Landlocked CASCADE;

-- Query 4 statements
CREATE VIEW IndirectAccess AS
SELECT neighbour.country, oceanAccess.oid
FROM neighbour, oceanAccess
WHERE neighbour.neighbor = oceanAccess.cid;

CREATE VIEW CompleteAccess AS
SELECT * 
FROM oceanAccess
UNION
SELECT *
FROM IndirectAccess;

INSERT INTO Query4(
SELECT country.cname AS cname, ocean.oname AS oname
FROM CompleteAccess, country, ocean
WHERE CompleteAccess.cid = country.cid AND CompleteAccess.oid = ocean.oid
ORDER BY cname, oname DESC);

DROP VIEW IF EXISTS CompleteAccess CASCADE;
DROP VIEW IF EXISTS IndirectAccess CASCADE;

-- Query 5 statements
CREATE VIEW Years AS
SELECT cid, AVG(hdi_score) AS avghdi
FROM hdi
WHERE year >= 2009 AND year <= 2013
GROUP BY cid
ORDER BY avghdi DESC
LIMIT 10;

INSERT INTO Query5(
SELECT Years.cid, country.cname AS cname, avghdi
FROM Years, country
WHERE Years.cid = country.cid);

DROP VIEW IF EXISTS Years CASCADE;

-- Query 6 statements
CREATE VIEW Years AS
SELECT cid, year, hdi_score
FROM hdi
WHERE year >= 2009 AND year <= 2013
ORDER BY cid, year;

CREATE VIEW Bad AS
SELECT a.cid
FROM Years a, Years b
WHERE a.cid = b.cid AND a.year > b.year AND a.hdi_score <= b.hdi_score;

INSERT INTO Query6(
SELECT DISTINCT Years.cid, country.cname AS cname
FROM Years, country
WHERE Years.cid NOT IN (SELECT cid FROM Bad) AND Years.cid = country.cid
ORDER BY cname);

DROP VIEW IF EXISTS Bad CASCADE;
DROP VIEW IF EXISTS Years CASCADE;

-- Query 7 statements
CREATE VIEW ReligionStats AS
SELECT religion.rid, (country.population * religion.rpercentage / 100) AS followers
FROM religion, country
WHERE religion.cid = country.cid;

CREATE VIEW Followers AS
SELECT ReligionStats.rid, SUM(followers) AS followers
FROM ReligionStats
GROUP BY ReligionStats.rid;

INSERT INTO Query7(
SELECT DISTINCT Followers.rid, religion.rname, Followers.followers
FROM Followers, religion
WHERE Followers.rid = religion.rid
ORDER BY followers DESC);

DROP VIEW IF EXISTS Followers CASCADE;
DROP VIEW IF EXISTS ReligionStats CASCADE;

-- Query 8 statements
CREATE VIEW LanguageStats AS
SELECT language.cid, language.lid, (country.population * language.lpercentage / 100) AS speakers
FROM language, country
WHERE language.cid = country.cid;

CREATE VIEW MostPopular AS
SELECT LanguageStats.cid, MAX(speakers) as popularLanguage
FROM LanguageStats
GROUP BY LanguageStats.cid;

CREATE VIEW MostPopularPerCountry AS
SELECT LanguageStats.cid, LanguageStats.lid
FROM LanguageStats, MostPopular
WHERE LanguageStats.cid = MostPopular.cid AND LanguageStats.speakers = MostPopular.popularLanguage;

CREATE VIEW GetData AS
SELECT neighbour.country, neighbour.neighbor, a.lid AS alid, b.lid AS blid
FROM neighbour, MostPopularPerCountry a, MostPopularPerCountry b
WHERE neighbour.country = a.cid AND neighbour.neighbor = b.cid;

CREATE VIEW ShareLanguage AS
SELECT GetData.country, GetData.neighbor, Getdata.alid
FROM GetData
WHERE GetData.alid = GetData.blid;

INSERT INTO Query8(
SELECT DISTINCT c1.cname AS c1name, c2.cname AS c2name, language.lname AS lname
FROM ShareLanguage, country c1, country c2, language
WHERE c1.cid = ShareLanguage.country AND c2.cid = ShareLanguage.neighbor AND language.lid = ShareLanguage.alid
ORDER BY lname, c1name DESC);

DROP VIEW IF EXISTS ShareLanguage CASCADE;
DROP VIEW IF EXISTS GetData CASCADE;
DROP VIEW IF EXISTS MostPopularPerCountry CASCADE;
DROP VIEW IF EXISTS MostPopular CASCADE;
DROP VIEW IF EXISTS LanguageStats CASCADE;

-- Query 9 statements
CREATE VIEW GetDepth AS
SELECT oceanAccess.cid, MAX(ocean.depth) AS depth
FROM oceanAccess, ocean
WHERE oceanAccess.oid = ocean.oid
GROUP BY oceanAccess.cid;

CREATE VIEW FillMissing AS
SELECT country.cid, 0 as depth
FROM country
WHERE country.cid NOT IN (SELECT GetDepth.cid FROM GetDepth)
UNION
SELECT *
FROM GetDepth;

INSERT INTO Query9(
SELECT country.cname AS cname, (FillMissing.depth + country.height) AS totalspan
FROM FillMissing, country
WHERE country.cid = FillMissing.cid
ORDER BY totalspan DESC
LIMIT 1);

DROP VIEW IF EXISTS FillMissing CASCADE;
DROP VIEW IF EXISTS GetDepth CASCADE;

-- Query 10 statements
CREATE VIEW TotalLength AS
SELECT neighbour.country, SUM(neighbour.length) AS borderslength
FROM neighbour
GROUP BY neighbour.country
ORDER BY borderslength DESC
LIMIT 1;

INSERT INTO Query10(
SELECT country.cname AS cname, TotalLength.borderslength AS borderslength
FROM TotalLength, country
WHERE country.cid = TotalLength.country);

DROP VIEW IF EXISTS TotalLength CASCADE;