-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

DELETE FROM Query1;
DELETE FROM Query2;
DELETE FROM Query3;
DELETE FROM Query4;
DELETE FROM Query5;
DELETE FROM Query6;
DELETE FROM Query7;
DELETE FROM Query8;
DELETE FROM Query9;
DELETE FROM Query10;

-- Query 1 statements

CREATE VIEW highestNeighbour AS (SELECT c1.cid, MAX(c2.height) max FROM neighbour, country c1, country c2 WHERE neighbour.country=c1.cid AND neighbour.neighbor=c2.cid GROUP BY c1.cid);

CREATE VIEW heighestNeighbourCID AS (SELECT c1.cid c1id, c2.cid c2id FROM neighbour, country c1, country c2 WHERE neighbour.country=c1.cid AND neighbour.neighbor=c2.cid AND EXISTS (SELECT * FROM highestneighbour WHERE highestneighbour.cid=c1.cid AND max=c2.height));

CREATE VIEW matchedNeighbours AS (SELECT c1id, c1.cname c1name, c2id, c2.cname c2name FROM heighestneighbourcid, country c1, country c2 WHERE c1id=c1.cid AND c2id=c2.cid ORDER BY c1name ASC);

INSERT INTO Query1 (SELECT * FROM matchedNeighbours);

DROP VIEW matchedNeighbours;

DROP VIEW heighestNeighbourCID;

DROP VIEW highestNeighbour;

-- Query 2 statements

CREATE VIEW landlockedcid AS (SELECT cid FROM country WHERE cid NOT IN (SELECT cid FROM oceanAccess));

CREATE VIEW landlocked AS (SELECT cid, cname FROM country WHERE cid IN (SELECT cid FROM landlockedcid) ORDER BY cname ASC);

INSERT INTO Query2 (SELECT * FROM landlocked);

DROP VIEW landlocked;

DROP VIEW landlockedcid;

-- Query 3 statements

CREATE VIEW landlockedcid AS (SELECT cid FROM country WHERE cid NOT IN (SELECT cid FROM oceanAccess));

CREATE VIEW oneNeighbourLand AS (SELECT country FROM neighbour WHERE country IN (SELECT cid country FROM landlockedcid) GROUP BY country HAVING COUNT(*)=1);

CREATE VIEW landLockSurround AS (SELECT c1.cid c1id, c1.cname c1name, c2.cid c2id, c2.cname c2name FROM country c1, country c2, neighbour WHERE c1.cid=neighbour.country AND c2.cid=neighbour.neighbor AND neighbour.country IN (SELECT country FROM oneNeighbourLand));

INSERT INTO Query3 (SELECT * FROM landLockSurround ORDER BY c1name ASC);

DROP VIEW landLockSurround;

DROP VIEW oneNeighbourLand;

DROP VIEW landlockedcid;

-- Query 4 statements

CREATE VIEW coastlineCountries AS (SELECT cid, oid FROM oceanAccess);

CREATE VIEW indirectAccess AS (SELECT country cid, oid FROM neighbour, oceanAccess WHERE country NOT IN (SELECT cid country FROM coastlineCountries) AND neighbor IN (SELECT cid neighbour FROM coastlineCountries) AND neighbor=oceanAccess.cid);

CREATE VIEW yesOcean AS ((SELECT cid, oid FROM coastlineCountries) UNION (SELECT cid, oid FROM indirectAccess));

CREATE VIEW oceanCountryList AS (SELECT cname, oname FROM country, ocean, yesOcean WHERE yesOcean.cid=country.cid AND yesOcean.oid=ocean.oid ORDER BY cname ASC, oname DESC);

INSERT INTO Query4 (SELECT * FROM oceanCountryList);

DROP VIEW oceanCountryList;

DROP VIEW yesOcean;

DROP VIEW indirectAccess;

DROP VIEW coastlineCountries;

-- Query 5 statements

CREATE VIEW rangedHDI AS (SELECT * FROM hdi WHERE year >=2009 AND year <=2013);

CREATE VIEW aggregatedHDI AS (SELECT cid, AVG(hdi_score) avghdi FROM rangedHDI GROUP BY cid);

CREATE VIEW topHDI AS (SELECT cid, avghdi FROM aggregatedHDI ORDER BY avghdi DESC LIMIT 10);

CREATE VIEW reportHDI AS (SELECT country.cid, cname, avghdi FROM topHDI, country WHERE topHDI.cid=country.cid ORDER BY avghdi DESC);

INSERT INTO Query5 (SELECT * FROM reportHDI);

DROP VIEW reportHDI;

DROP VIEW topHDI;

DROP VIEW aggregatedHDI;

DROP VIEW rangedHDI;

-- Query 6 statements

CREATE VIEW increasingHDI2010 AS (SELECT h2.cid FROM hdi h1, hdi h2 WHERE h1.cid=h2.cid AND h1.year=2009 AND h2.year=2010 AND h2.hdi_score>h1.hdi_score);

CREATE VIEW increasingHDI2011 AS (SELECT h2.cid FROM hdi h1, hdi h2 WHERE h1.cid=h2.cid AND h1.year=2010 AND h2.year=2011 AND h2.hdi_score>h1.hdi_score);

CREATE VIEW increasingHDI2012 AS (SELECT h2.cid FROM hdi h1, hdi h2 WHERE h1.cid=h2.cid AND h1.year=2011 AND h2.year=2012 AND h2.hdi_score>h1.hdi_score);

CREATE VIEW increasingHDI2013 AS (SELECT h2.cid FROM hdi h1, hdi h2 WHERE h1.cid=h2.cid AND h1.year=2012 AND h2.year=2013 AND h2.hdi_score>h1.hdi_score);

CREATE VIEW increasingHDI AS (SELECT cid FROM increasingHDI2010) INTERSECT (SELECT cid FROM increasingHDI2011) INTERSECT (SELECT cid FROM increasingHDI2012) INTERSECT (SELECT cid FROM increasingHDI2013);

CREATE VIEW increaseHDIcountries AS (SELECT cid, cname FROM country WHERE cid IN (SELECT cid FROM increasingHDI) ORDER BY cname ASC);

INSERT INTO Query6 (SELECT * FROM increaseHDIcountries);

DROP VIEW increaseHDIcountries;

DROP VIEW increasingHDI;

DROP VIEW increasingHDI2013;

DROP VIEW increasingHDI2012;

DROP VIEW increasingHDI2011;

DROP VIEW increasingHDI2010;

-- Query 7 statements

CREATE VIEW religionRaw AS (SELECT rid, rpercentage*population fol FROM religion, country WHERE religion.cid=country.cid);

CREATE VIEW religionAgg AS (SELECT rid, SUM(fol) followers FROM religionRaw GROUP BY rid);

CREATE VIEW religionDetail AS (SELECT religion.rid, rname, CAST(followers AS INT) FROM religionAgg, religion WHERE religion.rid=religionAgg.rid);

INSERT INTO Query7 (SELECT DISTINCT * FROM religionDetail ORDER BY followers DESC);

DROP VIEW religionDetail;

DROP VIEW religionAgg;

DROP VIEW religionRaw;

-- Query 8 statements

CREATE VIEW popLang AS (SELECT cid, lid FROM language l WHERE NOT EXISTS (SELECT * FROM language k WHERE k.cid=l.cid AND k.lpercentage > l.lpercentage));

CREATE VIEW neighLang AS (SELECT country c1id, neighbor c2id, l1.lid lid FROM neighbour, popLang l1, popLang l2 WHERE l1.cid=country AND l2.cid=neighbor AND l1.lid=l2.lid);

CREATE VIEW neighLangClean AS (SELECT c1id, c2id, lid FROM neighLang WHERE c1id < c2id);

CREATE VIEW samePair AS (SELECT c1.cname c1name, c2.cname c2name, lname FROM neighLangClean, country c1, country c2, language WHERE c1.cid=c1id AND c2.cid=c2id AND neighLangClean.lid=language.lid);

INSERT INTO Query8 (SELECT DISTINCT * FROM samePair ORDER BY lname ASC, c1name DESC);

DROP VIEW samePair;

DROP VIEW neighLangClean;

DROP VIEW neighLang;

DROP VIEW popLang;

-- Query 9 statements

CREATE VIEW deepestOcean AS (SELECT cid, MAX(depth) deep FROM oceanAccess, ocean WHERE oceanAccess.oid=ocean.oid GROUP BY cid);

CREATE VIEW highLow AS (SELECT country.cid, height+deep diff FROM country, deepestOcean WHERE country.cid=deepestOcean.cid);

CREATE VIEW highLand AS (SELECT cid, height diff FROM country WHERE cid NOT IN (SELECT cid FROM deepestOcean));

CREATE VIEW bigDiff AS (SELECT cid, diff FROM ((SELECT cid, diff FROM highLow) UNION (SELECT cid, diff FROM highLand)) AS raceDiff ORDER BY diff DESC LIMIT 1);

CREATE VIEW highest AS (SELECT cname, diff totalspan FROM country, bigDiff WHERE country.cid=bigDiff.cid);

INSERT INTO Query9 (SELECT * FROM highest);

DROP VIEW highest;

DROP VIEW bigDiff;

DROP VIEW highLand;

DROP VIEW highLow;

DROP VIEW deepestOcean;

-- Query 10 statements

CREATE VIEW sumBorder AS (SELECT country, SUM(length) borderslength FROM neighbour GROUP BY country);

CREATE VIEW superBorder AS (SELECT cname, borderslength FROM country, sumBorder WHERE country.cid=sumBorder.country ORDER BY borderslength DESC LIMIT 1);

INSERT INTO Query10 (SELECT * FROM superBorder);

DROP VIEW superBorder;

DROP VIEW sumBorder;
