-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

SET search_path TO A2;

-- Query 1 statements
CREATE VIEW noNeighbor AS
SELECT cid AS c1id, cname AS c1name, null AS c2id, null AS c2name
FROM country
WHERE cid NOT IN (SELECT country FROM neighbour);

CREATE VIEW neighborHeight AS
SELECT country, neighbor, height
FROM neighbour, country
WHERE neighbor = cid;

CREATE VIEW highest AS
SELECT country, neighbor
FROM neighborHeight nh1
WHERE nh1.height = (SELECT MAX(nh2.height)
                FROM neighborHeight nh2
                WHERE nh1.country = nh2.country);

INSERT INTO Query1 (
SELECT *
FROM ((SELECT DISTINCT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id,
      c2.cname AS c2name
      FROM highest, country c1, country c2
      WHERE c1.cid = country AND c2.cid = neighbor) UNION (SELECT * FROM noNeighbor)) allCountries
ORDER BY c1name ASC);

DROP VIEW highest;
DROP VIEW neighborHeight;
DROP VIEW noNeighbor;


-- Query 2 statements

INSERT INTO Query2 (
SELECT cid, cname
FROM country
WHERE cid NOT IN(SELECT cid
                 FROM oceanAccess)
ORDER BY cname ASC);


-- Query 3 statements

CREATE VIEW onlyOneNeighbor AS
SELECT nb.country AS c1id, MAX(c1.cname) AS c1name, MAX(nb.neighbor) AS c2id, MAX(c2.cname) AS c2name
FROM neighbour nb, country c1, country c2
WHERE nb.country = c1.cid AND nb.neighbor= c2.cid
GROUP BY nb.country
HAVING COUNT(neighbor)=1;

INSERT INTO Query3 (
SELECT c1id, c1name, c2id, c2name
FROM country, onlyOneNeighbor
WHERE cid NOT IN(SELECT cid
                 FROM oceanAccess)
      AND cid=c1id
ORDER BY c1name ASC);

DROP VIEW onlyOneNeighbor;


-- Query 4 statements

CREATE VIEW indirect AS
SELECT country AS cid, oid
FROM neighbour, oceanAccess
WHERE neighbor = cid;

INSERT INTO Query4 (
SELECT cname, oname
FROM ((SELECT * FROM oceanAccess) UNION (SELECT * FROM indirect)) accessible, country, ocean
WHERE accessible.cid = country.cid AND accessible.oid = ocean.oid
ORDER BY cname ASC, oname DESC
);

DROP VIEW indirect;


-- Query 5 statements

INSERT INTO Query5 (
SELECT hdi.cid, MAX(cname), avg(hdi_score) AS avghdi
FROM hdi, country
WHERE year >= 2009 AND year <= 2013
GROUP BY hdi.cid
ORDER BY avghdi DESC
LIMIT 10
);

-- Query 6 statements

CREATE VIEW fiveyears AS
SELECT cid, year, hdi_score
FROM hdi
WHERE year >= 2009 AND year <= 2013;

CREATE VIEW notAlwaysIncrease AS
SELECT f1.cid
FROM fiveyears f1, fiveyears f2
WHERE f1.cid = f2.cid AND f1.year > f2.year AND f1.hdi_score <= f2.hdi_score;

INSERT INTO Query6 (
SELECT cid, cname
FROM country
WHERE cid NOT IN (SELECT * FROM notAlwaysIncrease) 
      AND cid IN (SELECT cid FROM fiveyears)
ORDER BY cname ASC
);

DROP VIEW notAlwaysIncrease;
DROP VIEW fiveyears;

-- Query 7 statements

CREATE VIEW countryAndReligion AS
SELECT rid, rname, religion.cid, rpercentage*population AS followersEach
FROM religion, country
WHERE religion.cid = country.cid;

INSERT INTO Query7(
SELECT rid, MAX(rname), SUM(followersEach) AS followers
FROM countryAndReligion
GROUP BY rid
ORDER BY followers DESC
);

DROP VIEW countryAndReligion;


-- Query 8 statements

CREATE VIEW mostPopularLanguage AS
SELECT cid, lid, lname
FROM language l1
WHERE l1.lpercentage = (SELECT MAX(l2.lpercentage)
                        FROM language l2
                        WHERE l1.cid = l2.cid);

CREATE VIEW sameMostPopularLanguage AS
SELECT m1.cid AS c1id, m2.cid AS c2id, m1.lname AS lname
FROM neighbour, mostPopularLanguage m1, mostPopularLanguage m2
WHERE country = m1.cid AND neighbor = m2.cid AND m1.lid = m2.lid;

INSERT INTO Query8(
SELECT c1.cname AS c1name, c2.cname AS c2name, lname
FROM sameMostPopularLanguage, country c1, country c2
WHERE c1id = c1.cid AND c2id = c2.cid
ORDER BY lname ASC, c1name DESC
);

DROP VIEW sameMostPopularLanguage;
DROP VIEW mostPopularLanguage;


-- Query 9 statements

CREATE VIEW noOceanAccess AS
SELECT cid, cname, height, 0 AS depth
FROM country
WHERE cid NOT IN (SELECT cid
                  FROM oceanAccess);

CREATE VIEW oceanDepth AS
SELECT cid, depth
FROM ocean, oceanAccess
WHERE ocean.oid = oceanAccess.oid;

CREATE VIEW deepest AS
SELECT od1.cid AS cid, cname, height, od1.depth AS depth
FROM oceanDepth od1, country
WHERE od1.cid = country.cid AND od1.depth = (SELECT MAX(od2.depth)
                                             FROM oceanDepth od2
                                             WHERE od1.cid = od2.cid);
                                             
CREATE VIEW span AS
SELECT cname, (height + depth) AS totalspan
FROM ((SELECT * FROM noOceanAccess) UNION (SELECT * FROM deepest)) allspan;

INSERT INTO Query9(
SELECT a1.cname, a1.totalspan
FROM span a1
WHERE a1.totalspan = (SELECT MAX(a2.totalspan)
                      FROM span a2)
);

DROP VIEW span;
DROP VIEW deepest;
DROP VIEW oceanDepth;
DROP VIEW noOceanAccess;


-- Query 10 statements

CREATE VIEW totalBorderLength AS
SELECT country , SUM(length) AS totalLength
FROM neighbour
GROUP BY country;

INSERT INTO Query10(
SELECT cname, totalLength AS borderslength
FROM totalBorderLength, country
WHERE country = cid AND totalLength = (SELECT MAX(totalLength)
                                       FROM totalBorderLength)
);

DROP VIEW totalBorderLength;








