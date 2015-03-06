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
CREATE VIEW neighHeight AS
    SELECT * FROM neighbour JOIN country
    ON neighbour.neighbor = country.cid;
CREATE VIEW highest AS
    SELECT country, max(height) FROM neighHeight
    GROUP BY country;
CREATE VIEW pairs AS
    SELECT country, neighbor FROM neighHeight JOIN highest USING(country)
    WHERE height = max;
INSERT INTO Query1
    SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name
    FROM pairs JOIN country AS c1 ON country = c1.cid
    JOIN country AS c2 ON neighbor = c2.cid
    ORDER BY c1name ASC;
DROP VIEW pairs;
DROP VIEW highest;
DROP VIEW neighHeight;

-- Query 2 statements
CREATE VIEW landlocked AS
    (SELECT cid FROM country) EXCEPT (SELECT cid FROM oceanAccess);
INSERT INTO Query2
    SELECT cid, cname FROM country JOIN landlocked USING(cid)
    ORDER BY cname ASC;
DROP VIEW landlocked;

-- Query 3 statements
CREATE VIEW oneNeigh AS
    SELECT country FROM neighbour
    GROUP BY country HAVING count(neighbor) = 1;
CREATE VIEW oneNeighLandlocked AS
    SELECT * FROM oneNeigh
    WHERE NOT EXISTS
        (SELECT * FROM oceanAccess
        WHERE oneNeigh.country = oceanAccess.cid);
INSERT INTO Query3
    SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name
    FROM oneNeighLandlocked
    JOIN country AS c1 ON oneNeighLandlocked.country = c1.cid
    JOIN neighbour ON c1.cid = neighbour.country
    JOIN country AS c2 ON neighbour.neighbor = c2.cid
    ORDER BY c1name ASC;
DROP VIEW oneNeighLandlocked;
DROP VIEW oneNeigh;

-- Query 4 statements
CREATE VIEW directAccess AS
    SELECT cid, oid FROM oceanAccess;
CREATE VIEW indirectAccess AS
    SELECT country AS cid, oid
    FROM neighbour JOIN directAccess ON neighbor = cid;
CREATE VIEW IDs AS
    (select * from directAccess) UNION (select * from indirectAccess);
INSERT INTO Query4
    SELECT cname, oname FROM IDs JOIN country USING(cid) JOIN ocean USING (oid)
    ORDER BY cname ASC, oname DESC;
DROP VIEW IDs;
DROP VIEW indirectAccess;
DROP VIEW directAccess;

-- Query 5 statements
CREATE VIEW dateRange AS
    SELECT * FROM hdi WHERE year >= 2009 AND year <= 2013;
CREATE VIEW avgHDI AS
    SELECT avg(hdi_score) AS avghdi, cid FROM dateRange GROUP BY cid
    ORDER BY avghdi DESC LIMIT 10;
INSERT INTO Query5
    SELECT cid, cname, avghdi
    FROM avgHDI JOIN country USING(cid)
    ORDER BY avghdi DESC;
DROP VIEW avgHDI;
DROP VIEW dateRange;

-- Query 6 statements
CREATE VIEW dateRange AS
    SELECT * FROM hdi WHERE year >= 2009 AND year <= 2013;
CREATE VIEW IDs AS
    SELECT d1.cid FROM dateRange AS d1, dateRange AS d2, dateRange AS d3,
        dateRange AS d4, dateRange AS d5
    WHERE (d1.year < d2.year AND d2.year < d3.year AND d3.year< d4.year
        AND d4.year < d5.year) AND (d1.cid = d2.cid AND d2.cid = d3.cid
        AND d3.cid = d4.cid AND d4.cid = d5.cid) AND (d1.hdi_score < d2.hdi_score
        AND d2.hdi_score < d3.hdi_score AND d3.hdi_score < d4.hdi_score
        AND d4.hdi_score < d5.hdi_score);
INSERT INTO Query6
    SELECT cid, cname FROM IDs JOIN country USING(cid)
    ORDER BY cname ASC;
DROP VIEW IDs;
DROP VIEW dateRange;

-- Query 7 statements
CREATE VIEW perCountry AS
    SELECT rid, rpercentage*population AS num
    FROM religion JOIN country USING(cid);
CREATE VIEW perReligion AS
    SELECT rid, sum(num) AS followers FROM perCountry GROUP BY rid;
INSERT INTO Query7
    SELECT DISTINCT rid, rname, followers
    FROM perReligion JOIN religion USING(rid)
    ORDER BY followers DESC;
DROP VIEW perReligion;
DROP VIEW perCountry;

-- Query 8 statements
CREATE VIEW maxLang AS
    SELECT cid, max(lpercentage) FROM language GROUP BY cid;
CREATE VIEW mostSpoken AS
    SELECT cid, lid, lname FROM language WHERE EXISTS
    (SELECT * FROM maxLang WHERE language.cid = maxLang.cid
        AND language.lpercentage = maxLang.max);
CREATE VIEW pairs AS
    SELECT m1.cid AS c1id, m2.cid AS c2id, m1.lname
    FROM mostSpoken AS m1, mostSpoken AS m2
    WHERE m1.cid != m2.cid AND m1.lid = m2.lid
    AND EXISTS (SELECT * FROM neighbour
        WHERE m1.cid = country AND m2.cid = neighbor);
INSERT INTO Query8
    SELECT c1.cname AS c1name, c2.cname AS c2name, lname
    FROM pairs JOIN country AS c1 ON pairs.c1id = c1.cid
    JOIN country AS c2 ON pairs.c2id = c2.cid
    ORDER BY lname ASC, c1name DESC;
DROP VIEW pairs;
DROP VIEW mostSpoken;
DROP VIEW maxLang;

-- Query 9 statements
CREATE VIEW geoInfo AS
    SELECT cname, CASE
        WHEN depth IS NULL THEN height ELSE height + depth END AS totalspan
    FROM country LEFT JOIN oceanAccess USING(cid) LEFT JOIN ocean USING(oid);
INSERT INTO Query9
    SELECT * FROM geoInfo
    WHERE totalspan IN (SELECT max(totalspan) FROM geoinfo);
DROP VIEW geoInfo;

-- Query 10 statements
CREATE VIEW borders AS
    SELECT country, sum(length) AS borderslength
    FROM neighbour GROUP BY country;
INSERT INTO Query10
    SELECT cname, borderslength
    FROM borders JOIN country ON borders.country = country.cid
    WHERE borderslength IN (SELECT max(borderslength) FROM borders);
DROP VIEW borders;
