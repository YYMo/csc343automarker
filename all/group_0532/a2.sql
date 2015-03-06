-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

-- The country table contains all the countries in the world and their facts.
-- 'cid' is the id of the country.
-- 'name' is the name of the country.
-- 'height' is the highest elevation point of the country.
-- 'population' is the population of the country.
    
-- The neighbour table provides information about the countries and their neighbours.
-- 'country' refers to the cid of the first country.
-- 'neighbor' refers to the cid of a country that is neighbouring the first country.
-- 'length' is the length of the border between the two neighbouring countries.
-- Note that if A and B are neighbours, then there two tuples are stored in the table to represent that information (A, B) and (B, A). 

-- Similar to query 8
-- find maximal neighbouring height
CREATE VIEW maxHeight AS
SELECT
    c1.cid AS c1id,
    c1.cname AS c1name,
    MAX(c2.height) AS height
FROM country AS c1
INNER JOIN neighbour
    ON neighbour.country = c1.cid
INNER JOIN country AS c2
    ON c2.cid = neighbour.neighbor
GROUP BY c1.cid, c1.cname;

-- join country with neighbour(s) that has maximal height
INSERT INTO Query1 (
    SELECT
        maxHeight.c1id,
        maxHeight.c1name,
        country.cid AS c2id,
        country.cname AS c2name
    FROM maxHeight
    INNER JOIN neighbour
        ON neighbour.country = maxHeight.c1id
    INNER JOIN country
        ON country.cid = neighbour.neighbor
        AND country.height = maxHeight.height
    ORDER BY maxHeight.c1name ASC);

DROP VIEW maxHeight;

-- Query 2 statements

-- The country table contains all the countries in the world and their facts.
-- 'cid' is the id of the country.
-- 'name' is the name of the country.
-- 'height' is the highest elevation point of the country.
-- 'population' is the population of the country.

-- The oceanAccess table provides information about the countries which have a border with an ocean.
-- 'cid' refers to the cid of the country.
-- 'oid' refers to the oid of the ocean.

-- Shared by question 3
-- cids that have access to oceans, not land locked
CREATE VIEW notLandlocked AS
SELECT DISTINCT country.cid
FROM country
INNER JOIN oceanAccess
    ON oceanAccess.cid = country.cid;

--Shared by question 3
INSERT INTO Query2 (
    SELECT country.cid, country.cname AS cname
    FROM country 
    WHERE country.cid NOT IN (
        SELECT cid
        FROM notLandlocked)
    ORDER BY cname ASC);

DROP VIEW notLandlocked;

-- Query 3 statements

-- The country table contains all the countries in the world and their facts.
-- 'cid' is the id of the country.
-- 'name' is the name of the country.
-- 'height' is the highest elevation point of the country.
-- 'population' is the population of the country.

-- The oceanAccess table provides information about the countries which have a border with an ocean.
-- 'cid' refers to the cid of the country.
-- 'oid' refers to the oid of the ocean.

-- The neighbour table provides information about the countries and their neighbours.
-- 'country' refers to the cid of the first country.
-- 'neighbor' refers to the cid of a country that is neighbouring the first country.
-- 'length' is the length of the border between the two neighbouring countries.
-- Note that if A and B are neighbours, then there two tuples are stored in the table to represent that information (A, B) and (B, A). 

-- Shared by question 2
-- cids that have access to oceans, not land locked
CREATE VIEW notLandlocked AS
SELECT DISTINCT country.cid
FROM country
INNER JOIN oceanAccess
    ON oceanAccess.cid = country.cid;

-- Shared by question 2
-- cids that are not not land locked
CREATE VIEW landlocked AS
SELECT country.cid, country.cname AS cname
FROM country 
WHERE country.cid NOT IN (
    SELECT cid
    FROM notLandlocked);

-- landlocked cids with exactly one neighbour
CREATE VIEW landlockedByOne AS
SELECT landlocked.cid, landlocked.cname
FROM landlocked
INNER JOIN neighbour
    ON neighbour.country = landlocked.cid
GROUP BY landlocked.cid, landlocked.cname
HAVING COUNT(neighbour.neighbor) = 1;

-- joins for neighbour and names
INSERT INTO Query3 (
    SELECT
        landlockedByOne.cid AS c1id,
        landlockedByOne.cname AS c1name,
        country.cid AS c2id,
        country.cname AS c2name
    FROM landlockedByOne
    INNER JOIN neighbour
        ON neighbour.country = landlockedByOne.cid
    INNER JOIN country
        ON country.cid = neighbour.neighbor
    ORDER BY c1name ASC);

DROP VIEW notLandlocked, landlocked, landlockedByOne;

-- Query 4 statements

-- The country table contains all the countries in the world and their facts.
-- 'cid' is the id of the country.
-- 'name' is the name of the country.
-- 'height' is the highest elevation point of the country.
-- 'population' is the population of the country.

-- The neighbour table provides information about the countries and their neighbours.
-- 'country' refers to the cid of the first country.
-- 'neighbor' refers to the cid of a country that is neighbouring the first country.
-- 'length' is the length of the border between the two neighbouring countries.
-- Note that if A and B are neighbours, then there two tuples are stored in the table to represent that information (A, B) and (B, A). 

-- The oceanAccess table provides information about the countries which have a border with an ocean.
-- 'cid' refers to the cid of the country.
-- 'oid' refers to the oid of the ocean.

-- The ocean table contains information about oceans on the earth.
-- 'oid' is the id of the ocean.
-- 'oname' is the name of the ocean.
-- 'depth' is the depth of the deepest part of the ocean

-- cname, onames which cname has direct access to
CREATE VIEW directAccess AS
SELECT country.cname AS cname, ocean.oname
FROM country
INNER JOIN oceanAccess
    ON oceanAccess.cid = country.cid
INNER JOIN ocean
    ON ocean.oid = oceanAccess.oid;

-- cname, onames which cname has indirect access to
CREATE VIEW indirectAccess AS
SELECT country.cname AS cname, ocean.oname
FROM country
INNER JOIN neighbour
    ON neighbour.country = country.cid
INNER JOIN oceanAccess
    ON oceanAccess.cid = neighbour.neighbor
INNER JOIN ocean
    ON ocean.oid = oceanAccess.oid
GROUP BY country.cname, ocean.oname;  -- Remove duplicates

-- union all forms of ocean access
INSERT INTO Query4 (
    SELECT q.cname, q.oname
    FROM (
        SELECT cname, oname FROM directAccess
        UNION  -- Remove duplicates
        SELECT cname, oname FROM indirectAccess
        ) AS q
    ORDER BY q.cname ASC, q.oname DESC);

DROP VIEW directAccess, indirectAccess;
    
-- Query 5 statements

-- The country table contains all the countries in the world and their facts.
-- 'cid' is the id of the country.
-- 'name' is the name of the country.
-- 'height' is the highest elevation point of the country.
-- 'population' is the population of the country.

-- The hdi table contains the human development index of each country per year. (http://en.wikipedia.org/wiki/Human_Development_Index)
-- 'cid' is the id of the country.
-- 'year' is the year when the hdi score has been estimated.
-- 'hdi_score' is the Human Development Index score of the country that year. It takes values [0, 1] with a larger number representing a higher HDI.

-- Shared by question 6
-- country and hdi joined
CREATE VIEW countryHDI AS 
SELECT country.cid, country.cname AS cname, hdi.hdi_score, hdi.year
FROM country
INNER JOIN hdi
    ON hdi.cid = country.cid
WHERE hdi.year >= 2009 AND hdi.year <= 2013;

-- average hdi, top 10
INSERT INTO Query5 (
    SELECT cid, cname, AVG(hdi_score) AS avghdi
    FROM countryHDI
    GROUP BY cid, cname
    ORDER BY avghdi DESC
    LIMIT 10);

DROP VIEW countryHDI;

-- Query 6 statements

-- Shared by question 5
-- country and hdi joined
CREATE VIEW countryHDI AS 
SELECT country.cid, country.cname AS cname, hdi.hdi_score, hdi.year
FROM country
INNER JOIN hdi
    ON hdi.cid = country.cid
WHERE hdi.year >= 2009 AND hdi.year <= 2013;

-- cid of countries with at least one year of non-increasing hdi
CREATE VIEW nonIncreasingHDI AS
SELECT DISTINCT hdi1.cid
FROM countryHDI AS hdi1
INNER JOIN countryHDI AS hdi2
    ON hdi2.cid = hdi1.cid
    AND hdi2.year > hdi1.year
    AND hdi2.hdi_score <= hdi1.hdi_score;

-- all countries that don't have a non-increasing hdi year
INSERT INTO Query6 (
    SELECT cid, cname
    FROM countryHDI
    WHERE countryHDI.cid NOT IN (
        SELECT cid
        FROM nonIncreasingHDI)
    GROUP BY cid, cname  -- remove duplicates
    ORDER BY cname ASC);

DROP VIEW countryHDI, nonIncreasingHDI;

-- Query 7 statements

-- The country table contains all the countries in the world and their facts.
-- 'cid' is the id of the country.
-- 'name' is the name of the country.
-- 'height' is the highest elevation point of the country.
-- 'population' is the population of the country.

-- The religion table contains information about the religions and the percentage of the population in each country that follow the religion.
-- 'cid' is the id of the country.
-- 'rid' is the id of the religion.
-- 'rname' is the name of the religion.
-- 'rpercentage' is the percentage of the population in the country who follows the religion.

-- rid, rname, followers per country
CREATE VIEW popPerCountry AS
SELECT
    country.cname,
    religion.rid,
    religion.rname,
    country.population * religion.rpercentage AS countrypop  -- assuming 0 <= rpercentage <= 1
FROM country
INNER JOIN religion
    ON religion.cid = country.cid;

-- sum of all followers for all countries per religion
INSERT INTO Query7 (
    SELECT rid, rname, SUM(countrypop) AS followers
    FROM popPerCountry
    GROUP BY rid, rname
    ORDER BY followers DESC);

DROP VIEW popPerCountry;

-- Query 8 statements

-- The country table contains all the countries in the world and their facts.
-- 'cid' is the id of the country.
-- 'name' is the name of the country.
-- 'height' is the highest elevation point of the country.
-- 'population' is the population of the country.

-- The neighbour table provides information about the countries and their neighbours.
-- 'country' refers to the cid of the first country.
-- 'neighbor' refers to the cid of a country that is neighbouring the first country.
-- 'length' is the length of the border between the two neighbouring countries.
-- Note that if A and B are neighbours, then there two tuples are stored in the table to represent that information (A, B) and (B, A). 

-- The language table contains information about the languages and the percentage of the speakers of the language for each country.
-- 'cid' is the id of the country.
-- 'lid' is the id of the language.
-- 'lname' is the name of the language.
-- 'lpercentage' is the percentage of the population in the country who speak the language.

-- Similar to query 1
-- join cid with languages which have maximal percent speakers
CREATE VIEW cidPopularLanguage AS
SELECT language.cid, language.lname
FROM (
    -- find maximal percent of speakers per cid
    SELECT cid, max(lpercentage) AS maxpercent
    FROM language
    GROUP BY cid
    ) AS maximal
INNER JOIN language
    ON language.cid = maximal.cid
    AND language.lpercentage = maximal.maxpercent;

-- all neighbours that share the same popular language
INSERT INTO Query8 (
    SELECT
        c1.cname AS c1name,
        c2.cname AS c2name,
        l1.lname
    FROM country AS c1
    INNER JOIN cidPopularLanguage AS l1
        ON l1.cid = c1.cid
    INNER JOIN neighbour
        ON neighbour.country = l1.cid
    INNER JOIN cidPopularLanguage AS l2
        ON l2.cid = neighbour.neighbor
    INNER JOIN country AS c2
        ON c2.cid = l2.cid
    WHERE l1.lname = l2.lname  -- c1.cid != c2.cid implied by neighbour
    ORDER BY lname ASC, c1name DESC);

DROP VIEW cidPopularLanguage;

-- Query 9 statements

-- The country table contains all the countries in the world and their facts.
-- 'cid' is the id of the country.
-- 'name' is the name of the country.
-- 'height' is the highest elevation point of the country.
-- 'population' is the population of the country.

-- The oceanAccess table provides information about the countries which have a border with an ocean.
-- 'cid' refers to the cid of the country.
-- 'oid' refers to the oid of the ocean.

-- The ocean table contains information about oceans on the earth.
-- 'oid' is the id of the ocean.
-- 'oname' is the name of the ocean.
-- 'depth' is the depth of the deepest part of the ocean

-- all spans, height + ocean depth or zero
CREATE VIEW allSpans AS
SELECT country.cname AS cname, country.height + ocean.depth AS span  -- assuming depth is positive
FROM country
INNER JOIN oceanAccess
    ON oceanAccess.cid = country.cid
INNER JOIN ocean
    ON ocean.oid = oceanAccess.oid
UNION ALL
SELECT country.cname AS cname, country.height AS span
FROM country;

-- maximal span(s) out of all countries
INSERT INTO Query9 (
    SELECT allSpans.cname, maximal.span
    FROM (
        SELECT MAX(allSpans.span) AS span
        FROM allSpans
        ) AS maximal
    INNER JOIN allSpans
        ON allSpans.span = maximal.span);

DROP VIEW allSpans;

-- Query 10 statements

-- The country table contains all the countries in the world and their facts.
-- 'cid' is the id of the country.
-- 'name' is the name of the country.
-- 'height' is the highest elevation point of the country.
-- 'population' is the population of the country.

-- The neighbour table provides information about the countries and their neighbours.
-- 'country' refers to the cid of the first country.
-- 'neighbor' refers to the cid of a country that is neighbouring the first country.
-- 'length' is the length of the border between the two neighbouring countries.
-- Note that if A and B are neighbours, then there two tuples are stored in the table to represent that information (A, B) and (B, A). 

-- cname, total length of borders per cname
CREATE VIEW allBorders AS
SELECT country.cname AS cname, SUM(neighbour.length) AS borderslength
FROM country
INNER JOIN neighbour
    ON neighbour.country = country.cid
GROUP BY country.cname;

-- maximal total border length(s) out of all countries
INSERT INTO Query10 (
    SELECT allBorders.cname, maximal.borderslength
    FROM (
        SELECT MAX(allBorders.borderslength) AS borderslength
        FROM allBorders
        ) AS maximal
    INNER JOIN allBorders
        ON allBorders.borderslength = maximal.borderslength);

DROP VIEW allBorders;
