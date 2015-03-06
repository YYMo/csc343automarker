-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

SET SEARCH_PATH TO a2;

-- Query 1 statements
CREATE VIEW nei_heights AS
    SELECT country, max(height) AS height
    FROM country JOIN neighbour ON cid = neighbor
    GROUP BY country;

CREATE VIEW c2 AS
    SELECT country AS c1id, cid AS c2id, cname AS c2name
    FROM nei_heights JOIN country ON nei_heights.height = country.height
    WHERE EXISTS (SELECT *
                  FROM neighbour
                  WHERE neighbour.country = nei_heights.country AND neighbour.neighbor = country.cid);

INSERT INTO Query1 (SELECT c1id, cname AS c1name, c2id, c2name         
                           FROM country JOIN c2 ON c1id = cid);

DROP VIEW c2;
DROp VIEW nei_heights;

-- Query 2 statements

INSERT INTO Query2 (
    SELECT cid, cname
    FROM country
    WHERE cid NOT IN (
        SELECT cid
        FROM country NATURAL JOIN oceanAccess)
    ORDER BY cname ASC);

-- Query 3 statements

CREATE VIEW Q2 AS
     SELECT cid, cname
    FROM country
    WHERE cid NOT IN (
        SELECT cid
        FROM country NATURAL JOIN oceanAccess);

CREATE VIEW one_nei AS
    SELECT cid AS c1id, cname AS c1name, neighbor AS c2id
    FROM Q2 JOIN neighbour ON cid = country
    WHERE cid = (SELECT cid FROM Q2 JOIN neighbour ON cid = country GROUP BY cid HAVING count(neighbor)=1);

INSERT INTO Query3 (
    SELECT c1id, c1name, c2id, cname AS c2name
    FROM one_nei JOIN country ON cid = c2id
    ORDER BY c1name ASC);

DROP VIEW one_nei;
DROP VIEW Q2;

-- Query 4 statements

CREATE VIEW indirect AS
    SELECT DISTINCT country AS cid, oid
    FROM neighbour JOIN oceanAccess ON neighbor = cid;

CREATE VIEW all_pairs AS
    SELECT *
    FROM (SELECT * FROM oceanAccess) AS oa UNION (SELECT * FROM indirect);

INSERT INTO Query4 (
    SELECT cname, oname
    FROM all_pairs JOIN country ON all_pairs.cid = country.cid JOIN ocean ON all_pairs.oid = ocean.oid
    ORDER BY cname ASC, oname DESC);

DROP VIEW all_pairs;
DROP VIEW indirect;

-- Query 5 statements

CREATE VIEW avg_hdi AS
    SELECT cid, AVG(hdi_score) AS avghdi
    FROM hdi
    WHERE year >= 2009 AND year <= 2013
    GROUP BY cid
    ORDER BY AVG(hdi_score) DESC;

INSERT INTO Query5 (
    SELECT country.cid AS cid, cname, avghdi
    FROM avg_hdi JOIN country ON avg_hdi.cid = country.cid
    LIMIT 10);

DROP VIEW avg_hdi;

-- Query 6 statements

CREATE VIEW increase AS
    SELECT hdi.cid AS cid
    FROM hdi JOIN hdi AS hdi1 ON hdi.cid = hdi1.cid JOIN hdi AS hdi2 ON hdi.cid = hdi2.cid JOIN hdi AS hdi3 ON hdi.cid = hdi3.cid JOIN hdi AS hdi4 ON hdi.cid = hdi4.cid
    WHERE hdi.year = 2009 AND hdi1.year = 2010 AND hdi2.year = 2011 AND hdi3.year = 2012 AND hdi4.year = 2013 AND hdi.hdi_score < hdi1.hdi_score AND hdi1.hdi_score < hdi2.hdi_score AND hdi2.hdi_score < hdi3.hdi_score AND hdi3.hdi_score < hdi4.hdi_score;

INSERT INTO Query6 (
    SELECT cid, cname
    FROM increase NATURAL JOIN country
    ORDER BY cname ASC);

DROP VIEW increase;

-- Query 7 statements

CREATE VIEW num_ctry AS
    SELECT rid, rname, religion.rpercentage * country.population AS num
    FROM religion NATURAL JOIN country;

INSERT INTO Query7 (
    SELECT rid, rname, SUM(num) AS followers
    FROM num_ctry
    GROUP BY rid, rname
    ORDER BY followers DESC);

DROP VIEW num_ctry;

-- Query 8 statements

CREATE VIEW pop_lang AS
    SELECT cid, MAX(lpercentage) AS max
    FROM language
    GROUP BY cid;

CREATE VIEW pop_lang_name AS
    SELECT language.cid AS cid, lid, lname
    FROM pop_lang JOIN language ON pop_lang.cid = language.cid
    WHERE lpercentage = max;

INSERT INTO Query8 (
    SELECT c1.cname AS c1name, c2.cname AS c2name, clang.lname AS lname
    FROM neighbour JOIN pop_lang_name AS clang ON neighbour.country = clang.cid JOIN pop_lang_name AS nlang ON neighbour.neighbor = nlang.cid JOIN country AS c1 ON neighbour.country = c1.cid JOIN country AS c2 ON neighbour.neighbor = c2.cid
    WHERE clang.lid = nlang.lid
    ORDER BY lname ASC, c1name DESC);

DROP VIEW pop_lang_name;
DROP VIEW pop_lang;

-- Query 9 statements

CREATE VIEW has_ocean AS
    SELECT cid, cname, MAX(height + depth) AS totalspan
    FROM oceanAccess NATURAL JOIN country NATURAL JOIN ocean
    GROUP BY cid, cname;

CREATE VIEW no_ocean AS
    SELECT country.cid, cname, height AS totalspan
    FROM ((SELECT cid FROM country) EXCEPT (SELECT cid FROM oceanAccess)) AS ctry JOIN country ON ctry.cid = country.cid;

INSERT INTO Query9 (
    SELECT cname, totalspan
    FROM ((SELECT * FROM has_ocean) UNION (SELECT * FROM no_ocean)) AS oc_uni
    WHERE totalspan = (SELECT MAX(totalspan) FROM ((SELECT * FROM has_ocean) UNION (SELECT * FROM no_ocean)) AS oc_un));

DROP VIEW no_ocean;
DROP VIEW has_ocean;

-- Query 10 statements

INSERT INTO Query10 (
    SELECT cname, SUM(length) AS borderslength
    FROM neighbour JOIN country ON neighbour.country = country.cid
    GROUP BY neighbour.country, cname
    HAVING SUM(length) = (SELECT MAX(sum) FROM (SELECT SUM(length) AS sum FROM neighbour GROUP BY country) AS len));
