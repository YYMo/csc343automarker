 -- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements 
-- WIP
 
 
-- Query 2 statements
SELECT DISTINCT country.cid AS cid, country.cname AS cname 
FROM (country LEFT OUTER JOIN oceanAccess 
    ON country.cid = oceanAccess.cid)
WHERE oceanAccess.cid IS NULL
ORDER BY cname ASC;


-- Query 3 statements
CREATE VIEW landlocked AS
SELECT DISTINCT country.cid AS cid, country.name AS cname
FROM (country LEFT OUTER JOIN oceanAccess
    ON country.cid = oceanAccess.cid)
WHERE oceanAccess.cid IS NULL
ORDER BY cname ASC; 

SELECT l.cid AS c1id, l.name AS c1name, n.cid AS c2id, n.name AS c2name 
FROM (landlocked l JOIN neighbour n
    ON l.cid = n.country)
GROUP BY l.cid
HAVING COUNT(n.neighbor) = 1
ORDER BY c1name ASC;

DROP VIEW landlocked;


-- Query 4 statements
SELECT DISTINCT c.name AS cname, o.oname AS oname
FROM country c 
    JOIN neighbour n 
        ON c.cid = n.country
    JOIN oceanAccess oa 
        ON c.cid = oa.cid OR n.neighbor = oa.cid 
    JOIN ocean o 
        ON oa.oid = o.oid
ORDER BY cname ASC
    oname DESC;


-- Query 5 statements
SELECT country.cid AS cid, country.name AS cname, AVG(hdi) AS avghdi 
FROM country 
    JOIN hdi
ON country.cid = hdi.cid
WHERE hdi.cid BETWEEN 2009 AND 2013
GROUP BY country.cid, country.name 
ORDER BY  avghdi DESC 55 LIMIT 10;


-- Query 6 statements
INSERT INTO Query6
(
    SELECT validcid AS cid, country.cname AS cname
    FROM ((
        SELECT h1.cid AS validcid
        FROM hdi h1, hdi h2, hdi h3, hdi h4, hdi h5,
        WHERE h1.year = 2009 AND h2.year = 2010 AND h3.year = 2011 AND h4.year = 2012 AND h5.year = 2013
            AND h5.hdi_score > h4.hdi_score AND h4.hdi_score > h3.hdi_score AND h3.hdi_score > h2.hdi_score 
            AND h2.hdi_score > h1.hdi_score) 
        AS increase5years 
    		JOIN country ON validcid = country.cid)
    ORDER BY cname ASC
); 

-- Query 7 statements
INSERT INTO Query7
(
    SELECT religion.rid AS rid, religion.rname AS rname, SUM(religion.rpercentage * country.population) AS followers
    FROM religion JOIN country
    ON country.cid = religion.cid
    GROUP BY rname
    ORDER BY followers DESC
);

-- Query 8 statements
CREATE VIEW popularlanguage AS
SELECT cid, MAX(lpercentage) AS maxlang
FROM language
GROUP BY cid;

CREATE VIEW countrydata AS
SELECT cid, c.cname AS cname, language.lname AS lname ,maxlang
FROM (VIEW popularlanguage pl JOIN TABLE country c ON pl.cid = c.cid; 
    JOIN language 
    ON maxlang = lpercentage AND pl.cid = language.cid
);

INSERT INTO Query8
(
    SELECT cd1.cname AS c1name, cd2.cname AS c2name, cd.lname AS lname
    FROM VIEW countrydata cd JOIN TABLE neighbour n JOIN VIEW countrydata cd2 
    WHERE (cd.cid = n.country AND n.neighbor = cd2.cid
        AND cd.cid <> cd2.cid AND cd.lname = cd2.lname)
    ORDER BY lname ASC, c1name DESC
);

DROP VIEW popularlanguage;
DROP VIEW countrydata;


-- Query 9 statements
CREATE VIEW oceanspan AS
SELECT c.cname AS cname, MAX(c.height + o.depth) AS span
FROM country c JOIN oceanAccess oa ON c.cid = oa.cid
    JOIN ocean o ON oa.oid = o.oid
GROUP BY cname;

CREATE VIEW landspan AS 
SELECT c.cname AS cname, MAX(c.height) AS span
FROM (country LEFT OUTER JOIN oceanAccess ON country.cid = oceanAccess.cid)
WHERE oceanAccess.cid IS NULL
GROUP BY cname;

INSERT INTO Query9
(
    SELECT cname, span AS totalspan
    FROM VIEW oceanspan UNION VIEW landspan
    ORDER BY totalspan DESC
    LIMIT 1
);

DROP VIEW oceanspan;
DROP VIEW landspan;


-- Query 10 statements
INSERT INTO Query10
(
    SELECT  country.cname AS cname, SUM(neighbour.length) AS borderslength
    FROM country JOIN neighbour ON country.cid = neighbour.country
    GROUP BY cname
    ORDER BY borderslength DESC
    LIMIT 1
);