-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW neighbourElev AS (
    SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name, c2.height AS c2height
    FROM country c1, country c2, neighbour n
    WHERE c1.cid = n.country AND c2.cid = n.neighbor);

CREATE VIEW answer AS (
    SELECT c1id, c1name, c2id, c2name, max(c2height) 
    FROM neighbourElev 
    GROUP BY c1id, c1name, c2id, c2name
    ORDER BY c1name);
    
INSERT INTO Query1 (SELECT c1id, c1name, c2id, c2name FROM answer);

DROP VIEW answer;
DROP VIEW neighbourElev;

-- Query 2 statements
CREATE VIEW answer AS (
    SELECT cid
    FROM country
    WHERE NOT EXISTS (SELECT cid FROM oceanAccess));

INSERT INTO Query2 (
    SELECT answer.cid as cid, country.cname as cname FROM answer, country
    WHERE answer.cid = country.cid
    ORDER BY cname);

DROP VIEW answer;

-- Query 3 statements
CREATE VIEW oneNeigh AS (
    SELECT country as c1id, neighbor as c2id
    FROM neighbour
    GROUP BY c1id, c2id
    HAVING (count(country) = 1));

CREATE VIEW answer AS (
    SELECT c1id, c2id
    FROM oneNeigh, Query2
    WHERE (c1id = cid));

INSERT INTO Query3(
    SELECT c1id, c1.cname as c1name, c2id, c2.cname as c2name
    FROM answer, country c1, country c2
    WHERE (c1id = c1.cid) AND (c2id = c2.cid)
    ORDER BY c1name);

DROP VIEW answer;
DROP VIEW oneNeigh;

-- Query 4 statements
CREATE VIEW directOceans AS (
    SELECT c.cname as cname, o.oname as oname
    FROM country c, ocean o, oceanAccess oA
    WHERE ((c.cid = oA.cid) AND (o.oid = oA.oid)));
    
CREATE VIEW indirectOceans AS (
    SELECT c.cname as cname, o.oname as oname
    FROM neighbour n, ocean o, oceanAccess oA, country c
    WHERE ((n.country = c.cid) AND (n.neighbor = oA.cid) AND (o.oid = oA.oid)));

CREATE VIEW answer AS (
    (SELECT cname, oname FROM directOceans)
    UNION
    (SELECT cname, oname FROM indirectOceans));

INSERT INTO Query4 (SELECT cname, oname FROM answer ORDER BY cname, oname DESC);

DROP VIEW answer;
DROP VIEW indirectOceans;
DROP VIEW directOceans;

-- Query 5 statements
CREATE VIEW avgHDIs AS (
    SELECT c.cid as cid, c.cname as cname, avg(hdi.hdi_score) as avghdi
    FROM country c, hdi
    WHERE ((c.cid = hdi.cid) AND (year >= 2009) AND (year <= 2013))
    GROUP BY c.cid, c.cname
    ORDER BY avghdi DESC LIMIT 10);

INSERT INTO Query5 (SELECT * FROM avgHDIs);

DROP VIEW avgHDIs;

-- Query 6 statements
CREATE VIEW countriesHDI AS (
    SELECT c.cid as cid, c.cname as cname, year, hdi_score
    FROM country c, hdi
    WHERE ((c.cid = hdi.cid) AND (year >= 2009) AND (year <= 2013)));

CREATE VIEW answer AS (
    SELECT y13.cid as cid, y13.cname as cname
    FROM countriesHDI y09, countriesHDI y10, countriesHDI y11, countriesHDI y12, countriesHDI y13
    WHERE (
            (y09.cid = y10.cid) AND (y10.cid = y11.cid) AND (y11.cid = y12.cid) AND (y12.cid = y13.cid) AND
            (y09.year = 2009) AND (y10.year = 2010) AND (y11.year = 2011) AND (y12.year = 2012) AND (y13.year = 2013) AND
            (y13.hdi_score > y12.hdi_score) AND (y12.hdi_score > y11.hdi_score) AND (y11.hdi_score > y10.hdi_score) AND (y10.hdi_score > y09.hdi_score)
           )
    ORDER BY cname);

INSERT INTO Query6 (SELECT * FROM answer);

DROP VIEW answer;
DROP VIEW countriesHDI;

-- Query 7 statements
CREATE VIEW populationsReligions AS (
    SELECT rid, rname, rpercentage, population
    FROM religion r, country c
    WHERE (c.cid = r.cid));

CREATE VIEW answer AS (
    SELECT rid, rname, sum(rpercentage * population) as followers
    FROM populationsReligions
    GROUP BY rid, rname
    ORDER BY followers DESC );
    
INSERT INTO Query7 (SELECT * FROM answer);

DROP VIEW answer;
DROP VIEW populationsReligions;

-- Query 8 statements
CREATE VIEW mostPopLang AS (
    SELECT cid, lname, lid, max(lpercentage)
    FROM language
    GROUP BY cid, lname, lid);

CREATE VIEW answer AS (
    SELECT country as c1name, neighbor as c2name, L1.lname as lname
    FROM neighbour, mostPopLang L1, mostPopLang L2
    WHERE ((country = L1.cid) AND (neighbor = L2.cid) AND (L1.lid = L2.lid))
    ORDER BY lname, c1name DESC);
    
INSERT INTO Query8 (SELECT * FROM answer);

DROP VIEW answer;
DROP VIEW mostPopLang;

-- Query 9 statements
CREATE VIEW countriesDeepestOcean AS (
    SELECT c.cid as cid, cname, height, max(o.depth) as depth
    FROM country c, oceanAccess oA, ocean o
    WHERE ((c.cid = oA.cid) AND (oA.oid = o.oid))
    GROUP BY c.cid);

CREATE VIEW differences AS (
    SELECT cname, sum(height + depth) as totalspan
    FROM countriesDeepestOcean
    GROUP BY cname);
    
CREATE VIEW noOcean AS (
    SELECT cname, height as totalspan
    FROM country
    WHERE cid NOT IN (SELECT cid FROM oceanAccess));

CREATE VIEW answer AS (
    (SELECT cname, max(totalspan) as totalspan
    FROM differences
    GROUP BY cname)
    
    UNION
    
    (SELECT cname, max(totalspan) as totalspan
    FROM noOcean
    GROUP BY cname));

INSERT INTO Query9 (SELECT cname, max(totalspan) as totalspan FROM answer GROUP BY cname);

DROP VIEW answer;
DROP VIEW noOcean;
DROP VIEW differences;
DROP VIEW countriesDeepestOcean;

-- Query 10 statements
CREATE VIEW totalBorders AS (
    SELECT country AS cname, sum(length) AS borders
    FROM neighbour
    GROUP BY cname);
    
INSERT INTO Query10 (
    SELECT cname, max(borders) AS borderslength
    FROM totalBorders
    GROUP BY cname);

DROP VIEW totalBorders;
