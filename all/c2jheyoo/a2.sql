-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW allNeighbor AS
    SELECT n.country, n.neighbor AS c2id, c.cname AS c2name, c.height
    FROM neighbour n JOIN country c ON n.neighbor = c.cid;

CREATE VIEW highestNeighbor AS 
    SELECT country AS c1id, MAX(height) AS highestHeight
    FROM allNeighbor
    GROUP BY country;

CREATE VIEW countryWithHighest AS
    SELECT hn.c1id, AN.c2id, AN.c2name
    FROM allNeighbor AN, highestNeighbor HN
    WHERE AN.country = HN.c1id AND AN.height = HN.highestHeight;

INSERT INTO Query1 (
    SELECT cwh.c1id, c.cname AS c1name, cwh.c2id, cwh.c2name
    FROM countryWithHighest cwh JOIN country c ON cwh.c1id = c.cid
    ORDER BY c.cname);

DROP VIEW countryWithHighest;
DROP VIEW highestNeighbor;
DROP VIEW allNeighbor;


-- Query 2 statements
    
INSERT INTO Query2 (
    SELECT cid, cname
    FROM country
    WHERE cid NOT IN (
        SELECT cid
        FROM oceanAccess)
    ORDER BY cname);

-- Query 3 statements
    
CREATE VIEW landLocked AS
    SELECT cid, cname
    FROM country
    WHERE cid NOT IN (
        SELECT cid
        FROM oceanAccess);

CREATE VIEW onlyNeighbourCountry AS
    SELECT country
    FROM neighbour
    GROUP BY country
    HAVING COUNT(neighbor) = 1;
    
INSERT INTO Query3 (
    SELECT onc.country AS c1id, ll.cname AS c1name, n.neighbor AS c2id, c.cname AS c2name
    FROM onlyNeighbourCountry onc, neighbour n, country c, landLocked ll
    WHERE onc.country = n.country AND onc.country = ll.cid AND n.neighbor = c.cid
    ORDER BY ll.cname);
    
DROP VIEW landLocked;    
DROP VIEW onlyNeighbourCountry;


-- Query 4 statements

CREATE VIEW inDirect AS
    SELECT n.country AS cid, oa.oid
    FROM neighbour n JOIN oceanAccess oa ON n.neighbor = oa.cid;
    
CREATE VIEW allPairs AS
    SELECT *
    FROM inDirect
    UNION
    SELECT *
    FROM oceanAccess;
    
INSERT INTO Query4 (
    SELECT c.cname, o.oname
    FROM allPairs ap, country c, ocean o
    WHERE ap.cid = c.cid AND ap.oid = o.oid
    ORDER BY c.cname ASC, o.oname DESC);
    
DROP VIEW allPairs;
DROP VIEW inDirect;



-- Query 5 statements

CREATE VIEW average AS
    SELECT cid, AVG(hdi_score) AS avghdi
    FROM hdi
    WHERE year >= 2009 AND year <= 2013
    GROUP BY cid;
    
INSERT INTO Query5 (
    SELECT av.cid, c.cname, av.avghdi
    FROM average av JOIN country c ON av.cid = c.cid
    ORDER BY av.avghdi DESC
    LIMIT 10);
    
DROP VIEW average;


-- Query 6 statements

CREATE VIEW rangeHdi AS
    SELECT *
    FROM hdi 
    WHERE year >= 2009 AND year <= 2013;
    
CREATE VIEW nonIncreasing AS
    SELECT h1.cid AS cid
    FROM rangeHdi h1, rangeHdi h2
    WHERE h1.cid = h2.cid AND h1.year > h2.year AND h1.hdi_score <= h2.hdi_score;
    
INSERT INTO Query6 (
    SELECT rh.cid, c.cname
    FROM rangeHdi rh JOIN country c ON rh.cid = c.cid
    WHERE rh.cid NOT IN (
         SELECT *
         FROM nonIncreasing)
    ORDER BY c.cname);
         
DROP VIEW nonIncreasing;        
DROP VIEW rangeHdi;

-- Query 7 statements

INSERT INTO Query7 (
    SELECT r.rid, r.rname, SUM(c.population * r.rpercentage) AS followers
    FROM religion r JOIN country c ON r.cid = c.cid
    GROUP BY r.rid, r.rname
    ORDER BY followers DESC);
    
-- Query 8 statements

CREATE VIEW maxPercentage AS
    SELECT cid, MAX(lpercentage) AS maxlpercent
    FROM language
    GROUP BY cid;
    
CREATE VIEW mostPopular AS
    SELECT mp.cid, l.lname
    FROM maxPercentage mp, language l
    WHERE mp.cid = l.cid AND mp.maxlpercent = l.lpercentage;
    
INSERT INTO Query8 (
    SELECT n.country, n.neighbor, mpop1.lname
    FROM neighbour n, mostPopular mpop1, mostPopular mpop2
    WHERE n.country = mpop1.cid AND n.neighbor = mpop2.cid AND mpop1.lname = mpop2.lname
    ORDER BY mpop1.lname ASC, n.country DESC);
    
DROP VIEW mostPopular;    
DROP VIEW maxPercentage;


-- Query 9 statements

CREATE VIEW maxDirectSpan AS
    SELECT c.cname AS cname, MAX(c.height + o.depth) AS totalspan
    FROM oceanAccess oa, country c, ocean o
    WHERE oa.cid = c.cid AND oa.oid = o.oid
    GROUP BY c.cname;
    
CREATE VIEW noDirectSpan AS
    SELECT cname, height AS totalspan
    FROM country
    WHERE cid NOT IN (
        SELECT cid
        FROM oceanAccess);
    
CREATE VIEW allSpans AS
    SELECT *
    FROM maxDirectSpan
    UNION
    SELECT *
    FROM noDirectSpan;
    
CREATE VIEW maxValue AS
    SELECT MAX(totalspan) as totalspan
    FROM allSpans;
    
INSERT INTO Query9 (
    SELECT AP.cname, MV.totalspan
    FROM allSpans AP, maxValue MV
    WHERE AP.totalspan = MV.totalspan);
    
DROP VIEW maxValue;
DROP VIEW allSpans;    
DROP VIEW maxDirectSpan;
DROP VIEW noDirectSpan;


-- Query 10 statements

CREATE VIEW totalLength AS
    SELECT country, SUM(length) AS total
    FROM neighbour
    GROUP BY country;
    
CREATE VIEW longest AS
    SELECT MAX(total) AS total
    FROM totalLength;

INSERT INTO Query10 (
    SELECT c.cname, l.total
    FROM longest l, country c, totalLength tl
    WHERE l.total = tl.total AND tl.country = c.cid);
    
DROP VIEW longest;    
DROP VIEW totalLength;

