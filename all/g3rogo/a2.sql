-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW info AS
SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name, c2.height as height
FROM neighbour, country c1, country c2
WHERE c1.cid = country AND c2.cid = neighbor;

INSERT INTO Query1 (
    SELECT i.c1id as c1id, i.c1name as c1name, i.c2id as c2id, i.c2name as c2name
    FROM info i, (
        SELECT c1id, max(height)as maxh 
	FROM info
	GROUP BY c1id) mh
    WHERE i.c1id = mh.c1id AND i.height = mh.maxh
    ORDER BY c1id ASC);
    
DROP VIEW info;

-- Query 2 statements
INSERT INTO Query2 (
    SELECT cid, cname
    FROM country
    WHERE cid NOT IN (SELECT cid FROM oceanAccess)
    ORDER BY cname ASC);


-- Query 3 statements
CREATE VIEW ones AS 
    (SELECT country, count(neighbour) as count
    FROM neighbour
    GROUP BY country
    HAVING count(neighbour) = 1);
    
INSERT INTO Query3 (
    SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name
    FROM neighbour, country c1, country c2
    WHERE country = c1.cid AND neighbor = c2.cid AND c1.cid IN (SELECT country as c1id FROM ones) 
    AND c1.cid NOT IN (SELECT cid from oceanAccess)
    ORDER BY c1name ASC);
    
DROP VIEW ones;


-- Query 4 statements
INSERT INTO Query4(
    (SELECT cname, oname
    FROM oceanAccess, ocean, country
    WHERE oceanAccess.cid = country.cid AND oceanAccess.oid = ocean.oid
    )
    UNION
    (SELECT c.cname as cname, o.oname as oname
    FROM neighbour n, country c, oceanAccess a, ocean o
    WHERE n.country = c.cid AND n.neighbor = a.cid AND a.oid = o.oid
    )
    ORDER BY cname ASC, oname DESC
);


-- Query 5 statements
CREATE VIEW avgs AS
   SELECT cid, avg(hdi_score) as avhdi
   FROM hdi
   WHERE year <= 2013 AND year >= 2009
   GROUP BY cid;

INSERT INTO Query5(
     SELECT avgs.cid as cid, cname, avhdi
     FROM avgs, country 
     WHERE avgs.cid = country.cid
     ORDER BY avhdi DESC
     LIMIT 10);
     
DROP VIEW avgs;


-- Query 6 statements
CREATE VIEW increasing AS
    SELECT h1.cid as cid
    FROM hdi h1, hdi h2, hdi h3, hdi h4, hdi h5
    WHERE h1.cid = h2.cid AND h2.cid = h3.cid AND h3.cid = h4.cid AND h4.cid = h5.cid
         AND h1.year = 2009 AND h2.year = 2010 AND h3.year = 2011 AND h4.year = 2012 AND h5.year = 2013
	 AND h1.hdi_score < h2.hdi_score AND h2.hdi_score < h3.hdi_score AND h3.hdi_score < h4.hdi_score AND h4.hdi_score < h5.hdi_score;
	 
INSERT INTO Query6(
    SELECT country.cid as cid, cname
    FROM increasing, country
    WHERE increasing.cid = country.cid
    ORDER BY cname ASC);

DROP VIEW increasing;

-- Query 7 statements
CREATE VIEW peoples AS
    SELECT rid, cname, rname, (rpercentage * population) as people
    FROM country, religion
    WHERE country.cid = religion.cid;
	 
INSERT INTO Query7(
    SELECT rid, rname, sum(people) as followers
    FROM peoples
    GROUP BY rid, rname
    ORDER BY followers DESC);

DROP VIEW peoples;

-- Query 8 statements
CREATE VIEW maxes AS
    SELECT cid, max(lpercentage) as maxp
    FROM language
    GROUP BY cid;
    
CREATE VIEW cl AS
    SELECT country.cid as cid, country.cname as cname, language.lname as lname
    FROM maxes, language, country
    WHERE maxes.cid = language.cid AND language.cid = country.cid AND maxes.maxp = language.lpercentage;
    
INSERT INTO Query8(
    SELECT cl1.cname as c1name, cl2.cname as c2name, cl1.lname as lname
    FROM neighbour n, cl cl1, cl cl2
    WHERE n.country = cl1.cid AND n.neighbor = cl2.cid AND cl1.lname = cl2.lname
    ORDER BY lname ASC, c1name DESC);

DROP VIEW cl;
DROP VIEW maxes;



-- Query 9 statements
CREATE VIEW mdepths AS
    SELECT cid, max(depth) as mdepth
    FROM oceanAccess, ocean
    WHERE oceanAccess.oid = ocean.oid
    GROUP BY cid;
 
INSERT INTO Query9 (   
    SELECT c.cname as cname, (m.mdepth + c.height) as totalspan
    FROM country c, mdepths m
    WHERE c.cid = m.cid AND (m.mdepth + c.height) IN 
                                    (SELECT max(a.height+b.mdepth) FROM country a, mdepths b WHERE a.cid = b.cid));
    
DROP VIEW mdepths;

-- Query 10 statements
CREATE VIEW bl AS
    SELECT country as cid, sum(length) as blength
    FROM neighbour
    GROUP BY country;
    
INSERT INTO Query10 (
    SELECT country.cname as cname, bl.blength as borderslength
    FROM country, bl
    WHERE country.cid = bl.cid AND bl.blength IN (SELECT max(blength) FROM bl));
    
DROP VIEW bl;

