--SET search_path TO A2;
-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
SET search_path TO A2;

INSERT INTO query1ans
SELECT c1.cid c1id, c1.cname c1name, c2.cid c2id, c2.cname c2name
FROM country c1, country c2, neighbour n 
WHERE c1.cid = n.country AND c2.cid = n.neighbor 
  AND c2.height >= ALL(SELECT c3.height c3height
            FROM country c3, neighbour n2 
            WHERE c1.cid = n2.country AND c3.cid = n2.neighbor)
ORDER BY c1name ASC;

-- Query 2 statements
INSERT INTO query2ans
SELECT cid, cname
FROM country
WHERE (cid) NOT IN (SELECT DISTINCT cid
                    FROM oceanAccess)
ORDER BY cname ASC;

-- Query 3 statements
CREATE VIEW landLockedCountries AS
SELECT cid
FROM country
WHERE (cid) NOT IN (SELECT DISTINCT cid
                    FROM oceanAccess);

CREATE VIEW oneNeighbour AS
SELECT nc.cid cid
FROM (SELECT country cid, COUNT(country) numberOfNeighbours
      FROM neighbour
      GROUP BY country
     ) AS nc
WHERE nc.numberOfNEighbours = 1;

INSERT INTO query3ans
SELECT c1.cid c1id, c1.cname c1name, c2.cid c2id, c2.cname c2name
FROM (SELECT * FROM landLockedCountries 
      INTERSECT 
      SELECT * FROM oneNeighbour) AS t, country c1, country c2, neighbour n
WHERE t.cid = c1.cid AND t.cid = n.country AND n.neighbor = c2.cid
ORDER BY c1name ASC; 

DROP VIEW landLockedCountries;
DROP VIEW oneNeighbour;

-- Query 4 statements
INSERT INTO query4ans
SELECT c.cname cname, o.oname oname
FROM
(SELECT neighbour.country cid, oceanAccess.oid oid
 FROM neighbour, oceanAccess
 WHERE neighbour.neighbor = oceanAccess.cid
 UNION
 SELECT * 
 FROM oceanAccess) AS t, country c, ocean o
WHERE t.cid = c.cid AND t.oid = o.oid
ORDER BY cname ASC, oname DESC; 

-- Query 5 statements
INSERT INTO query5ans
SELECT c.cid cid, c.cname cname, t.avghdi avghdi 
FROM 
(SELECT cid, AVG(hdi_score) avghdi
 FROM hdi
 WHERE year >= 2009 AND year <= 2013
 GROUP BY cid) AS t, country c
WHERE t.cid = c.cid
ORDER BY avghdi DESC
LIMIT 10;

-- Query 6 statements
CREATE VIEW latestHDI AS
SELECT *
FROM hdi
WHERE year >= 2009 AND year <= 2013;

INSERT INTO query6ans
SELECT country.cid cid, country.cname cname
FROM 
(SELECT t1.cid cid
 FROM latestHDI t1, latestHDI t2, latestHDI t3, latestHDI t4, latestHDI t5
 WHERE t1.cid = t2.cid AND t1.cid = t3.cid AND t1.cid = t4.cid AND t1.cid = t5.cid AND
       t1.year = 2009 AND t2.year = 2010 AND t3.year = 2011 AND t4.year = 2012 AND t5.year = 2013 AND
       t1.hdi_score < t2.hdi_score AND t2.hdi_score < t3.hdi_score AND t3.hdi_score < t4.hdi_score AND t4.hdi_score < t5.hdi_score) AS r, country
WHERE r.cid = country.cid
ORDER BY cname ASC;

DROP VIEW latestHDI;

-- Query 7 statements
INSERT INTO query7ans
SELECT religion.rid rid, religion.rname rname, SUM(country.population*religion.rpercentage) followers 
FROM country, religion
WHERE country.cid = religion.cid
GROUP BY rid, rname
ORDER BY followers DESC;

-- Query 8 statements
CREATE VIEW mostSpoken AS
SELECT language.cid cid, language.lname lname
FROM (SELECT cid, MAX(lpercentage) maxPercent
      FROM language
      GROUP BY cid) AS t, language
WHERE t.cid = language.cid AND t.maxPercent = language.lpercentage;

INSERT INTO query8ans
SELECT c1.cname c1name, c2.cname c2name, m1.lname lname
FROM neighbour n, mostSpoken m1, mostSpoken m2, country c1, country c2
WHERE n.country = m1.cid AND n.neighbor = m2.cid AND m1.lname=m2.lname AND
      c1.cid = n.country AND c2.cid = n.neighbor 
ORDER BY lname ASC, c1name DESC;

DROP VIEW mostSpoken;

-- Query 9 statements
CREATE VIEW oa AS
SELECT country.cname cname, country.height+ocean.depth totalspan
FROM country, ocean, oceanAccess
WHERE country.cid = oceanAccess.cid AND ocean.oid = oceanAccess.oid AND
(country.height+ocean.depth) IN (SELECT MAX(country.height + ocean.depth)
                                 FROM country, ocean, oceanAccess
                                 WHERE country.cid = oceanAccess.cid AND ocean.oid = oceanAccess.oid); 

CREATE VIEW noa AS
SELECT country.cname cname, country.height totalspan
FROM country
WHERE (country.height) IN (SELECT MAX(country.height)
                           FROM country); 

INSERT INTO query9ans
SELECT t2.cname cname, t2.totalspan totalspan
FROM
(SELECT * 
FROM oa
UNION
SELECT *
FROM noa) AS t2
WHERE (t2.totalspan) IN (SELECT MAX(t1.totalspan)
                         FROM
                         (SELECT * 
                          FROM oa
                          UNION
                          SELECT *
                          FROM noa) AS t1); 

DROP VIEW oa;
DROP VIEW noa;

-- Query 10 statements
CREATE VIEW totalBorder AS
SELECT neighbour.country cid, SUM(neighbour.length) borderslength
FROM neighbour
GROUP BY cid;

INSERT INTO query10ans
SELECT country.cname cname, totalBorder.borderslength borderslength
FROM country, totalBorder
WHERE country.cid = totalBorder.cid AND (totalBorder.borderslength) IN (SELECT MAX(borderslength)
                                                                       FROM totalBorder);

DROP VIEW totalBorder;
