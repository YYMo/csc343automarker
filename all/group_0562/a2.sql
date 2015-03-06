-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
DELETE FROM query1;
--DROP VIEW IF EXISTS countryWithNeighbors CASCADE;
--DROP VIEW IF EXISTS maxNeighbor CASCADE;
--DROP VIEW IF EXISTS maxWithCountry CASCADE;
CREATE VIEW countryWithNeighbors AS SELECT n.country, n.neighbor, c.cname, c.height
  FROM neighbour as n
  INNER JOIN country as c
  ON n.neighbor = c.cid;
CREATE VIEW maxNeighbor AS SELECT country, MAX(height) FROM countryWithNeighbors GROUP BY country;
CREATE VIEW maxWithCountry AS SELECT n.country as c1id, c.cname as c1name, n.neighbor as c2id, n.cname as c2name
  FROM countryWithNeighbors as n
  JOIN maxNeighbor as m
  ON n.height = m.max 
  AND n.country = m.country
  JOIN country as c
  ON c.cid = n.country
  ORDER BY c1name ASC;
insert into query1 select * FROM maxWithCountry;
DROP VIEW IF EXISTS countryWithNeighbors CASCADE;
DROP VIEW IF EXISTS maxNeighbor CASCADE;
DROP VIEW IF EXISTS maxWithCountry CASCADE;

-- Query 2 statements
DELETE FROM query2;
INSERT INTO query2 (
SELECT country.cid, country.cname 
  FROM country 
  LEFT JOIN oceanAccess 
  ON country.cid=oceanAccess.cid 
  WHERE oid IS NULL);

-- Query 3 statements
DELETE FROM query3;
--DROP VIEW IF EXISTS landLocked CASCADE;
--DROP VIEW IF EXISTS landNeighbors CASCADE;
CREATE VIEW landLocked AS SELECT country.cid, country.cname 
  FROM country 
  LEFT JOIN oceanAccess 
  ON country.cid=oceanAccess.cid 
  WHERE oid IS NULL;
CREATE VIEW landNeighbors AS SELECT l.cid as c1id, l.cname as c1name, n.neighbor as c2id, c.cname as c2name
  FROM neighbour as n 
  JOIN landLocked as l 
  ON l.cid=n.country
  JOIN country as c
  ON c.cid = n.neighbor;
INSERT INTO query3 (
SELECT landNeighbors.c1id, c1name, c2id, c2name 
  FROM landNeighbors 
  JOIN (SELECT c1id FROM landNeighbors GROUP BY c1id HAVING COUNT(c1id)=1) as t 
  ON t.c1id = landNeighbors.c1id
  ORDER BY c1name ASC);
DROP VIEW IF EXISTS landLocked CASCADE;
DROP VIEW IF EXISTS landNeighbors CASCADE;

-- Query 4 statements
DELETE FROM query4;
--DROP VIEW IF EXISTS countriesNeighborsOceans CASCADE;
CREATE VIEW countriesNeighborsOceans AS SELECT c.cname, n.country, n.neighbor, a.oid, o.oname
  FROM country as c
  JOIN neighbour as n
  ON n.country = c.cid
  JOIN oceanAccess as a
  ON c.cid=a.cid
  OR n.neighbor=a.cid
  JOIN ocean as o
  ON o.oid=a.oid;
INSERT INTO query4 (
SELECT DISTINCT(cname), oname 
  FROM countriesNeighborsOceans
  ORDER BY cname ASC, oname DESC);
DROP VIEW IF EXISTS countriesNeighborsOceans CASCADE;

-- Query 5 statements
DELETE FROM query5;
--DROP VIEW IF EXISTS highestHDI5Years CASCADE;
CREATE VIEW highestHDI5Years AS SELECT c.cid, c.cname, SUM(hdi_score)/COUNT(hdi_score) as avghdi
  FROM hdi
  JOIN country as c
  ON c.cid = hdi.cid
  WHERE hdi.year>2008
  AND hdi.year<2014
  GROUP BY c.cid, c.cname;
INSERT INTO query5 (
SELECT * 
  FROM highestHDI5Years
  ORDER BY avghdi DESC
  LIMIT 10);
DROP VIEW IF EXISTS highestHDI5Years CASCADE;

-- Query 6 statements

DELETE FROM query6;
CREATE VIEW increasing AS SELECT hdi.cid 
  FROM hdi 
  FULL OUTER JOIN (SELECT h1.cid, h1.year, h1.hdi_score 
    FROM (hdi as h1 CROSS JOIN hdi AS h2) 
    WHERE h1.year > h2.year 
    AND h2.year <= 2013 
    AND h2.year >= 2009 
    AND h1.year >= 2009 
    AND h1.year <= 2013 
    AND h1.cid=h2.cid 
    AND h1.hdi_score<h2.hdi_score) AS h3 
  ON hdi.cid=h3.cid 
  WHERE h3.cid IS NULL;
INSERT INTO query6 SELECT country.cid, country.cname 
  FROM country 
  JOIN increasing 
  ON country.cid=increasing.cid 
  GROUP BY country.cid, country.cname
  ORDER BY country.cname ASC;
DROP VIEW increasing;

-- Query 7 statements
DELETE FROM query7;
CREATE VIEW totalReligion AS SELECT r.rid, r.rname, r.rpercentage * c.population AS cfollowers 
  FROM religion as r
  JOIN country as c
  ON r.cid=c.cid;
INSERT INTO query7 (
  SELECT tr.rid, tr.rname, SUM(tr.cfollowers) AS followers
  FROM totalReligion AS tr
  GROUP BY tr.rid, tr.rname 
  ORDER BY followers DESC);
DROP VIEW totalReligion;

-- Query 8 statements
DELETE FROM query8;
DROP VIEW IF EXISTS cMaxLangPerPop;
CREATE VIEW cMaxLangPerPop AS SELECT c.cid, c.cname, l.lname
  FROM country AS c
  JOIN language AS l
  ON c.cid=l.cid
  JOIN (SELECT cid, MAX(lpercentage) AS maxp FROM language GROUP BY cid) as m
  ON c.cid=m.cid
  AND l.lpercentage = m.maxp
  GROUP BY c.cid, c.cname, l.lname;
INSERT INTO query8(
SELECT c1.cname AS c1name, c2.cname AS c2name, c1.lname
  FROM cMaxLangPerPop AS c1
  JOIN neighbour AS n
  ON c1.cid=n.country
  JOIN cMaxLangPerPop AS c2
  ON c2.cid=n.neighbor
  WHERE c1.lname=c2.lname
  ORDER BY lname ASC, c1name DESC);
DROP VIEW IF EXISTS cMaxLangPerPop;

-- Query  9 statements
DELETE FROM query9;
DROP VIEW IF EXISTS countryOceans;
CREATE VIEW countryOceans AS SELECT c.cname, c.height, o.depth
  FROM country AS c
  FULL OUTER JOIN oceanAccess AS oa
  ON c.cid=oa.cid
  FULL OUTER JOIN ocean AS o
  ON oa.oid=o.oid;
INSERT INTO query9(
SELECT cname, MAX(height+depth)
  FROM (SELECT cname, height, CASE WHEN depth IS NULL THEN 0 ELSE depth END FROM countryOceans) as c
  GROUP BY cname);
DROP VIEW IF EXISTS countryOceans;

-- Query 10 statements
DELETE FROM query10;
DROP VIEW IF EXISTS sumLength;
CREATE VIEW sumLength AS SELECT c.cname, SUM(n.length) AS borderslength
  FROM neighbour AS n
  JOIN country as c
  ON c.cid=n.country
  GROUP BY c.cname;
INSERT INTO query10 (
SELECT cname, borderslength
  FROM sumLength
  JOIN (SELECT MAX(borderslength) as maxb FROM sumLength) as m
  ON m.maxb=borderslength);
DROP VIEW IF EXISTS sumLength;

