-- Add below your SQL statements.                                                                                                                                                    
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.                                                           
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.                                                                                             

-- Query 1 statements

CREATE VIEW neighbours as
SELECT country as c1id, cname as c1name, neighbor as c2id 
FROM a2.neighbour join a2.country on neighbour.country=country.cid;

INSERT INTO Query1 (SELECT c1id, c1name, c2id, cname as c2name
FROM (neighbours join a2.country on neighbours.c2id=country.cid) N1 
WHERE height>= (SELECT MAX(height) 
FROM neighbours join a2.country on neighbours.c2id=country.cid 
GROUP BY c1id HAVING N1.c1id=c1id)
ORDER BY c1name ASC);

DROP VIEW neighbours;

-- Query 2 statements 

INSERT INTO Query2 (SELECT cid, cname  
FROM  a2.country
WHERE cid NOT IN (SELECT cid 
FROM a2.oceanAccess)
ORDER BY cname ASC);

-- Query 3 statements  

CREATE VIEW landLocked as
SELECT cid, cname  
FROM  a2.country
WHERE cid NOT IN (SELECT cid 
FROM a2.oceanAccess)
ORDER BY cname ASC;


CREATE VIEW oneNeighbour as
SELECT cid 
FROM landLocked join a2.neighbour on landLocked.cid=neighbour.country 
GROUP BY cid 
HAVING count(neighbor)=1;

CREATE VIEW information as
SELECT oneNeighbour.cid as c1id, neighbour.neighbor as c2id, country.cname as c2name
FROM oneNeighbour, a2.neighbour, a2.country
WHERE oneNeighbour.cid = neighbour.country AND neighbour.neighbor=country.cid 
ORDER BY oneNeighbour.cid ASC;

INSERT INTO Query3 (SELECT c1id, country.cname as c1name, c2id, c2name
FROM  information join a2.country on information.c1id=country.cid
ORDER BY cname ASC);

DROP VIEW information;
DROP VIEW oneNeighbour;
DROP VIEW landLocked;

-- Query 4 statements     

CREATE VIEW directAccess as
SELECT cname, oname
FROM a2.oceanAccess natural join a2.country natural join ocean
ORDER BY cname ASC, oname DESC;

CREATE VIEW indirectAccess as
SELECT cname, neighbor 
FROM a2.country join a2.neighbour on country.cid = neighbour.country
WHERE neighbor IN (SELECT cid FROM directAccess join country on directAccess.cname=country.cname);

INSERT INTO Query4 ((SELECT cname, oname
FROM a2.oceanAccess natural join a2.country natural join a2.ocean)
UNION
(SELECT country.cname as cname, oname 
FROM indirectAccess,a2.oceanAccess,a2.ocean,a2.country
WHERE indirectAccess.neighbor = oceanAccess.cid and oceanAccess.oid = ocean.oid and indirectAccess.cname=country.cname)
ORDER BY cname ASC, oname DESC);

DROP VIEW indirectAccess;
DROP VIEW directAccess;

-- Query 5 statements

CREATE VIEW hdi_2009_2013 as
SELECT cid, cname, hdi_score, year 
FROM a2.hdi natural join a2.country
WHERE year>= 2009 and year<=2013
ORDER BY year;

CREATE VIEW hdiavg as
SELECT cid, avg(hdi_score) as avghdi
FROM hdi_2009_2013
GROUP BY cid
LIMIT 10;

INSERT INTO Query5 (SELECT country.cid as cid, cname, avghdi
FROM hdiavg join a2.country on hdiavg.cid=country.cid
ORDER BY avghdi DESC);

DROP VIEW hdiavg;
DROP VIEW hdi_2009_2013;

-- Query 6 statements                                                                                                                               

CREATE VIEW hdi_2009_2013 as
SELECT cid, cname, hdi_score, year 
FROM a2.hdi natural join a2.country
WHERE year>= 2009 and year<=2013
ORDER BY year ASC, cid ASC;

CREATE VIEW hdi_2010 as
SELECT G2.cid, G2.cname, G2.hdi_score, G2.year
FROM (SELECT * FROM hdi_2009_2013 WHERE year=2009) G1,
(SELECT * FROM hdi_2009_2013 WHERE year=2010) G2
WHERE G1.cid=G2.cid and G1.hdi_score<G2.hdi_score;

CREATE VIEW hdi_2011 as
SELECT G2.cid, G2.cname, G2.hdi_score, G2.year
FROM (SELECT * FROM hdi_2010) G1,
(SELECT * FROM hdi_2009_2013 WHERE year=2011) G2
WHERE G1.cid=G2.cid and G1.hdi_score<G2.hdi_score;

CREATE VIEW hdi_2012 as
SELECT G2.cid, G2.cname, G2.hdi_score, G2.year
FROM (SELECT * FROM hdi_2011) G1,
(SELECT * FROM hdi_2009_2013 WHERE year=2012) G2
WHERE G1.cid=G2.cid and G1.hdi_score<G2.hdi_score;

CREATE VIEW hdi_2013 as
SELECT G2.cid, G2.cname, G2.hdi_score, G2.year
FROM (SELECT * FROM hdi_2012) G1,
(SELECT * FROM hdi_2009_2013 WHERE year=2013) G2
WHERE G1.cid=G2.cid and G1.hdi_score<G2.hdi_score;


INSERT INTO Query6 (SELECT cid, cname
FROM hdi_2013 
ORDER BY cname ASC);

DROP VIEW hdi_2013;
DROP VIEW hdi_2012;
DROP VIEW hdi_2011;
DROP VIEW hdi_2010;
DROP VIEW hdi_2009_2013;

-- Query 7 statements                                                                                                                                                                 

CREATE VIEW ridFollowers as 
SELECT rid, sum(rpercentage*population) as followers
FROM a2.religion natural join a2.country
GROUP BY rid
ORDER BY sum(rpercentage*population) DESC;

INSERT INTO Query7 (SELECT distinct rid, rname, followers
FROM ridFollowers natural join a2.religion
ORDER BY followers DESC);

DROP VIEW ridFollowers;

-- Query 8 statements                                                                                                                                                 

CREATE VIEW neighbours as
SELECT cid as country, cname, neighbor 
FROM a2.neighbour join a2.country on neighbour.country=country.cid;

CREATE VIEW popularLanguage as
SELECT cid as pais, lname, lpercentage
FROM (a2.language natural join a2.country) L1
WHERE lpercentage >= (SELECT MAX(lpercentage) FROM a2.language natural join a2.country WHERE cid=L1.cid);

CREATE VIEW countryLanguage as 
SELECT country,cname,lname,neighbor 
FROM neighbours join popularLanguage on neighbours.country=popularLanguage.pais;

CREATE VIEW neighbourLanguage as 
SELECT neighbor,cname,lname,country 
FROM neighbours join popularLanguage on neighbours.neighbor=popularLanguage.pais;

CREATE VIEW languages as 
SELECT countryLanguage.cname as c1name, neighbourLanguage.neighbor as c2id, neighbourLanguage.lname as lname 
FROM countryLanguage join neighbourLanguage on countryLanguage.neighbor=neighbourLanguage.neighbor 
WHERE countryLanguage.country=neighbourLanguage.country and countryLanguage.lname=neighbourLanguage.lname 
ORDER BY neighbourLanguage.lname ASC, countryLanguage.cname DESC;

INSERT INTO Query8 (SELECT c1name, cname as c2name, lname
FROM languages join a2.country on languages.c2id=country.cid);

DROP VIEW languages;
DROP VIEW neighbourLanguage;
DROP VIEW countryLanguage;
DROP VIEW neighbours;
DROP VIEW popularLanguage;

-- Query 9 statements                                                                                                                                                                

CREATE VIEW directAccessSpan as
SELECT cid, MAX(depth+height) as totalspan
FROM a2.oceanAccess natural join a2.country natural join a2.ocean
GROUP BY cid;

CREATE VIEW directAccessSpanNames as
SELECT cname, totalspan
FROM directAccessSpan join a2.country on directAccessSpan.cid=country.cid;

CREATE VIEW countriesSpan as
(SELECT * FROM directAccessSpanNames)
UNION
(SELECT cname, height as totalspan
FROM a2.country
WHERE cid NOT IN (SELECT cid FROM directAccessSpanNames));

INSERT INTO Query9 (SELECT cname, totalspan
FROM countriesSpan
WHERE totalspan >= ALL (SELECT totalSpan FROM countriesSpan));

DROP VIEW countriesSpan;
DROP VIEW directAccessSpanNames;
DROP VIEW directAccessSpan;

-- Query 10 statements 

CREATE VIEW totalBorders as
SELECT cname, sum(length) as borderslength
FROM a2.country join a2.neighbour on country.cid=neighbour.country
GROUP BY cname;

INSERT INTO Query10 (SELECT cname, borderslength
FROM totalBorders
WHERE borderslength >= ALL (SELECT borderslength FROM totalBorders));

DROP VIEW totalBorders;












