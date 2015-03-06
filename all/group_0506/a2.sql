-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
--all neighbour pair infos
CREATE VIEW NeighbourInfo AS
SELECT c1.cid As c1id, c1.cname As c1name, c2.cid As c2id, c2.cname As c2name, c2.height As c2height
FROM country c1, country c2, neighbour n
WHERE c1.cid = n.country And c2.cid = n.neighbor;

--all pairs where neighbor country does not have highest elevation point
CREATE VIEW NotHighestNeighbor AS
SELECT n1.c1id As c1id, n1.c1name As c1name, n1.c2id As c2id, n1.c2name As c2name
FROM NeighbourInfo n1, NeighbourInfo n2
WHERE n1.c1id = n2.c1id And n1.c2id <> n2.c2id And n1.c2height < n2.c2height;

INSERT INTO Query1 (
(SELECT c1id, c1name, c2id, c2name
FROM NeighbourInfo)
EXCEPT
(SELECT c1id, c1name, c2id, c2name
FROM NotHighestNeighbor)
ORDER BY c1name ASC);

DROP VIEW NotHighestNeighbor;
DROP VIEW NeighbourInfo;


-- Query 2 statements

--landlocked country = country with no ocean access
INSERT INTO Query2 (
(SELECT cid, cname
FROM country)
EXCEPT
(SELECT DISTINCT c.cid, cname
FROM oceanAccess o, country c
WHERE o.cid = c.cid)
ORDER BY cname ASC);

-- Query 3 statements
--view of land locked countries
CREATE VIEW LandLockedCountry AS
(SELECT cid, cname
FROM country)
EXCEPT
(SELECT DISTINCT c.cid, cname
FROM oceanAccess o, country c
WHERE o.cid = c.cid);

CREATE VIEW LandLockedNeighbours AS
SELECT l.cid As c1id, l.cname As c1name, c.cid As c2id, c.cname As c2name
FROM LandLockedCountry l, country c, neighbour n
WHERE l.cid = n.country And c.cid = n.neighbor;


CREATE VIEW MoreThanOneSurrounding AS
SELECT DISTINCT l1.c1id, l1.c1name, l1.c2id, l1.c2name
FROM LandLockedNeighbours l1, LandLockedNeighbours l2
WHERE l1.c1id = l2.c1id And l1.c2id <> l2.c2id;

INSERT INTO Query3 (
(SELECT *
FROM LandLockedNeighbours)
EXCEPT
(SELECT *
FROM MoreThanOneSurrounding));

DROP VIEW MoreThanOneSurrounding;
DROP VIEW LandLockedNeighbours;
DROP VIEW LandLockedCountry;


-- Query 4 statements
CREATE VIEW DirectAccess AS
SELECT c.cname As cname, c.cid As cid, o.oname As oname
FROM oceanAccess oa, country c, ocean o
WHERE oa.cid = c.cid And oa.oid = o.oid;

CREATE VIEW IndirectAccess AS
SELECT c.cname, d.oname
FROM neighbour n, country c, DirectAccess d
WHERE n.country = c.cid And n.neighbor = d.cid;

INSERT INTO Query4 ( 
(SELECT cname, oname
FROM DirectAccess)
UNION 
(SELECT *
FROM IndirectAccess));

DROP VIEW IndirectAccess;
DROP VIEW DirectAccess;



-- Query 5 statements

INSERT INTO Query5 (
(SELECT h.cid As cid, c.cname As cname, AVG(h.hdi_score) as avghdi
FROM hdi h, country c
WHERE h.cid = c.cid And h.year <= '2013' and h.year >= '2009'
GROUP BY h.cid, c.cname
ORDER BY avghdi DESC
LIMIT 10));	

-- Query 6 statements
CREATE VIEW HDI2009 AS
SELECT cid, hdi_score
FROM hdi h
WHERE h.year ='2009';

CREATE VIEW HDI2010 AS
SELECT cid, hdi_score
FROM hdi h
WHERE h.year ='2010';


CREATE VIEW HDI2011 AS
SELECT cid, hdi_score
FROM hdi h
WHERE h.year ='2011';


CREATE VIEW HDI2012 AS
SELECT cid, hdi_score
FROM hdi h
WHERE h.year ='2012';


CREATE VIEW HDI2013 AS
SELECT cid, hdi_score
FROM hdi h
WHERE h.year ='2013';

INSERT INTO Query6 
(
SELECT h09.cid, c.cname
FROM HDI2009 h09, HDI2010 h10, HDI2011 h11, HDI2012 h12, HDI2013 h13, country c
WHERE h09.cid = c.cid And h09.cid = h10.cid And h09.cid = h11.cid And h09.cid = h12.cid And h09.cid = h13.cid And h09.hdi_score < h10.hdi_score And h10.hdi_score < h11.hdi_score And h11.hdi_score < h12.hdi_score And h12.hdi_score < h13.hdi_score
);

DROP VIEW HDI2009;
DROP VIEW HDI2010;
DROP VIEW HDI2011;
DROP VIEW HDI2012;
DROP VIEW HDI2013;



-- Query 7 statements

CREATE VIEW RPopPerCountry As
(SELECT r.rid As rid, r.rname As rname, r.cid As cid, (r.rpercentage*c.population) As followers
FROM religion r, country c
WHERE r.cid=c.cid);

INSERT INTO Query7
(
SELECT rid, rname, SUM(followers) As followers
FROM RPopPerCountry
GROUP BY rid, rname
);

DROP VIEW RPopPerCountry;


-- Query 8 statements

CREATE VIEW CountryLanguage AS
SELECT c.cid As cid, c.cname As cname, l.lid As lid, l.lname As lname, l.lpercentage As lpercentage
FROM country c, language l
WHERE c.cid = l.cid;

CREATE VIEW CountryNotMostPopLanguage AS
SELECT c1.cid As cid, c1.cname As cname, c1.lid As lid, c1.lname As lname
FROM CountryLanguage c1, CountryLanguage c2
WHERE c1.cid = c2.cid And c1.lid <> c2.lid And c1.lpercentage < c2.lpercentage;

CREATE VIEW CountryMostPopLanguage AS
(
SELECT cid, cname, lid, lname
FROM CountryLanguage
EXCEPT
SELECT cid, cname, lid, lname
FROM CountryNotMostPopLanguage
);


INSERT INTO Query8 
(
SELECT c1.cname As c1name, c2.cname As c2name, c1.lname As lname
FROM neighbour n, CountryMostPopLanguage c1, CountryMostPopLanguage c2
WHERE n.country = c1.cid And n.neighbor = c2.cid And c1.lid = c2.lid
);

DROP VIEW CountryMostPopLanguage;
DROP VIEW CountryNotMostPopLanguage;
DROP VIEW CountryLanguage;


-- Query 9 statements


CREATE VIEW CountriesWithOceanAccess AS
SELECT c.cid As cid, c.cname As cname, c.height As height, o.oid As oid, o.depth As depth
FROM country c, oceanAccess oa, ocean o
WHERE oa.cid = c.cid And oa.oid = o.oid;

CREATE VIEW NotDeepestOceanAccess AS
SELECT c1.cid As cid, c1.cname As cname, c1.height As height, c1.oid As oid, c1.depth As depth
FROM CountriesWithOceanAccess c1, CountriesWithOceanAccess c2
WHERE c1.cname = c2.cname And c1.oid <> c2.oid And c1.depth < c2.depth;

CREATE VIEW DeepestOceanAccess AS
(SELECT *
FROM CountriesWithOceanAccess)
EXCEPT
(SELECT *
FROM NotDeepestOceanAccess);


CREATE VIEW CountriesWithoutOceanAccess AS
(SELECT DISTINCT cid, cname, height
FROM country)
EXCEPT
(
SELECT DISTINCT cid, cname, height 
FROM CountriesWithOceanAccess
);


INSERT INTO Query9 (
(
SELECT cname, (height+depth) As totalspan
FROM DeepestOceanAccess
)
UNION
(
SELECT cname, height As totalspan
FROM CountriesWithoutOceanAccess
)
);

DROP VIEW CountriesWithoutOceanAccess;
DROP VIEW DeepestOceanAccess;
DROP VIEW NotDeepestOceanAccess;
DROP VIEW CountriesWithOceanAccess;


-- Query 10 statements
INSERT INTO Query10
(
SELECT c.cname As cname ,SUM(n.length) As borderslength
FROM neighbour n, country c
WHERE n.country = c.cid 
GROUP BY cname
ORDER BY borderslength DESC
LIMIT 1
);

