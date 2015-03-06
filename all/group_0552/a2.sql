-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE TABLE Query1(
	c1id	INTEGER,
    c1name	VARCHAR(20),
	c2id	INTEGER,
    c2name	VARCHAR(20)
);

CREATE view v_combin AS 
SELECT cid, cname, neighbor
FROM country, neighbour
WHERE country.cid = neighbour.country;

CREATE view v_combin2 AS 
SELECT v_combin.cid as c1id, v_combin.cname as c1name, v_combin.neighbor as c2id, country.cname as c2name, country.height as height
FROM v_combin, country
WHERE country.cid = v_combin.neighbor;

CREATE view v_maxHeight AS
SELECT v_combin2.c1id, max(height) as height
FROM v_combin2
GROUP BY v_combin2.c1id;

CREATE view v_query1
as
select v_combin2.c1id as c1id, v_combin2.c1name as c1name, v_combin2.c2id as c2id, v_combin2.c2name as c2name
FROM v_combin2 JOIN v_maxHeight ON v_combin2.c1id = v_maxHeight.c1id AND v_combin2.height = v_maxHeight.height
ORDER BY c1name ASC;

INSERT INTO Query1 (SELECT * from v_query1);

DROP VIEW IF EXISTS v_combin CASCADE;
DROP VIEW IF EXISTS v_combin2 CASCADE;
DROP VIEW IF EXISTS v_maxHeight CASCADE;
DROP VIEW IF EXISTS v_query1 CASCADE;

-- Query 2 statements
CREATE TABLE Query2(
	cid		INTEGER,
    cname	VARCHAR(20)
);

CREATE view v_noOceanAccess
as
(select cid from country) except (select cid from oceanAccess);

CREATE view v_query2
as
select country.cid, country.cname from v_noOceanAccess join country on v_noOceanAccess.cid=country.cid ORDER by cname ASC;

INSERT INTO Query2 (SELECT * FROM v_query2);

DROP VIEW IF EXISTS v_noOceanAccess CASCADE;
DROP VIEW IF EXISTS v_query2 CASCADE;

-- Query 3 statements
CREATE TABLE Query3(
	c1id	INTEGER,
    c1name	VARCHAR(20),
	c2id	INTEGER,
    c2name	VARCHAR(20)
);

CREATE view v_noOceanAccess2
as
(select cid from country) except (select cid from oceanAccess);

CREATE view v_landlockedSurByOne
as 
select * from v_noOceanAccess2 join neighbour on cid = country;

CREATE view v_lonelyCountries
as
select c1id, neighbor as c2id from (select cid as c1id from v_landlockedSurByOne group by c1id having count(cid)=1) m join neighbour on m.c1id = neighbour.country;

CREATE view v_addNameToID
as
select c1id, cname as c1name, c2id, c2name from 
(select c1id, c2id, cname as c2name from country join v_lonelyCountries on c2id = cid) m join country on m.c1id = cid ORDER by c1name ASC; 

CREATE view v_query3
as select * from v_addNameToID;

INSERT INTO Query3 (SELECT * FROM v_query3);

DROP VIEW IF EXISTS v_noOceanAccess2 CASCADE;
DROP VIEW IF EXISTS v_landlockedSurByOne CASCADE;
DROP VIEW IF EXISTS v_lonelyCountries CASCADE;
DROP VIEW IF EXISTS v_addNameToID CASCADE;
DROP VIEW IF EXISTS v_query3 CASCADE;

-- Query 4 statements
CREATE TABLE Query4(
	cname	VARCHAR(20),
    oname	VARCHAR(20)
);

CREATE view v_countryDirectOceanAccess
as
select m.cid as cid, m.cname as cname, m.oid as oid, ocean.oname as oname from ocean join (select country.cid,country.cname,oceanAccess.oid from country join oceanAccess on country.cid=oceanAccess.cid)m on m.oid = ocean.oid;

CREATE view v_countryNeighbour
as
select cid, cname, neighbor from country join neighbour on cid=country;

CREATE view v_neighbourWithDirectOceanAccess
as
select m.cid as cid, m.cname as cname, ocean.oid as oid, ocean.oname as oname from ocean join (
select v_countryNeighbour.cid as cid,v_countryNeighbour.cname as cname,v_countryNeighbour.neighbor
,oceanAccess.oid as rename_oid
from v_countryNeighbour join oceanAccess on v_countryNeighbour.neighbor=oceanAccess.cid) m on ocean.oid = m.rename_oid;

-- Remove duplicates
CREATE view v_query4
as
select cname, oname from (
select  cid, cname, oid, oname from v_neighbourWithDirectOceanAccess UNION select * from v_countryDirectOceanAccess) m ORDER by cname ASC, oname DESC;

INSERT INTO Query4 (SELECT * FROM v_query4);

DROP VIEW IF EXISTS v_countryDirectOceanAccess CASCADE;
DROP VIEW IF EXISTS v_countryNeighbour CASCADE;
DROP VIEW IF EXISTS v_neighbourWithDirectOceanAccess CASCADE;
DROP VIEW IF EXISTS v_query4 CASCADE;

-- Query 5 statements
CREATE TABLE Query5(
	cid		INTEGER,
    cname	VARCHAR(20),
	avghdi	REAL
);

CREATE view v_2009_2013
as
select cid, avg(hdi_score) as avghdi from  hdi where year BETWEEN 2009 AND 2013 group by cid;

CREATE view v_query5
as
select country.cid as cid, country.cname as cname, avghdi from country join v_2009_2013 on country.cid = v_2009_2013.cid order by avghdi desc limit 10;

INSERT INTO Query5 (SELECT * FROM v_query5);

DROP VIEW IF EXISTS v_2009_2013 CASCADE;
DROP VIEW IF EXISTS v_query5 CASCADE;

-- Query6
CREATE TABLE Query6(
	cid		INTEGER,
    cname	VARCHAR(20)
);

CREATE VIEW v_2009 as
SELECT country.cid as cid, country.cname as cname, hdi_score
FROM country JOIN hdi ON country.cid = hdi.cid
WHERE year=2009;

CREATE VIEW v_2010 as
SELECT country.cid as cid, country.cname as cname, hdi_score
FROM country JOIN hdi ON country.cid = hdi.cid
WHERE year=2010;

CREATE VIEW v_2011 as
SELECT country.cid as cid, country.cname as cname, hdi_score
FROM country JOIN hdi ON country.cid = hdi.cid
WHERE year=2011;

CREATE VIEW v_2012 as
SELECT country.cid as cid, country.cname as cname, hdi_score
FROM country JOIN hdi ON country.cid = hdi.cid
WHERE year=2012;

CREATE VIEW v_2013 as
SELECT country.cid as cid, country.cname as cname, hdi_score
FROM country JOIN hdi ON country.cid = hdi.cid
WHERE year=2013;

CREATE VIEW v_query6 as
SELECT v_2009.cid as cid, v_2009.cname as cname
FROM ((((v_2009 JOIN v_2010 ON v_2009.cid = v_2010.cid) 
	JOIN v_2011 ON v_2009.cid = v_2011.cid AND v_2010.cid = v_2011.cid) 
	JOIN v_2012 ON v_2009.cid = v_2012.cid AND v_2010.cid = v_2012.cid AND v_2011.cid = v_2012.cid)
	JOIN v_2013 ON v_2009.cid = v_2013.cid AND v_2010.cid = v_2013.cid AND v_2011.cid = v_2013.cid AND v_2012.cid = v_2013.cid)
	
	WHERE ((v_2009.cid = v_2010.cid AND v_2009.cid = v_2011.cid AND v_2009.cid = v_2012.cid AND v_2009.cid = v_2013.cid) AND
	(v_2010.cid = v_2011.cid AND v_2010.cid = v_2012.cid AND v_2010.cid = v_2013.cid) AND
	(v_2011.cid = v_2012.cid AND v_2011.cid = v_2013.cid) AND
	(v_2012.cid = v_2013.cid))
	AND
	((v_2013.hdi_score > v_2012.hdi_score AND v_2013.hdi_score > v_2011.hdi_score AND v_2013.hdi_score > v_2010.hdi_score AND v_2013.hdi_score > v_2009.hdi_score) AND
	(v_2012.hdi_score > v_2011.hdi_score AND v_2012.hdi_score > v_2010.hdi_score AND v_2012.hdi_score > v_2009.hdi_score) AND
	(v_2011.hdi_score > v_2010.hdi_score AND v_2011.hdi_score > v_2009.hdi_score) AND
	(v_2010.hdi_score > v_2009.hdi_score))
	ORDER BY cname ASC;

INSERT INTO Query6 (SELECT * FROM v_query6);

DROP VIEW IF EXISTS v_2009 CASCADE;
DROP VIEW IF EXISTS v_2010 CASCADE;
DROP VIEW IF EXISTS v_2011 CASCADE;
DROP VIEW IF EXISTS v_2012 CASCADE;
DROP VIEW IF EXISTS v_2013 CASCADE;
DROP VIEW IF EXISTS v_query6 CASCADE;

-- Query7
CREATE TABLE Query7(
	rid			INTEGER,
    rname		VARCHAR(20),
	followers	INTEGER
);

CREATE view v_followerPerCountry
as
select country.cid, country.population, religion.rid as rid, religion.rname as rname, religion.rpercentage, religion.rpercentage * country.population as followersPerCountry 
from country join religion on country.cid=religion.cid;

CREATE view v_query7
as
select rid, rname, sum(followersPerCountry) as followers
from v_followerPerCountry group by rid, rname order by followers DESC;

INSERT INTO Query7 (SELECT * FROM v_query7);

DROP VIEW IF EXISTS v_followerPerCountry CASCADE;
DROP VIEW IF EXISTS v_query7 CASCADE;

-- Query8
CREATE TABLE Query8(
	c1name	VARCHAR(20),
    c2name	VARCHAR(20),
	lname	VARCHAR(20)
);

-- Get a table with each country and its most popular language then join with neighbours and compare
CREATE view v_countryLanguagesPopularity
as
select country.cid, cname, lname, population * lpercentage as LanguagePopularity from country join language on country.cid = language.cid;

CREATE view v_countryMostPopularLanguage
as
select v_countryLanguagesPopularity.cid, cname, lname, mostPopularLanguage from v_countryLanguagesPopularity join 
(select cid, max(LanguagePopularity) as mostPopularLanguage from v_countryLanguagesPopularity group by cid) m on v_countryLanguagesPopularity.cid = m.cid;

CREATE view v_countryJoinNeighbour1
as
select cid as c1id, cname as c1name, lname, mostPopularLanguage, neighbor 
from v_countryMostPopularLanguage join neighbour on v_countryMostPopularLanguage.cid = neighbour.country;

CREATE view v_countryJoinNeighbour2
as
select c1id, c1name, v_countryMostPopularLanguage.lname as lname,  cid as c2id, cname as c2name
from v_countryJoinNeighbour1 join v_countryMostPopularLanguage 
on v_countryJoinNeighbour1.neighbor=v_countryMostPopularLanguage.cid and v_countryJoinNeighbour1.lname=v_countryMostPopularLanguage.lname;

CREATE view v_query8
as
SELECT c1name, c2name, lname
FROM v_countryJoinNeighbour2
ORDER BY lname ASC, c1name DESC;

INSERT INTO Query8 (SELECT * FROM v_query8);

DROP VIEW IF EXISTS v_countryLanguagesPopularity CASCADE;
DROP VIEW IF EXISTS v_countryMostPopularLanguage CASCADE;
DROP VIEW IF EXISTS v_countryJoinNeighbour1 CASCADE;
DROP VIEW IF EXISTS  v_countryJoinNeighbour2 CASCADE;
DROP VIEW IF EXISTS v_query8 CASCADE;

-- Query9
CREATE TABLE Query9(
    cname		VARCHAR(20),
	totalspan	INTEGER
);

CREATE view v_DeepestOceanDirectAccess
as
SELECT cid, ocean.oid as oid, depth
FROM ocean JOIN oceanAccess ON ocean.oid = oceanAccess.oid;

CREATE view v_NoDirectAccess
as
(select cid from country) except (select cid from oceanAccess);

CREATE view v_DiffWithOcean
as
SELECT country.cid as cid, (country.height + v_DeepestOceanDirectAccess.depth) as totalspan
FROM country JOIN v_DeepestOceanDirectAccess on country.cid = v_DeepestOceanDirectAccess.cid;

CREATE view v_DiffWithSelf
as
SELECT country.cid as cid, country.height as totalspan
FROM country JOIN v_NoDirectAccess ON country.cid = v_NoDirectAccess.cid;

CREATE view v_combined
as
select * from v_DiffWithSelf UNION select * from v_DiffWithOcean;

CREATE view v_maxdiff
as
SELECT max(totalspan) as maxtotalspan
FROM v_combined;

CREATE view v_query9
as
SELECT country.cname as cname, v_maxdiff.maxtotalspan as totalspan
FROM country, v_combined, v_maxdiff
WHERE country.cid = v_combined.cid AND v_combined.totalspan = v_maxdiff.maxtotalspan;

INSERT INTO Query9 (SELECT * FROM v_query9);

DROP VIEW IF EXISTS v_DeepestOceanDirectAccess CASCADE;
DROP VIEW IF EXISTS v_NoDirectAccess CASCADE;
DROP VIEW IF EXISTS v_DiffWithOcean CASCADE;
DROP VIEW IF EXISTS v_DiffWithSelf CASCADE;
DROP VIEW IF EXISTS v_combined CASCADE;
DROP VIEW IF EXISTS v_maxdiff CASCADE;
DROP VIEW IF EXISTS v_query9 CASCADE;

-- Query10
CREATE TABLE Query10(
    cname			VARCHAR(20),
	borderslength	INTEGER
);

CREATE view v_countryWithNeighbors
as
select * from country join neighbour on country.cid = neighbour.country;

CREATE view v_countryTotalBorderLength
as
select cname, sum(length) as sumTotalBorderLength from v_countryWithNeighbors group by cname order by cname asc;

CREATE view v_maxTotalBorderLength
as
select max(sumTotalBorderLength) as borderslength from v_countryTotalBorderLength;

CREATE view v_query10
as 
select v_countryTotalBorderLength.cname as cname, borderslength from v_maxTotalBorderLength join v_countryTotalBorderLength on v_countryTotalBorderLength.sumTotalBorderLength = borderslength;

INSERT INTO Query10 (SELECT * FROM v_query10);

DROP VIEW IF EXISTS v_countryWithNeighbors CASCADE;
DROP VIEW IF EXISTS v_countryTotalBorderLength CASCADE;
DROP VIEW IF EXISTS v_maxTotalBorderLength CASCADE;
DROP VIEW IF EXISTS v_query10 CASCADE;
