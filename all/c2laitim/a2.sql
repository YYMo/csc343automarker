-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW highestelevationneighbour AS 
SELECT c1.cid, c1.cname, MAX(c2.height) AS maxheight
FROM country c1, country c2, neighbour
WHERE c1.cid=neighbour.country AND c2.cid=neighbour.neighbor
GROUP BY c1.cid;

INSERT INTO QUERY1(SELECT h.cid AS c1id, h.cname AS c1name, c.cid AS c2id,
c.cname AS c2name
FROM highestelevationneighbour h, country c, neighbour n
WHERE h.cid = n.country AND c.cid = n.neighbor AND h.maxheight = c.height
ORDER BY c1name ASC);

DROP VIEW IF EXISTS highestelevationneighbour CASCADE;

-- Query 2 statements

INSERT INTO QUERY2(SELECT cid, cname 
FROM country
WHERE NOT EXISTS 
   (SELECT o.cid
   FROM oceanAccess o
   WHERE country.cid = o.cid)
ORDER BY cname ASC);

-- Query 3 statements

CREATE VIEW landlockedcountry AS
SELECT cid
FROM country
WHERE NOT EXISTS
   (SELECT o.cid
   FROM oceanAccess o
   WHERE country.cid = o.cid);

CREATE VIEW countneighbour AS
SELECT l.cid, COUNT(n.neighbor) AS numneighbours
FROM landlockedcountry l,neighbour n
WHERE l.cid = n.country 
GROUP BY l.cid;

CREATE VIEW getcid AS
SELECT n.country, n.neighbor
FROM countneighbour cn, neighbour n
WHERE cn.numneighbours = 1 AND cn.cid = n.country;

INSERT INTO QUERY3(SELECT c1.cid AS c1id, c1.cname AS c1name,
c2.cid AS c2id, c2.cname AS c2name
FROM getcid, country c1, country c2
WHERE country = c1.cid AND neighbor = c2.cid
ORDER BY c1name ASC);

DROP VIEW IF EXISTS landlockedcountry CASCADE;
DROP VIEW IF EXISTS countneighbour CASCADE;
DROP VIEW IF EXISTS getcid CASCADE;

-- Query 4 statements

--find all countries wit coastline(direct)
CREATE VIEW direct AS
SELECT cid, oid
FROM oceanAccess;

--all countries with coastline neighbours should be included(indirect)
CREATE VIEW indirect AS
SELECT neighbour.neighbor, direct.oid
FROM direct, neighbour
WHERE direct.cid = neighbour.country;

--combine
CREATE VIEW cidoid AS
(SELECT *
FROM direct)
union
(SELECT neighbor AS cid, oid
FROM indirect);

--fix it up (get cname and oname)
INSERT INTO QUERY4(SELECT country.cname, ocean.oname
FROM cidoid, country, ocean
WHERE cidoid.cid = country.cid AND cidoid.oid = ocean.oid
ORDER BY cname ASC, oname DESC);

DROP VIEW IF EXISTS direct CASCADE;
DROP VIEW IF EXISTS indirect CASCADE;
DROP VIEW IF EXISTS cidoid CASCADE;

-- Query 5 statements

INSERT INTO QUERY5(SELECT country.cid, country.cname, SUM(hdi.hdi_score)/5 AS avghdi
FROM country, hdi
WHERE country.cid = hdi.cid AND hdi.year >= 2009 AND hdi.year <= 2013
GROUP BY country.cid 
ORDER BY avghdi DESC LIMIT 10);

-- Query 6 statements

-- 2010>2009
CREATE VIEW increasing2010 AS
SELECT h2.cid 
FROM hdi h1, hdi h2
WHERE h1.year = 2009 AND h2.year = 2010 AND h1.cid = h2.cid 
AND h2.hdi_score > h1.hdi_score;

-- 2011 > 2010
CREATE VIEW increasing2011 AS
SELECT h2.cid 
FROM hdi h1, hdi h2
WHERE h1.year = 2010 AND h2.year = 2011 AND h1.cid = h2.cid 
AND h2.hdi_score > h1.hdi_score;

-- 2012 > 2011
CREATE VIEW increasing2012 AS
SELECT h2.cid 
FROM hdi h1, hdi h2
WHERE h1.year = 2011 AND h2.year = 2012 AND h1.cid = h2.cid 
AND h2.hdi_score > h1.hdi_score;

-- 2013 > 2012
CREATE VIEW increasing2013 AS
SELECT h2.cid 
FROM hdi h1, hdi h2
WHERE h1.year = 2012 AND h2.year = 2013 AND h1.cid = h2.cid 
AND h2.hdi_score > h1.hdi_score;

-- All views together to get 5 year increasing hdi cid's
CREATE VIEW increasingallcid AS
SELECT i10.cid
FROM increasing2010 i10, increasing2011 i11, increasing2012 i12, 
increasing2013 i13
WHERE i10.cid = i11.cid AND i10.cid = i12.cid AND i10.cid = i13.cid;

-- We have all cids. Now find name and order by asc
INSERT INTO QUERY6(SELECT country.cid, country.cname
FROM increasingallcid, country
WHERE increasingallcid.cid = country.cid
ORDER BY country.cname ASC);

DROP VIEW IF EXISTS increasing2010 CASCADE;
DROP VIEW IF EXISTS increasing2011 CASCADE;
DROP VIEW IF EXISTS increasing2012 CASCADE;
DROP VIEW IF EXISTS increasing2013 CASCADE;
DROP VIEW IF EXISTS increasingallcid CASCADE;

-- Query 7 statements

CREATE VIEW ridfollowers AS
SELECT religion.rid,
SUM(country.population * religion.rpercentage/100) AS followers
FROM country, religion
WHERE country.cid = religion.cid
GROUP BY religion.rid;

INSERT INTO QUERY7(SELECT DISTINCT religion.rid, religion.rname, followers
FROM ridfollowers, religion
WHERE ridfollowers.rid = religion.rid
ORDER BY followers DESC);

DROP VIEW IF EXISTS ridfollowers CASCADE;

-- Query 8 statements

--most popular language from country

CREATE VIEW highestpercent AS
SELECT country.cid, MAX(lpercentage) AS highestpercent
FROM country, language
WHERE country.cid = language.cid
GROUP BY country.cid;

CREATE VIEW mostpopularlanguage AS
SELECT l.cid, l.lid, l.lpercentage
FROM highestpercent hp, language l
WHERE hp.cid = l.cid AND hp.highestpercent = l.lpercentage;

--now that we have cid with most spoken lid and most spoken percent
-- find neighbours with alike
CREATE VIEW cidneighbour AS
SELECT mpl1.cid as c1cid, mpl2.cid as c2cid, mpl1.lid
FROM mostpopularlanguage mpl1, neighbour, mostpopularlanguage mpl2
WHERE mpl1.cid = neighbour.country AND mpl2.cid = neighbour.neighbor
AND mpl1.lid = mpl2.lid;

INSERT INTO QUERY8(SELECT DISTINCT c1.cname AS c1name, c2.cname AS c2name, language.lname
FROM cidneighbour, country c1, country c2, language
WHERE c1cid = c1.cid AND c2cid = c2.cid AND cidneighbour.lid = language.lid
ORDER BY lname ASC, c1name DESC);

DROP VIEW IF EXISTS highestpercent CASCADE;
DROP VIEW IF EXISTS mostpopularlanguage CASCADE;
DROP VIEW IF EXISTS cidneighbour CASCADE;

-- Query 9 statements
CREATE VIEW cidoceandepth AS
SELECT oceanAccess.cid, MAX(ocean.depth) AS oceandepth
FROM oceanAccess, ocean
WHERE oceanAccess.oid = ocean.oid
GROUP BY oceanAccess.cid;

CREATE VIEW cidnoocean AS
SELECT cid, 0 AS oceandepth
FROM country
WHERE NOT EXISTS 
   (SELECT o.cid
   FROM oceanAccess o
   WHERE country.cid = o.cid);

CREATE VIEW allcidoceandepth AS
SELECT * FROM cidoceandepth 
UNION 
SELECT * FROM cidnoocean;

INSERT INTO QUERY9(SELECT country.cname, (height + oceandepth) AS totalspan
FROM allcidoceandepth, country
WHERE allcidoceandepth.cid = country.cid
ORDER BY totalspan DESC LIMIT 1);

DROP VIEW IF EXISTS cidoceandepth CASCADE;
DROP VIEW IF EXISTS cidnoocean CASCADE;
DROP VIEW IF EXISTS allcidoceandepth CASCADE;

-- Query 10 statements

CREATE VIEW cidlength AS
SELECT neighbour.country, SUM(neighbour.length) AS borderslength
FROM neighbour
GROUP BY neighbour.country;

CREATE VIEW maxlength AS
SELECT MAX(cidlength.borderslength) AS borderslength
FROM cidlength, country
WHERE cidlength.country = country.cid;

INSERT INTO QUERY10(SELECT DISTINCT country.cname, maxlength.borderslength
FROM maxlength, cidlength, country
WHERE maxlength.borderslength = cidlength.borderslength AND 
cidlength.country = country.cid);

DROP VIEW IF EXISTS cidlength CASCADE;
DROP VIEW IF EXISTS maxlength CASCADE;

