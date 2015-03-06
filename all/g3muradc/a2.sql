-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

set search_path to a2;

-- Query 1 statements

CREATE VIEW joined AS
SELECT cid, cname, height, country, neighbor
FROM country, neighbour
WHERE cid = country
ORDER BY cid ASC;

CREATE VIEW selected AS
SELECT a.cid AS c1id, a.cname AS c1name, a.height AS c1height, b.cid AS c2id, b.cname AS  c2name, b.height AS c2height
FROM joined a, joined b 
WHERE a.neighbor = b.cid and b.neighbor = a.cid 
ORDER BY c1id ASC;

CREATE VIEW maxheights AS
SELECT c1name, MAX(c2height) AS maxheight
FROM selected
GROUP by c1name; 

INSERT INTO Query1 (
SELECT DISTINCT selected.c1id AS c1id, selected.c1name AS c1name, selected.c2id AS c2id, selected.c2name AS c2name
FROM maxheights, selected
WHERE selected.c1name = maxheights.c1name AND maxheight = c2height
ORDER BY c1name ASC);

DROP VIEW maxheights;
DROP VIEW selected;
DROP VIEW joined;

-- Query 2 statements

CREATE VIEW landlocked AS
(SELECT country.cid AS cid
FROM country)
EXCEPT
(SELECT cid 
FROM oceanAccess);

INSERT INTO Query2 (
SELECT landlocked.cid AS cid, country.cname AS cname
FROM landlocked, country
WHERE landlocked.cid = country.cid
ORDER BY cname ASC);

DROP VIEW landlocked;

-- Query 3 statements

CREATE VIEW landlocked AS
(SELECT country.cid AS cid
FROM country)
EXCEPT
(SELECT cid 
FROM oceanAccess);

CREATE VIEW counted AS
SELECT cid, COUNT(cid) AS count
FROM landlocked, neighbour
WHERE landlocked.cid = neighbour.country 
GROUP BY cid;

CREATE VIEW neighbours AS
SELECT neighbour.country AS c1id, neighbour.neighbor AS neighbor
FROM counted, neighbour
WHERE counted.count = 1 and neighbour.country = counted.cid;

CREATE VIEW countrywithname AS
SELECT neighbours.c1id AS c1id, country.cname AS c1name
FROM neighbours, country
WHERE neighbours.c1id = country.cid;

CREATE VIEW neighbourwithname AS
SELECT neighbours.neighbor AS c2id, country.cname AS c2name
FROM neighbours, country
WHERE neighbours.neighbor = country.cid;

INSERT INTO Query3 (
SELECT c1id, c1name, c2id, c2name
FROM countrywithname, neighbourwithname, neighbour
WHERE c1id = country AND c2id = neighbor
ORDER BY c1name ASC);

DROP VIEW neighbourwithname;
DROP VIEW countrywithname;
DROP VIEW neighbours;
DROP VIEW counted;
DROP VIEW landlocked;

-- Query 4 statements

CREATE VIEW neighbouringaccessible AS 
SELECT neighbour.country AS cid, neighbour.neighbor AS neighbor, oceanAccess.oid AS oid
FROM neighbour, oceanAccess 
WHERE neighbour.neighbor = oceanAccess.cid;

CREATE VIEW joined AS
(SELECT cid, oid
FROM neighbouringaccessible)
UNION 
(SELECT cid, oid
FROM oceanAccess);

INSERT INTO Query4 (
SELECT country.cname, ocean.oname
FROM country, ocean, joined
WHERE country.cid = joined.cid AND ocean.oid = joined.oid
ORDER BY cname ASC, oname DESC);

DROP VIEW joined;
DROP VIEW neighbouringaccessible;

-- Query 5 statements

CREATE VIEW fiveyears AS
SELECT * 
FROM hdi
WHERE year >= 2009 AND year <= 2013;

CREATE VIEW average AS
SELECT cid, avg(hdi_score) AS avghdi
FROM fiveyears
GROUP BY cid;

INSERT INTO Query5 (
SELECT country.cid AS cid, country.cname AS cname, average.avghdi AS avghdi
FROM country, average 
WHERE country.cid = average.cid 
ORDER BY avghdi DESC
LIMIT 10);

DROP VIEW average;
DROP VIEW fiveyears;

-- Query 6 statements

CREATE VIEW hdi2009 AS
SELECT cid, hdi_score 
FROM hdi
WHERE year = 2009
ORDER BY cid ASC, year ASC;

CREATE VIEW hdi2010 AS
SELECT cid, hdi_score 
FROM hdi
WHERE year = 2010
ORDER BY cid ASC, year ASC;

CREATE VIEW hdi2011 AS
SELECT cid, hdi_score 
FROM hdi
WHERE year = 2011
ORDER BY cid ASC, year ASC;

CREATE VIEW hdi2012 AS
SELECT cid, hdi_score 
FROM hdi
WHERE year = 2012
ORDER BY cid ASC, year ASC;

CREATE VIEW hdi2013 AS
SELECT cid, hdi_score 
FROM hdi
WHERE year = 2013
ORDER BY cid ASC, year ASC;

CREATE VIEW increasing AS
SELECT a.cid AS cid
FROM hdi2009 a, hdi2010 b, hdi2011 c, hdi2012 d, hdi2013 e
WHERE a.hdi_score < b.hdi_score AND b.hdi_score < c.hdi_score AND c.hdi_score < d.hdi_score AND d.hdi_score < e.hdi_score 
AND a.cid = b.cid AND b.cid = c.cid AND c.cid = d.cid AND d.cid = e.cid;

INSERT INTO Query6 (
SELECT country.cid, country.cname
FROM increasing, country
WHERE country.cid = increasing.cid);


DROP VIEW increasing;
DROP VIEW hdi2013;
DROP VIEW hdi2012;
DROP VIEW hdi2011; 
DROP VIEW hdi2010;
DROP VIEW hdi2009;

-- Query 7 statements

CREATE VIEW followers AS
SELECT religion.cid AS cid, religion.rid AS rid, religion.rname AS rname, country.population * religion.rpercentage AS following
FROM religion, country
WHERE religion.cid = country.cid;

INSERT INTO Query7 (
SELECT rid, rname, SUM(following) AS followers 
FROM followers
GROUP BY rid, rname
ORDER BY followers DESC);

DROP VIEW followers;

-- Query 8 statements

CREATE VIEW languages AS
SELECT language.cid AS cid, language.lid AS lid, language.lname AS lname, country.population * language.lpercentage AS amountspeaking
FROM language, country
WHERE language.cid = country.cid;

CREATE VIEW mostpopular AS
SELECT cid, MAX(amountspeaking) as mostpopular
FROM languages
GROUP BY cid;

CREATE VIEW mostpopularlanguages AS
SELECT mostpopular.cid, lname, mostpopular
FROM mostpopular, languages
WHERE languages.amountspeaking = mostpopular.mostpopular;

CREATE VIEW pairs AS
SELECT a.cid AS c1id, b.cid AS c2id, a.lname AS lname
FROM mostpopularlanguages a, mostpopularlanguages b, neighbour
WHERE a.lname = b.lname AND a.cid = neighbour.country AND b.cid = neighbour.neighbor;

CREATE VIEW c1withname AS
SELECT country.cid AS c1id, country.cname AS c1name
FROM pairs, country
WHERE country.cid = pairs.c1id;

CREATE VIEW c2withname AS
SELECT country.cid AS c2id, country.cname AS c2name
FROM pairs, country
WHERE country.cid = pairs.c2id;

INSERT INTO Query8 (
SELECT DISTINCT c1withname.c1name, c2withname.c2name, pairs.lname
FROM c1withname, c2withname, pairs
WHERE c1withname.c1id = pairs.c1id AND c2withname.c2id = pairs.c2id
ORDER BY lname ASC, c1name DESC);

DROP VIEW c2withname;
DROP VIEW c1withname;
DROP VIEW pairs;
DROP VIEW mostpopularlanguages;
DROP VIEW mostpopular;
DROP VIEW languages;

-- Query 9 statements

CREATE VIEW differences AS
SELECT country.cname, ocean.oname, ABS(country.height + ocean.depth) AS difference
FROM oceanAccess, ocean, country
WHERE oceanAccess.cid = country.cid AND ocean.oid = oceanAccess.oid
ORDER BY country.cname;

CREATE VIEW maximum AS
SELECT MAX(difference) AS maxspan
FROM differences;

INSERT INTO Query9 (
SELECT cname, difference AS totalspan
FROM differences, maximum
WHERE maxspan = difference);

DROP VIEW maximum;
DROP VIEW differences;

-- Query 10 statements

CREATE VIEW bordersum AS
SELECT country, SUM(length) AS borderslength
FROM neighbour
GROUP BY country;

CREATE VIEW maximum AS
SELECT MAX(borderslength) AS borderslength
FROM bordersum;

INSERT INTO Query10 (
SELECT country.cname, maximum.borderslength
FROM maximum, country, bordersum
WHERE country.cid = bordersum.country AND maximum.borderslength = bordersum.borderslength);

DROP VIEW maximum;
DROP VIEW bordersum;


