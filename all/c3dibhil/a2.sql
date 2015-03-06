-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW info AS 
SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, 
			c2.cname as c2name, c2.height as neighbor_height
FROM neighbour, country as c1, country as c2
WHERE c1.cid = country AND c2.cid = neighbor;

CREATE VIEW highest_neighbor as
SELECT c1id, max(neighbor_height) as highest
FROM info
GROUP BY c1id;

INSERT INTO "Query1"
(SELECT c1id, c1name, c2id, c2name
FROM info NATURAL JOIN highest_neighbor
WHERE neighbor_height = highest
ORDER BY c1name ASC);

DROP VIEW info;
DROP VIEW highest_neighbor;



-- Query 2 statements

INSERT INTO "Query2"(
SELECT cid, cname
FROM country
WHERE cid NOT IN(SELECT cid FROM "oceanAccess")
ORDER BY cname ASC;

-- Query 3 statements

CREATE VIEW info as
SELECT cid 
FROM country
WHERE cid NOT IN (SELECT cid FROM "oceanAccess");

CREATE VIEW info1 as
SELECT country as cid, neighbor
FROM neighbour
WHERE country IN(SELECT country FROM neighbour GROUP BY country HAVING count(country) = 1);

INSERT INTO "Query3"(
SELECT c0.cid as c1id, c0.cname as c1name, c1.cid as c2id, c1.cname as c2name
FROM (info NATURAL JOIN info1) as info3, country as c0, country as c1
WHERE info3.cid = c0.cid AND c1.cid = info3.neighbor
ORDER BY c1name ASC);

DROP VIEW info1;
DROP VIEW info;



-- Query 4 statements

create view info as
select oid, oname, cid
from ocean natural join "oceanAccess";

create view info1 as
select oid,cname
from info natural join country;

create view info2 as
select cname, oname
from info1 natural join ocean;

create view info3 as
select * 
from neighbour inner join info on info.cid = neighbour.neighbor;

create view info4 as 
select cname, oname
from country join info3 on info3.country = country.cid;

INSERT INTO "Query4"
select * from info2 UNION select * from info4;

DROP VIEW info;
DROP VIEW info1;
DROP VIEW info2;
DROP VIEW info3;
DROP VIEW info4;

-- Query 5 statements

CREATE VIEW info AS
SELECT *
FROM hdi
WHERE hdi.year > 2008 AND hdi.year < 2014;

CREATE VIEW info1 AS
SELECT cid, avg(hdi_score) as avghdi
FROM info
GROUP BY cid
ORDER BY avghdi DESC limit 10;

INSERT INTO "Query5"(
SELECT country.cid, cname, avghdi
FROM info1 
INNER JOIN country ON info1.cid = country.cid 
ORDER BY avghdi DESC);

DROP VIEW info1;
DROP VIEW info;




-- Query 6 statements

CREATE VIEW info09 AS
SELECT cid, hdi_score as hdi9
FROM hdi
WHERE hdi.year = 2009;

CREATE VIEW info10 AS
SELECT cid, hdi_score as hdi10
FROM hdi
WHERE hdi.year = 2010;

CREATE VIEW info11 AS
SELECT cid, hdi_score as hdi11
FROM hdi
WHERE hdi.year = 2011;

CREATE VIEW info12 AS
SELECT cid, hdi_score as hdi12
FROM hdi
WHERE hdi.year = 2012;

CREATE VIEW info13 AS
SELECT cid, hdi_score as hdi13
FROM hdi
WHERE hdi.year = 2013;

INSERT INTO "Query10"(
SELECT cid, cname
FROM country NATURAL JOIN info09 NATURAL JOIN info10 NATURAL JOIN info11 NATURAL JOIN info12 NATURAL JOIN info13
WHERE (hdi9 < hdi10 AND hdi10 < hdi11 AND hdi11 < hdi12 AND hdi12 < hdi13)
ORDER BY cname ASC);

DROP VIEW info09;
DROP VIEW info10;
DROP VIEW info11;
DROP VIEW info12;
DROP VIEW info13;


-- Query 7 statements

CREATE VIEW info as
SELECT rid, rname, cid, rpercentage*population as followers
FROM religion NATURAL JOIN country;

INSERT INTO "Query7"(
SELECT rid, rname, sum(followers) as followers
FROM info
GROUP BY rid, rname
ORDER BY followers DESC);

DROP VIEW info;


-- Query 8 statements

CREATE VIEW info AS
SELECT cid, max(lpercentage) as maxpercent
FROM language
GROUP BY cid;

CREATE VIEW info1 AS
SELECT cid, lname
FROM language NATURAL JOIN info
WHERE lpercentage = maxpercent;

INSERT INTO "Query8"
(SELECT c1.cid as c1name, c2.cid as c2name, c1.lname
FROM info1 as c1, info1 as c2
WHERE c1.cid != c2.cid AND c1.lname = c2.lname
ORDER BY lname ASC, c1name DESC);

DROP VIEW info1;
DROP VIEW info;

-- Query 9 statements

CREATE VIEW info AS
SELECT cid, depth
FROM "oceanAccess" NATURAL JOIN ocean;

CREATE VIEW info1 AS
SELECT cname, (height + COALESCE(depth, 0)) as span
FROM info FULL OUTER JOIN country ON info.cid = country.cid
GROUP BY cname, span;

INSERT INTO "Query9"
(SELECT cname, max(span) as totalspan
FROM info1
GROUP BY cname
HAVING max(span) IN (SELECT max(span) FROM info1));

DROP VIEW info1;
DROP VIEW info;


-- Query 10 statements

CREATE VIEW info AS
SELECT country AS cname, sum(length) AS borderslength
FROM neighbour
GROUP BY cname;

INSERT INTO "Query10"(
SELECT cname, borderslength
FROM info
WHERE borderslength = (SELECT max(borderslength)
			FROM info));
DROP VIEW info;

