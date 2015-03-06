-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW v1 as
SELECT neighbour.country as c1id, max(country.height) as height
FROM country JOIN neighbour ON country.cid=neighbour.neighbor
GROUP by neighbour.country 
ORDER by c1id ASC;

CREATE VIEW v2 as
SELECT v1.c1id, country.cname as c2name, country.cid as c2id
FROM country, v1, neighbour
WHERE country.cid=neighbour.neighbor and country.height=v1.height and neighbour.country=v1.c1id 
ORDER by c1id ASC;

INSERT INTO Query1( 
SELECT v2.c1id, country.cname as c1name, v2.c2id, v2.c2name
FROM country, v2
WHERE country.cid=v2.c1id
ORDER by c1id ASC);

DROP VIEW v2;
DROP VIEW v1;

-- Query 2 statements

--Change oceanaccess to oceanAccess
INSERT INTO Query2(
SELECT cid,cname
FROM country a 
WHERE NOT EXISTS (SELECT cid
					FROM oceanAccess b
					WHERE a.cid = b.cid
					)
ORDER by cname ASC);

-- Query 3 statements
CREATE VIEW v1 as
SELECT cid,cname
FROM country a 
WHERE NOT EXISTS (SELECT cid
					FROM oceanAccess b
					WHERE a.cid = b.cid
					)
ORDER by cname ASC;

CREATE VIEW v2 as
SELECT v1.cid as c1id
FROM v1, neighbour
WHERE v1.cid=neighbour.country
GROUP by v1.cid
HAVING count(neighbour.neighbor)=1;

INSERT INTO Query3(
SELECT v2.c1id as c1id, C1.cname as c1name, neighbour.neighbor as c2id, C2.cname as c2name
FROM v2, neighbour, country C1, country C2
WHERE v2.c1id=neighbour.country and v2.c1id=C1.cid and neighbour.neighbor=C2.cid
ORDER by c1name ASC);

DROP VIEW v2;
DROP VIEW v1;


-- Query 4 statements
CREATE VIEW v1 as
(SELECT neighbour.country as cid, oceanAccess.oid as oid
FROM neighbour, oceanAccess
WHERE oceanAccess.cid=neighbour.neighbor)
UNION
(SELECT cid, oid
FROM oceanAccess)
ORDER by cid ASC;

INSERT INTO Query4(
SELECT country.cname as cname, ocean.oname as oname 
FROM country, v1, ocean
WHERE v1.cid=country.cid and v1.oid=ocean.oid
ORDER by cname ASC, oname DESC);

DROP VIEW v1;

-- Query 5 statements
CREATE VIEW v1 as
SELECT cid, avg(hdi_score) as avghdi
FROM hdi
WHERE year>=2009 and year<=2013
GROUP by cid
ORDER by avg(hdi_score) DESC
LIMIT 10;

INSERT INTO Query5(
SELECT v1.cid as cid, country.cname as cname, v1.avghdi as avghdi
FROM country, v1
WHERE v1.cid=country.cid
ORDER by avghdi DESC);

DROP VIEW v1;


-- Query 6 statements
CREATE VIEW v1 as
((SELECT cid
FROM hdi
GROUP by cid
ORDER by cid
) 
EXCEPT
(SELECT a.cid
FROM hdi a 
WHERE year>=2009 and year<=2013 and a.hdi_score <= (SELECT hdi_score
					FROM hdi b
					WHERE a.cid = b.cid and b.year=a.year-1
					)
GROUP by cid
ORDER by cid));

INSERT INTO Query6(
SELECT v1.cid as cid, country.cname as cname
FROM country, v1
WHERE v1.cid=country.cid
ORDER by cname ASC);

DROP VIEW v1;


-- Query 7 statements
CREATE VIEW v1 as
SELECT religion.rid as rid , religion.rname as rname, (country.population * religion.rpercentage) as followers 
FROM country,religion
WHERE country.cid=religion.cid;

INSERT INTO Query7(
SELECT v1.rid as rid, v1.rname as rname, sum(v1.followers) as followers 
FROM v1,religion
WHERE v1.rid=religion.rid
Group by v1.rid, v1.rname
Order by followers DESC);

DROP VIEW v1;

-- Query 8 statements
CREATE VIEW v1 as
SELECT cid, lname
FROM language a
WHERE NOT EXISTS(SELECT lpercentage FROM Language b WHERE a.cid=b.cid and a.lpercentage<b.lpercentage) ;

INSERT INTO Query8(
SELECT c1.cname as c1name, c2.cname as c2name, va.lname as lname
FROM v1 va, neighbour,v1 vb, country c1, country c2
WHERE va.cid=neighbour.country and vb.cid=neighbour.neighbor and va.lname=vb.lname and va.cid=c1.cid and vb.cid=c2.cid
ORDER by lname ASC, c1name DESC);

DROP VIEW v1;

-- Query 9 statements
CREATE VIEW v1 as
SELECT oceanAccess.cid as cid, ocean.depth as depth
FROM oceanAccess NATURAL JOIN ocean;

CREATE VIEW v2 as
SELECT country.cid as cid, country.height as height, v1.depth as depth
FROM country FULL OUTER JOIN v1 ON country.cid=v1.cid
ORDER BY country.cid; 

CREATE VIEW v3 as
SELECT cid, height, coalesce(depth, 0) as depth 
FROM v2;

CREATE VIEW v4 as
SELECT cid, height+depth as totalspan
FROM v3
EXCEPT
SELECT va.cid, va.height+va.depth as totalspan
FROM v3 va, v3 vb
WHERE (va.height+va.depth) < (vb.height+vb.depth);

INSERT INTO Query9(SELECT country.cname, v4.totalspan
FROM country, v4
WHERE country.cid=v4.cid);

DROP VIEW v4;
DROP VIEW v3;
DROP VIEW v2;
DROP VIEW v1;



-- Query 10 statements

CREATE VIEW v1 as
SELECT country as cid, sum(length) as borderslength
FROM neighbour
GROUP by cid;

CREATE VIEW v2 as
((SELECT *
FROM v1)
EXCEPT
(SELECT va.cid, va.borderslength
FROM v1 va, v1 vb
WHERE va.cid!=vb.cid and va.borderslength<vb.borderslength));

INSERT INTO Query10(SELECT country.cname as cname, v2.borderslength
FROM v2, country 
WHERE v2.cid = country.cid);

DROP VIEW v2;
DROP VIEW v1;

