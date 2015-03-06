-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1(
SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name
FROM country as c1, country as c2, neighbour
WHERE country = c1.cid and neighbor = c2.cid and c1.cid <> c2.cid and c2.height >=All(
	SELECT c.height
	FROM country as c, neighbour as n
	WHERE c1.cid = n.country and c.cid = neighbor)
ORDER BY c1name ASC);

-- Query 2 statements
INSERT INTO Query2(
SELECT cid, cname
FROM country 
WHERE cid NOT IN (SELECT cid FROM oceanAccess)
ORDER BY cname ASC
);

-- Query 3 statements
INSERT INTO Query3(
SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name
FROM country as c1, country as c2, neighbour as n
WHERE c1.cid<>c2.cid and c1.cid = n.country and c2.cid = n.neighbor
	and c1.cid IN (SELECT country FROM neighbour GROUP BY country HAVING count(*) = 1)  
	and c1.cid NOT IN (SELECT cid FROM oceanAccess)
ORDER BY c1name ASC
);

-- Query 4 statements
INSERT INTO Query4(
SELECT cname as cname, oname as oname
FROM ((SELECT c1.cname, ocean.oname
	FROM country as c1, oceanAccess as o, ocean
	WHERE c1.cid = o.cid and o.oid = ocean.oid)
	UNION
	(SELECT c1.cname, ocean.oname
	FROM country as c1, country as c2, neighbour as n, oceanAccess as o, ocean
	WHERE c1.cid<>c2.cid and c1.cid = n.country and c2.cid = n.neighbor and c2.cid = o.cid and o.oid = ocean.oid)) as u
ORDER BY cname ASC, oname DESC
);

-- Query 5 statements
INSERT INTO Query5(
SELECT cid, cname, avg(hdi_score) AS avghdi 
FROM hdi NATURAL JOIN country 
WHERE year >= 2009 AND year <= 2013 
GROUP BY cid, cname ORDER BY avghdi DESC
);

-- Query 6 statements
INSERT INTO Query6(
SELECT c.cid as cid, c.cname as cname
FROM country as c, hdi as h1, hdi as  h2, hdi as h3, hdi as h4, hdi as h5
WHERE c.cid = h1.cid and c.cid = h2.cid and c.cid = h3.cid and c.cid = h4.cid and c.cid = h5.cid
	and h1.year=2009 and h2.year=2010 and h3.year=2011 and h4.year=2012 and h5.year=2013
	and h1.hdi_score<h2.hdi_score and h2.hdi_score<h3.hdi_score and h3.hdi_score<h4.hdi_score and h4.hdi_score<h5.hdi_score
ORDER BY cname ASC
);

-- Query 7 statements
INSERT INTO Query7(
SELECT rid, rname, sum(rpercentage*population) AS followers
FROM country NATURAL JOIN religion
GROUP BY rid, rname
ORDER BY followers DESC
);

-- Query 8 statements
INSERT INTO Query8(
SELECT c1.cname AS c1name, c2.cname AS c2name, c1.lname AS lname 
FROM (
	SELECT c.cid,cname,lid,lname
	FROM country c, language l WHERE c.cid=l.cid AND lpercentage >= ALL(
		SELECT lpercentage FROM language WHERE c.cid = cid)) 
AS c1 
LEFT JOIN(
	SELECT * FROM neighbour n
	LEFT JOIN(
		SELECT c.cid,cname,lid,lname
		FROM country c, language l WHERE c.cid=l.cid AND lpercentage >= ALL(
			SELECT lpercentage FROM language WHERE c.cid = cid))
	AS temp ON cid=n.country)
AS c2 ON neighbor = c1.cid
WHERE c1.lid = c2.lid
ORDER BY lname ASC, c1name DESC
);

-- Query 9 statements
create view v as (SELECT c.cid, c.cname, depth, height FROM country c LEFT JOIN (
SELECT * FROM ocean NATURAL JOIN oceanAccess) AS x ON c.cid=x.cid);
create view x as (Select *, case when depth > 0 then depth else 0 end from v);
create view y as (select cname, x.case + height as totalspan from x);

INSERT INTO Query9(
select cname, totalspan from y where totalspan >=ALL(
select totalspan from y) 
);

drop view y;
drop view x;
drop view v;

-- Query 10 statements
INSERT INTO Query10(
SELECT cname, borderlength
FROM(
	SELECT country, sum(length) as borderlength
	FROM neighbour
	GROUP BY country
	HAVING sum(length) >= ALL(SELECT sum(length)
	FROM neighbour
	GROUP BY country)) as x, country
WHERE x.country = country.cid

);
