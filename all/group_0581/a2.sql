-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- *****VIEW***** 
CREATE VIEW question1 as 
--SELECT cid, country.height
SELECT c1id, c1name, c2id, cname AS c2name, country.height AS height
FROM country,
(SELECT cid AS c1id, cname AS c1name, neighbor AS c2id, height
FROM country, neighbour
WHERE cid=country)a
WHERE cid=c2id;

CREATE VIEW question2 as 
SELECT cid, cname 
FROM country
WHERE cid NOT IN 
(SELECT cid
FROM oceanAccess);

CREATE VIEW questionthree as
SELECT x1.c1id as c1id
FROM question1 AS x1, question1 as x2
WHERE x1.c1id=x2.c1id and x1.c2id <> x2.c2id;

CREATE VIEW surroundby1_q3 as
(SELECT cid AS c1id
FROM country)
	EXCEPT
(SELECT c1id
FROM questionthree);


CREATE VIEW question4 as 
(SELECT cname, oname
FROM oceanAccess, ocean, country
WHERE oceanAccess.cid=country.cid and oceanAccess.oid=ocean.oid);

CREATE VIEW question5 as 
(SELECT hdi.cid as cid, cname, hdi_score, year
FROM hdi, country
WHERE hdi.cid = country.cid);

-- Query 1 statements
INSERT INTO Query1
(SELECT c1id, question1.c1name AS c1name, c2id, c2name
FROM question1,
(SELECT c1name, max(height) as max
FROM question1
GROUP BY c1name)x
WHERE height = max and x.c1name=question1.c1name
ORDER BY c1name);
--SELECT * FROM question1;


-- Query 2 statements
INSERT INTO Query2
(SELECT cid, cname 
FROM country
WHERE cid NOT IN 
(SELECT cid
FROM oceanAccess)
ORDER BY cname
);


-- Query 3 statements
--The first task we need to accomplish is to find those countries with more than one neighbours 
INSERT INTO Query3 
(SELECT a.c1id as c1id, a.c1name as c1name, a.c2id as c2id, a.c2name as c2name
FROM question2,
(SELECT surroundby1_q3.c1id as c1id, c1name, c2id, c2name
FROM surroundby1_q3, question1
WHERE surroundby1_q3.c1id = question1.c1id)a
WHERE a.c1id=question2.cid
ORDER BY c1name ASC);


-- Query 4 statements
INSERT INTO Query4
((SELECT cname, oname
FROM question4)
UNION 
(SELECT c1name as cname, oname
FROM question1, oceanAccess, ocean
WHERE oceanAccess.cid=question1.c2id and  oceanAccess.oid=ocean.oid))
ORDER BY cname ASC, oname DESC;


-- Query 5 statements

--INSERT INTO Query5
INSERT INTO Query5
(
SELECT cid, cname, avg(hdi_score) as avghdi
FROM question5
WHERE year>2008 and year<2014
GROUP BY cid, cname
ORDER BY avghdi DESC
LIMIT 10
);

DROP view question5;
DROP view question4;

DROP view surroundby1_q3;

DROP view questionthree;

DROP view question1;
-- Query 6 statements
insert into Query6(
 select country.cid, cname from country, (select five.cid from 
 (select counhdi.cid,count(counhdi.cid) from (select a.cid from hdi a 
 inner join hdi b on a.cid = b.cid and a.year = b.year-1 
 where a.hdi_score < b.hdi_score and a.year >= 2009 and b.year 
 <= 2013 order by a.cid asc) as counhdi group by counhdi.cid) as five 
 where count = 4) as fullfive where country.cid = fullfive.cid 
 order by country.cid asc);


-- Query 7 statements
insert into Query7(
select rname, rid, sum(population*rpercentage) as followers
from (select rname, rid, country.cid, population, rpercentage  
from religion join country on religion.cid = country.cid) as religcoun 
group by rname, rid
order by followers desc);

-- Query 8 statements
insert into Query8(
with neighbours as 
(select cidlan.cid, cname, neighbor, lname from neighbour, 
(select cname, total.cid, lname from country, 
(select maxper.cid, lid,lname, maxlan from language, 
(select cid, max(lpercentage) as maxlan from language 
group by cid) as maxper
where maxper.maxlan = language.lpercentage and maxper.cid=language.cid) as total
where total.cid = country.cid) as cidlan
where neighbour.country = cidlan.cid)
select n1.cname as c1name, n2.cname as c2name, n1.lname 
from neighbours as n1, neighbours as n2
where n1.neighbor = n2.cid and n1.lname = n2.lname and n1.cid != n1.neighbor
group by n1.cname,n2.cname,n1.lname
order by lname asc, c1name desc)
;



-- Query 9 statements
insert into Query9(
select cname, max(height+depth) as totalspan from
(select countryocean.cid,cname,height,countryocean.oid, depth
from ocean join
(select country.cid, cname, height, oid
from country join oceanaccess 
on country.cid = oceanaccess.cid) as countryocean
on countryocean.oid = ocean.oid) as combined
group by cname)
;


-- Query 10 statements
insert into Query10(
select cname, borderslength 
from country,
(select country, sum(length) as borderslength from neighbour
group by country) as borders
where borders.country = cid);


