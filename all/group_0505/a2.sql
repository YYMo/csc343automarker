-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW topHeight AS
SELECT country AS c, max(height) AS m
FROM country, neighbour
WHERE cid = neighbor
GROUP BY country;

INSERT INTO Query1 (
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name
FROM topHeight, neighbour, country c1, country c2
WHERE c = country and m = c2.height and c1.cid = country and c2.cid = neighbor
ORDER BY c1name 
);

DROP VIEW topHeight;

-- Query 2 statements
CREATE VIEW landLocked AS
(SELECT cid
FROM country)
    EXCEPT
(SELECT cid
FROM oceanAccess);

INSERT INTO Query2 (
SElECT country.cid, cname
FROM landLocked, country
WHERE landLocked.cid = country.cid
ORDER BY cname
);

DROP VIEW landLocked;

-- Query 3 statements 
CREATE VIEW landLockedwithOne AS
(SELECT country AS cid 
FROM neighbour 
GROUP BY country 
HAVING count(neighbor) = 1)
    INTERSECT
((SELECT cid FROM country) 
    EXCEPT 
(SELECT cid FROM oceanAccess));
 
CREATE VIEW findNeighbour AS
SELECT neighbor AS cid 
FROM   neighbour, landLockedwithOne c1 
WHERE neighbour.country = c1.cid;

CREATE VIEW Nebr AS 
SELECT c.cid AS cid, c.cname AS cname
FROM findNeighbour n, country c
WHERE n.cid = c.cid;
 
INSERT INTO Query3 ( 
SELECT c1.cid AS c1id, c2.cname AS c1name,n.cid AS c2id,n.cname AS c2name 
FROM landLockedwithOne c1, country c2, Nebr n
WHERE c1.cid = c2.cid 
ORDER BY c1name 
);  

DROP VIEW landLockedwithOne, findNeighbour, Nebr;

-- Query 4 statements
CREATE VIEW  CwithOceanAcess AS  
SELECT cname, oname FROM oceanAccess o1, ocean o2, country c1 
WHERE o1.cid = c1.cid and o1.oid = o2.oid; 
 
CREATE VIEW NebWithOceanAcess AS  
SELECT c1.cname AS cname, oname  
FROM neighbour, oceanAccess o1, ocean o2, country c1 
WHERE neighbor = o1.cid and o1.oid = o2.oid and c1.cid = country; 
 
INSERT INTO Query4 
((Select * from CwithOceanAcess) UNION (Select * from NebWithOceanAcess) 
ORDER BY cname, oname DESC); 

DROP VIEW CwithOceanAcess,NebWithOceanAcess;

-- Query 5 statements
CREATE VIEW top10HDI AS
SELECT cid, avg(hdi_score) AS avghdi
FROM hdi
WHERE year > 2008 and year < 2014 
GROUP BY cid
ORDER BY avg(hdi_score) DESC
LIMIT 10;

INSERT INTO Query5 (
SELECT c2.cid AS cid, c2.cname AS cname, c1.avghdi AS avghdi
FROM top10HDI AS c1, country AS c2
WHERE c1.cid = c2.cid
ORDER BY avghdi DESC
);

DROP VIEW top10HDI;

-- Query 6 statements
create view hdiYear as 
select * from hdi 
where year >=2009 and year<=2013 
order by cid,year desc; 

create view increasing as
(select cid from country 
        except (
        select y1.cid 
        from hdiYear y1, hdiYear y2 
        where y1.year<y2.year 
        and y1.hdi_score>=y2.hdi_score 
        and y1.cid=y2.cid));
 
insert into Query6( 
select c.cid, c.cname  
from increasing, country c  
where increasing.cid=c.cid 
order by cname asc); 
 
drop view hdiYear,increasing;

-- Query 7 statements
CREATE VIEW religionFollowers AS
SELECT rid, rname, population * rpercentage AS followers
FROM religion, country
WHERE religion.cid = country.cid;

INSERT INTO Query7 (
SELECT rid, rname, sum(followers) AS followers
FROM religionFollowers
GROUP BY rid, rname
ORDER BY followers DESC
);

DROP VIEW religionFollowers;

-- Query 8 statements
CREATE VIEW onePercentage AS
SELECT cid, max(lpercentage) AS max
FROM language
GROUP BY cid;

CREATE VIEW maxPercentage AS
SELECT c1.cid AS cid, lname
FROM onePercentage c1, language c2
WHERE c1.cid = c2.cid and c2.lpercentage = c1.max;

CREATE VIEW maxCountry AS
SELECT country, neighbor, m1.lname AS lname
FROM maxPercentage m1, neighbour, maxPercentage m2
WHERE m1.cid = country
    and m2.cid = neighbor
    and m1.lname = m2.lname;

INSERT INTO Query8 (
SELECT c1.cname AS c1name, c2.cname AS c2name, lname
FROM maxCountry, country c1, country c2
WHERE c1.cid = maxCountry.country and c2.cid = maxCountry.neighbor
ORDER BY lname, c1name DESC
);

DROP VIEW onePercentage, maxPercentage, maxCountry;

-- Query 9 statements
CREATE VIEW noOcean AS
(SELECT cid
FROM country)
    EXCEPT
(SELECT cid
FROM oceanAccess);

CREATE VIEW yesOcean AS
SELECT max(depth) AS oceanDepth, cid
FROM oceanAccess o1, ocean o2
WHERE o1.oid = o2.oid
GROUP BY o1.cid;

INSERT INTO Query9 (
(SELECT cname, height AS totalspan
FROM country c1, noOcean o1
WHERE c1.cid = o1.cid)
    UNION
(SELECT cname, (height + oceanDepth) AS totalspan
FROM country c1, yesOcean o1
WHERE c1.cid = o1.cid)
);

DROP VIEW noOcean, yesOcean;

-- Query 10 statements
CREATE VIEW Maxlength AS
(select country,sum(length) from neighbour group by country); 
 
insert into Query10( 
select c.cname, s.sum as borderslength  
from  Maxlength s,country c , (select max(sum) from Maxlength) maxl 
where maxl.max = s.sum and c.cid = s.country
);

DROP VIEW Maxlength;

