-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW HeightestC AS
SELECT country AS cwithhighest, max(height) AS m
FROM country, neighbour
WHERE cid = neighbor
GROUP BY country;


INSERT INTO Query1 (
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name
FROM country c1, country c2, HeightestC, neighbour
WHERE cwithhighest = country and  c2.cid = neighbor and c1.cid = country and m = c2.height 
ORDER BY c1name 

);


DROP VIEW HeightestC;

-- Query 2 statements

CREATE VIEW landLocked AS
(SELECT cid FROM country)
    EXCEPT
(SELECT cid FROM oceanAccess);

INSERT INTO Query2 (
SElECT country.cid, cname
FROM landLocked, country
WHERE landLocked.cid = country.cid
ORDER BY cname
);

DROP VIEW landLocked;

-- Query 3 statements
CREATE VIEW landLockedwithOne AS
(SELECT country as cid
FROM neighbour
GROUP BY country
HAVING count(neighbor) = 1) intersect ((SELECT cid FROM country)
    EXCEPT
(SELECT cid FROM oceanAccess));

CREATE VIEW findNe AS
select neighbor as cid 
from   neighbour, landLockedwithOne c1 
where neighbour.country = c1.cid;

CREATE VIEW Neb AS 
select c.cid as cid, c.cname as cname 
from  findNe n join country c on n.cid = c.cid;

INSERT INTO Query3 (
SELECT c1.cid AS c1id, c2.cname AS c1name,n.cid AS c2id,n.cname AS c2name
FROM landLockedwithOne c1, country c2, Neb n
where c1.cid = c2.cid
ORDER BY c1name
); 

DROP VIEW landLockedwithOne,findNe, Neb;

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
CREATE VIEW HDItop10 AS
SELECT cid, avg(hdi_score) AS avghdi
FROM hdi
WHERE year > 2008 and year < 2014 
GROUP BY cid
ORDER BY avg(hdi_score) DESC
LIMIT 10;

INSERT INTO Query5 (
SELECT c2.cid AS cid, c2.cname AS cname, c1.avghdi AS avghdi
FROM HDItop10 AS c1, country AS c2
WHERE c1.cid = c2.cid
ORDER BY avghdi DESC
);

DROP VIEW HDItop10;

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

INSERT INTO Query7 (
SELECT rid, rname, sum(population * rpercentage) AS followers
FROM religion r, country c
WHERE r.cid = c.cid
GROUP BY rid, rname
ORDER BY followers DESC
);


-- Query 8 statements

CREATE VIEW maxL AS
SELECT c1.cid AS cid, lname
FROM 
	(SELECT cid, max(lpercentage) AS max
	FROM language
	GROUP BY cid) c1, language c2
WHERE c1.cid = c2.cid and c2.lpercentage = c1.max;

CREATE VIEW PopularPair AS
SELECT country, neighbor, max1.lname AS lname
FROM neighbour, maxL max1, maxL max2
WHERE max1.cid = country
    and max2.cid = neighbor
    and max1.lname = max2.lname;

INSERT INTO Query8 (
SELECT c1.cname AS c1name, c2.cname AS c2name, lname
FROM PopularPair pp, country c1, country c2
WHERE c1.cid = pp.country and c2.cid = pp.neighbor
ORDER BY lname, c1name DESC
);

DROP VIEW maxL, PopularPair;

-- Query 9 statements

CREATE VIEW OceanC AS
SELECT max(depth) as oceanDepth, cid
FROM oceanAccess o1, ocean o2
WHERE o1.oid = o2.oid
GROUP BY o1.cid;

CREATE VIEW landlocked AS
 (SELECT cid FROM country)
     EXCEPT
	(SELECT cid FROM oceanAccess);

CREATE VIEW landlockedC AS
Select 0 as oceanDepth, cid
From landlocked;

INSERT INTO Query9 (
SELECT cname, (height + oceanDepth) AS totalspan
FROM country c, ((Select * from OceanC) union (Select * from landlockedC)) o
WHERE c.cid = o.cid);

DROP VIEW OceanC, landlocked, landlockedC;



-- Query 10 statements

CREATE VIEW Maxlength AS
(select country,sum(length) from neighbour group by country);

insert into Query10(
select c.cname, s.sum as borderslength 
from  Maxlength s,country c , (select max(sum) from Maxlength) maxl
where maxl.max = s.sum and c.cid = s.country
);

DROP VIEW Maxlength;




