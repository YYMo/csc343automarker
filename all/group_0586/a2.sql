-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

--“INSERT INTO QueryX (SELECT ... <complete your SQL query here> ...)”

-- Query 1 statements
INSERT INTO Query1
(
select result.country as c1id, c1.cname as c1name, result.neighbor as c2id, c2.cname as c2name
from country c1,
(select distinct country, neighbor, height, max
from (select x.country, neighbor, height, max

from

(select neighbour.country, neighbour.neighbor, country.height
from neighbour, country
where neighbour.neighbor = country.cid) x,

(select a.country, max(a.height)
from (select neighbour.country, neighbour.neighbor, country.height
from neighbour, country
where neighbour.neighbor = country.cid) a
group by a.country) y

where x.country = y.country) aaa
where height > max or height = max ) result, country c2
where c1.cid = result.country and c2.cid = result.neighbor
order by c1name ASC
);

-- Query 2 statements --------------------------------
INSERT INTO Query2
(
SELECT DISTINCT country.cid AS cid, country.cname AS cname 
FROM (country LEFT JOIN oceanAccess 
	ON country.cid = oceanAccess.cid) 
WHERE oceanAccess.cid IS NULL
ORDER BY cname ASC
);

-- Query 3 statements --------------------------------
INSERT INTO Query3
(
select neighbour.country as c1id, c1.cname as c1name, neighbour.neighbor as c2id, c2.cname as c2name
from
(select country.cid 
FROM (country LEFT JOIN oceanAccess 
	ON country.cid = oceanAccess.cid)
WHERE oceanAccess.cid IS NULL

INTERSECT

select country as cid
from neighbour
group by country
having count(neighbor) = 1) result, country c1, country c2, neighbour 
where neighbour.country = c1.cid and neighbour.neighbor = c2.cid and neighbour.country = result.cid
order by c1name ASC
);

-- Query 4 statements --------------------------------
INSERT INTO Query4
(
select country.cname, ocean.oname
from
(select *
from oceanAccess

union 

select country as cid, oid
from neighbour, oceanAccess
where neighbour.neighbor = cid) result, country, ocean
where result.cid = country.cid and ocean.oid = result.oid
order by cname ASC, oname DESC
);

-- Query 5 statements --------------------------------
INSERT INTO Query5
(
select country.cid as cid, country.cname as cname, result.average as avghdi
from
(select *
from 
(select cid, avg(hdi_score) as average
from hdi
where year between 2009 and 2013
group by cid) a
order by a.average DESC
LIMIT 10) result, country where result.cid = country.cid
order by avghdi DESC
);

-- Query 6 statements --------------------------------
INSERT INTO Query6
(
select result.cid as cid, country.cname as cname
from
(select result.cid, result.y2009, result.score2009, result.y2010, result.score2010, result.y2011, result.score2011, result.y2012, result.score2012, 
             h5.year as y2013, h5.hdi_score as score2013
from 
(select result.cid, result.y2009, result.score2009, result.y2010, result.score2010, result.y2011, result.score2011, 
             h4.year as y2012, h4.hdi_score as score2012
from
(select result.cid, result.y2009, result.score2009, result.y2010, result.score2010, h3.year as y2011, h3.hdi_score as score2011
from
(select h1.cid as cid, h1.year as y2009, h1.hdi_score as score2009, h2.year as y2010, h2.hdi_score as score2010 
from hdi h1, hdi h2
where h1.cid = h2.cid and h1.year = 2009 and h2.year = 2010 and h1.hdi_score < h2.hdi_score) result, hdi h3
where h3.year = 2011 and h3.hdi_score > result.score2010 and h3.cid = result.cid) result, hdi h4
where h4.year = 2012 and h4.hdi_score > result.score2011 and h4.cid = result.cid) result, hdi h5
where h5.year = 2013 and h5.hdi_score > result.score2012 and h5.cid = result.cid) result, country
where result.cid = country.cid
order by cname ASC
);

-- Query 7 statements --------------------------------
INSERT INTO Query7
(
select distinct result.rid as rid, religion.rname as rname, followers
from
(select rid, sum(rpercentage*population/100) as followers
from religion, country
where religion.cid = country.cid
group by rid) result, religion
where result.rid = religion.rid
Order by followers DESC
);

-- Query 8 statements --------------------------------
INSERT INTO Query8 
(
select c1.cname as c1name, c2.cname as c2name, e.lname as lname
from
(select country, answer2.lname, answer2.neighbor
from
(select distinct country, answer1.lname, neighbor
from
(select result.cid, language.lname
from
(select cid, max(lpercentage) as max
from language
group by cid) result join language on result.cid = language.cid and result.max = language.lpercentage) answer1 join neighbour on country = answer1.cid) answer2 join (select result.cid, language.lname
from
(select cid, max(lpercentage) as max
from language
group by cid) result join language on result.cid = language.cid and result.max = language.lpercentage) answer3 on neighbor = answer3.cid where answer2.lname = answer3.lname and country < neighbor) e, country c1, country c2 where c1.cid = e.country and c2.cid = e.neighbor
order by lname ASC, c1name DESC
);


-- Query 9 statements
INSERT INTO Query9
(
select country.cname, (country.height - ocean.depth) as totalspan
from oceanAccess, country, ocean
where oceanAccess.cid = country.cid and ocean.oid = oceanAccess.oid

union

select country.cname, country.height as totalspan
from
(select cid
from country
where cid not in (
	select cid from oceanAccess
)) result, country
where result.cid = country.cid
order by totalspan DESC
LIMIT 1
);

-- Query 10 statements
INSERT INTO Query10
(
select country.cname, borderslength
from
(select country, sum(length) as borderslength
from neighbour
group by country
order by borderslength DESC
limit 1) result, country
where country.cid = country
);



