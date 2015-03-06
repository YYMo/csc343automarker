-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
insert into Query1
(
select tb3.c1id as c1id, tb3.c1name as c1name, tb3.c2id as c2id, tb3.c2name as c2name
from
(select c1id, max(height) as height from 
(select b.country as c1id, c1.cname as c1name, b.neighbor as c2id,c2.cname as c2name, c2.height
from country c1, neighbour b, country c2 
where (c1.cid=b.country) and (c2.cid=b.neighbor)) as tb1 group by c1id) as tb2,
(select b.country as c1id, c1.cname as c1name, b.neighbor as c2id,c2.cname as c2name, c2.height
from country c1, neighbour b, country c2 
where (c1.cid=b.country) and (c2.cid=b.neighbor)) as tb3
where tb3.c1id=tb2.c1id and tb3.height=tb2.height
order by c1name
);


-- Query 2 statements
insert into Query2
(
select cid, cname
from
(select distinct cid from country
Except
select distinct cid from oceanAccess) as tb1 natural join country
order by cname ASC
);

-- Query 3 statements
insert into Query3
(
select tb3.cid as c1id, tb3.cname as c1name, n2.neighbor as c2id, c2.cname as c2name
from
(select cid, cname
from
(select n.country as cid, count(n.country), c.cname
from neighbour n, country c
where n.country=c.cid
group by n.country, c.cname
having count(n.country) = 1) as tb1
natural join
(select cid, cname
from
(select distinct cid from country
Except
select distinct cid from oceanAccess) as tb1 natural join country
) as tb2
) as tb3, neighbour n2, country c2
where tb3.cid = n2.country and n2.neighbor = c2.cid
order by tb3.cname ASC
);

-- Query 4 statements
insert into Query4
(
select *
from
((select cname, oname
from country c41 natural join oceanAccess oa41 natural join ocean o41)

union

(select c42.cname as cname, o42.oname as oname
from country c42, neighbour n42, oceanAccess oa42, ocean o42
where c42.cid = n42.country and n42.neighbor = oa42.cid and oa42.oid = o42.oid
)) as tb41
order by cname ASC, oname DESC
);


-- Query 5 statements
insert into Query5 
(
select cid, cname, avghdi
from
(select cid, avg(hdi_score) as avghdi 
from hdi
where year<=2013 and year>=2009
group by cid
order by avghdi DESC
limit 10) as tb51 natural join country
order by avghdi DESC
limit 10
);

-- Query 6 statements
insert into Query6
(
SELECT cid, cname FROM 
(SELECT a1.cid FROM hdi AS a1, hdi AS a2 WHERE a1.year = 2009 AND a2.year = 2010 AND a1.cid = a2.cid AND a1.hdi_score < a2.hdi_score INTERSECT
SELECT a1.cid FROM hdi AS a1, hdi AS a2 WHERE a1.year = 2010 AND a2.year = 2011 AND a1.cid = a2.cid AND a1.hdi_score < a2.hdi_score INTERSECT
SELECT a1.cid FROM hdi AS a1, hdi AS a2 WHERE a1.year = 2011 AND a2.year = 2012 AND a1.cid = a2.cid AND a1.hdi_score < a2.hdi_score INTERSECT
SELECT a1.cid FROM hdi AS a1, hdi AS a2 WHERE a1.year = 2012 AND a2.year = 2013 AND a1.cid = a2.cid AND a1.hdi_score < a2.hdi_score) as tb1 NATURAL JOIN country ORDER BY cname ASC
);

-- Query 7 statements
insert into Query7
(
select rid, rname, sum(pop) as followers 
from
(select rid, rname, (population*rpercentage) pop
from 
country natural join religion) as tb1
group by rid,rname
order by followers DESC
);


-- Query 8 statements
insert into Query8
(
select n82.cname as c1name, n83.cname as c2name, tb3.lname as lname
from
(select n81.country as c1id, n81.neighbor as c2id, view1.lname as lname
from neighbour n81, 

(select tb1.cid as cid, l81.lid as lid, l81.lname as lname
	from
	(select cid, max(lpercentage)
		from language
		group by cid) as tb1, language l81
		where tb1.cid = l81.cid and tb1.max = l81.lpercentage
) as view1,

(select tb1.cid as cid, l81.lid as lid, l81.lname as lname
	from
	(select cid, max(lpercentage)
		from language
		group by cid) as tb1, language l81
		where tb1.cid = l81.cid and tb1.max = l81.lpercentage
) as view2

where n81.country = view1.cid and n81.neighbor = view2.cid and view1.lid = view2.lid) as tb3
,country n82,country n83

where tb3.c1id = n82.cid and tb3.c2id = n83.cid
order by tb3.lname ASC, n82.cname DESC
);


-- Query 9 statements
insert into Query9
(
select cname, totalspan
from
(SELECT cname, (height + depth) as totalspan, rank() over(order by (height + depth) desc)FROM 
(
(SELECT cid, height, oid, depth FROM country NATURAL JOIN oceanAccess NATURAL JOIN ocean) EXCEPT
(SELECT tb1.cid, tb1.height, tb1.oid, tb1.depth FROM 
(SELECT cid, height, oid, depth FROM country NATURAL JOIN oceanAccess NATURAL JOIN ocean)AS tb1, 
(SELECT cid, height, oid, depth FROM country NATURAL JOIN oceanAccess NATURAL JOIN ocean)AS tb2 
WHERE tb1.cid = tb2.cid and tb1.oid <> tb2.oid  and tb1.depth < tb2.depth) ) AS b1 NATURAL JOIN country) as tb2
Where rank = 1
);


-- Query 10 statements
insert into Query10
(
SELECT cname, borderslength  FROM (SELECT country FROM neighbour EXCEPT
(SELECT tb1.country 
		FROM (SELECT country, sum(length) as total FROM neighbour group by country) as tb1, 
			(SELECT country, sum(length) as total FROM neighbour group by country) as tb2
		WHERE tb1.total < tb2.total )) as t3 NATURAL JOIN (SELECT country, sum(length) as borderslength FROM neighbour group by country) as t4 JOIN country on t3.country = country.cid 
);

