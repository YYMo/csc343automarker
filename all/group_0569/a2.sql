-- Query 1 statements

CREATE VIEW neighbourinfo AS (
SELECT neighbour.country AS c1id, neighbour.neighbor AS c2id,
c1.cname AS c1name ,c2.cname AS c2name, c2.height AS h2, c1.height AS h1
FROM country c1, country c2, neighbour
WHERE c1.cid = neighbour.country AND c2.cid = neighbour.neighbor
);


CREATE VIEW highestneighbour AS (
SELECT c1id, MAX(h2) as highest
FROM neighbourinfo
GROUP BY c1id);

insert into Query1
(
SELECT highestneighbour.c1id, c1name,c2id,c2name
FROM neighbourinfo, highestneighbour
WHERE neighbourinfo.c1id = highestneighbour.c1id
AND neighbourinfo.h2 = highestneighbour.highest 
order  by c1name asc
);

drop view highestneighbour;
drop view neighbourinfo;



-- Query 2 statements

INSERT INTO Query2
(select cid,cname
 from country
 where NOT EXISTS (select cid from oceanAccess where oceanAccess.cid=country.cid)
 order by cname ASC
);



-- Query 3 statements

Create VIEW Q3 as
select country, COUNT(neighbor)
from neighbour
where EXISTS (select cid from Query2 where Query2.cid=neighbour.country)
group by country;

INSERT INTO Query3
(
select Q3.country as c1id, c1.cname as c1name, neighbour.neighbor as c2id, c2.cname as c2name
from Q3, neighbour, country c1, country c2
where Q3.country=c1.cid and neighbour.neighbor=c2.cid and Q3.count=1 and neighbour.country=Q3.country
order by c1name ASC
);

DROP VIEW Q3;




-- Query 4 statements

create VIEW Q4 as
(
(select country as cid, oid
from neighbour, oceanAccess
where EXISTS (select cid from oceanAccess where oceanAccess.cid=neighbour.neighbor) and oceanAccess.cid=neighbour.neighbor)
union
(select cid,oid from oceanAccess));

INSERT INTO Query4
(select country.cname, ocean.oname
 from country, ocean, Q4
 where country.cid=Q4.cid and ocean.oid=Q4.oid
 order by  cname asc,oname DESC
);

DROP VIEW Q4;



-- Query 5 statements

create view Q5 as 
( 
select hdi.cid, AVG(hdi.hdi_score) as avghdi
from hdi
where hdi.year >= 2009 and hdi.year <= 2013
group by hdi.cid
);

insert into Query5(
select Q5.cid, country.cname, avghdi
from Q5, country
where Q5.cid=country.cid
order by avghdi desc
limit 10
);

drop view Q5;




-- Query 6 statements

create VIEW Q6 as
select h1.cid
from hdi h1, hdi h2,  hdi h3, hdi h4, hdi h5
where (h1.cid=h2.cid and h2.cid=h3.cid and h3.cid=h4.cid and h4.cid=h5.cid) and (h1.year<h2.year and h2.year<h3.year and h3.year<h4.year and h4.year<h5.year) and (h1.hdi_score<h2.hdi_score and h2.hdi_score<h3.hdi_score and h3.hdi_score<h4.hdi_score and h4.hdi_score<h5.hdi_score);

INSERT INTO Query6
(select Q6.cid, country.cname
 from Q6, country
 where Q6.cid=country.cid
 order by cname ASC
);

DROP VIEW Q6;



-- Query 7 statements

create view Q7 as
(
select religion.rid, SUM (religion.rpercentage*country.population) as followers
from religion join country on country.cid=religion.cid
group by religion.rid
order by followers DESC
);

INSERT INTO Query7
(
select distinct religion.rid, religion.rname, followers
from Q7, religion
where Q7.rid =religion.rid
order by followers desc
);

DROP VIEW Q7;



-- Query 8 statements

create VIEW maxl as
select l.cid, l.lid, l.lname
from language l, (select cid, MAX(lpercentage) as maxx from language group by language.cid) l2
where l.cid=l2.cid and l.lpercentage= l2.maxx;

create VIEW almost as
select m1.cid as c1id, m2.cid as c2id, m1.lname as lname
from neighbour,maxl m1, maxl m2
where neighbour.country=m1.cid and neighbour.neighbor=m2.cid and m1.lname=m2.lname;

INSERT INTO Query8
(
select c1.cname as c1name, c2.cname as c2name, almost.lname
from almost,country c1, country c2
where almost.c1id=c1.cid and almost.c2id=c2.cid
order by lname ASC, c1name DESC
);

DROP VIEW almost;
DROP VIEW maxl;




-- Query 9 statements

create VIEW Q9 as
select country.cid, cname, height, oceanAccess.oid
from country left join oceanAccess on country.cid=oceanAccess.cid;

create VIEW Q9a as
select Q9.cid, cname,height, Q9.oid, ocean.depth, (height+COALESCE(ocean.depth,0)) as span
from Q9 left join ocean on Q9.oid=ocean.oid;

INSERT INTO Query9
(
select cname, Q9a.span as totalspan
from Q9a, (select max(span) from Q9a aa) aaa
where aaa.max=Q9a.span
);

DROP VIEW Q9a;
DROP VIEW Q9;



-- Query 10 statements

create VIEW Q10 as 
select country,sum(length) from neighbour group by country;

create view Q10a as 
select country, sum
from Q10 q1, (select max(sum) from Q10 q2) qq
where q1.sum=qq.max;


INSERT INTO Query10
(
select country.cname,sum as borderslength
from Q10a,country
where Q10a.country=country.cid
);

DROP VIEW Q10a;
DROP VIEW Q10;
