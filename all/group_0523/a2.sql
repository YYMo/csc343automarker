-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.


-- Query 1 statements
create view q1full as
select cid as c2id, cname as c2name, neighbor as c1id, height
from country join neighbour on country.cid=neighbour.country;

create view q1max as
select neighbor, max(height) as maxheight
from country join neighbour on country.cid=neighbour.country
group by neighbor;

create view q1tall as
select *
from q1full join q1max on q1full.c1id=q1max.neighbor and q1full.height=q1max.maxheight;

create view q1ans as
select c1id, cname as c1name, c2id, c2name
from q1tall join country on q1tall.c1id=country.cid 
order by c1id asc;

insert into Query1(select * from q1ans);

-- Query 2 statements
create view q2coast as
select cid as c2
from oceanAccess;

create view q2almost as
(select cid as ccc
from country)  
except all
(select *
from q2coast); 

create view q2ans as
select cid, cname
from country join q2almost on country.cid=q2almost.ccc
order by cname asc;

insert into Query2 (select * from q2ans);

-- Query 3 statements
create view q3vati as
select cid as c1id, cname as c1name
from q2ans join neighbour on q2ans.cid=neighbour.country
group by c1id, c1name
having count(neighbor) = 1;

create view q3add as
select c1id, c1name, neighbor as c2id
from q3vati join neighbour on q3vati.c1id=neighbour.country;

create view q3ans as
select c1id, c1name, c2id, country.cname as c2name
from q3add join country on q3add.c2id=country.cid
order by c1name asc;

insert into Query3 (select * from q3ans);

-- Query 4 statements
create view q4inde as
select country, neighbor, oid
from neighbour, oceanAccess 
where neighbour.country=oceanAccess.cid OR neighbour.neighbor=oceanAccess.cid;

create view q4island as
(select country as cid, oid
from q4inde)
union
(select *
from oceanAccess);

create view q4ans as
select cname, oname
from q4island, ocean, country
where ocean.oid=q4island.oid and country.cid=q4island.cid
order by cname asc, oname desc;

insert into Query4(select * from q4ans);

-- Query 5 statements
create view q5avg as
select cid as cc, avg(hdi_score) as avghdi
from hdi
where year>=2009 and year<=2013
group by cid;

create view q5ans as
select cc as cid, cname, avghdi
from q5avg, country
where q5avg.cc=country.cid
order by avghdi desc
limit 10;

insert into Query5(select * from q5ans);

-- Query 6 statements
create view q6hacky as
select h1.cid as h1c, h2.cid as h2c, h3.cid as h3c, h4.cid as h4c, h5.cid as h5c 
from hdi as h1, hdi as h2, hdi as h3, hdi as h4, hdi as h5
where h1.year=2009 and h2.year=2010 and h3.year=2011 and h4.year=2012 and h5.year=2013 and h1.hdi_score<h2.hdi_score and h2.hdi_score<h3.hdi_score and h3.hdi_score<h4.hdi_score and h4.hdi_score<h5.hdi_score and h1.cid=h2.cid and h2.cid=h3.cid and h3.cid=h4.cid and h4.cid=h5.cid;

create view q6ans as
select country.cid as cid, cname
from q6hacky, country
where q6hacky.h1c=country.cid
order by cname asc;

insert into Query6(select * from q6ans);

--Query 7 statements
create view q7total as
select rid, rname, rpercentage * population as followers
from religion, country
where religion.cid=country.cid;

create view q7sum as
select rid, rname, sum(followers) as followers
from q7total
group by rid, rname;

insert into Query7(select * from q7sum);

-- Query 8 statements
create view q8most as
select cid as cc, max(lpercentage) as most
from language
group by cid;

create view q8popu as
select cid, lid, lname, most
from q8most, language
where q8most.cc=language.cid and q8most.most=language.lpercentage;

create view q8nei as
select cid as cc, lid, lname, neighbor
from q8popu, neighbour
where q8popu.cid=neighbour.country;

create view q8name as
select cid as ccc, lid, lname, neighbor, cname as c1name
from q8nei, country c1
where q8nei.cc=c1.cid;  

create view q8names as
select cid, c1name, neighbor as c2id, cname as c2name, lid, lname as ll
from q8name, country c2
where q8name.neighbor=c2.cid;

create view q8ans as
select c1name, c2name, lname
from q8names, q8popu
where q8popu.cid=q8names.c2id and q8names.ll=q8popu.lname
order by c1name desc;

insert into Query8(select * from q8ans);

-- Query 9 statements
create view q9dir as
select cid as cc, oid as oo
from country natural left join oceanAccess;

create view q9deep as
select *
from q9dir join ocean on ocean.oid=q9dir.oo;

create view q9high as
(select cid
from country) 
except
(select cc as cid
from q9deep);

create view q9hhalf as
select cname, height as totalspan 
from q9high join country on q9high.cid=country.cid;

create view q9lhalf as
select cid, cname, height + depth as totalspan
from q9deep join country on q9deep.cc=country.cid;

create view q9lll as
select cid as cc, max(totalspan) as totalspan
from q9lhalf
group by cc;

create view q9lol as
select cname, totalspan
from q9lll join country on q9lll.cc=country.cid;

create view q9ans as
(select cname, totalspan
from q9lol)
union
(select cname, totalspan
from q9hhalf);

create view q9max as
select max(totalspan) as maxy
from q9ans;

create view q9final as
select cname, totalspan
from q9max, q9ans
where q9max.maxy=q9ans.totalspan;

insert into Query9(select * from q9final);

-- Query 10 statements
create view q0all as
select cid, cname, neighbor, length
from country join neighbour on country.cid=neighbour.country;

create view q0sum as
select cid as cc, sum(length) as borderslength
from q0all
group by cc;

create view q0ans as
select cname, borderslength
from q0sum join country on country.cid=q0sum.cc;

insert into Query10(select * from q0ans);

--All "drop view" statements
drop view q2coast cascade;
drop view q1full cascade;
drop view q1max cascade;
drop view q4inde cascade;
drop view q5avg cascade;
drop view q6hacky cascade;
drop view q7total cascade;
drop view q8most cascade;
drop view q9dir cascade;
drop view q0all cascade;

