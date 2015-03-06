-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
create view neighbourheights as 
select country as c1id, neighbor as c2id, cname as c2name, height 
from neighbour, country
where neighbor = cid;

create view maxheights as
select c1id, cname as c1name, c2id, c2name, neighbourheights.height
from neighbourheights, country
where c1id = cid;

create view finalans as
select distinct maxheights.c1id, maxheights.c1name, maxheights.c2id, maxheights.c2name 
from maxheights, (select c1name, MAX(height) as heights
from maxheights
group by maxheights.c1name) groupedheights
where maxheights.c1name = groupedheights.c1name 
and maxheights.height = groupedheights.heights
order by maxheights.c1name ASC;

Insert into Query1 (c1id, c1name, c2id, c2name) 
select distinct c1id, c1name, c2id, c2name
from finalans;


drop view finalans cascade;
drop view maxheights cascade;
drop view neighbourheights cascade;

-- Query 2 statements

create view finalans as 
select cid, cname
from country
where not exists(
select cid
from oceanAccess
where oceanAccess.cid = country.cid)
order by cname ASC;

Insert into Query2 (cid, cname) 
select *
from finalans;


drop view finalans cascade;

-- Query 3 statements
create view landlocked as 
select cid, cname
from country
where not exists(
select cid
from oceanAccess
where oceanAccess.cid = country.cid);

create view lockedbour as
select cid, cname, neighbor
from landlocked, neighbour
where landlocked.cid = country;

create view twoplus as
select lockedbour.cid, lockedbour.cname, lockedbour.neighbor
from lockedbour
JOIN lockedbour as dup
ON lockedbour.cid = dup.cid 
and lockedbour.neighbor <> dup.neighbor;

create view onebour as
select cid, cname, neighbor
from lockedbour 
where not exists (select * from twoplus);

create view withname1 as
select country.cid as c1id, country.cname as c1name, neighbor as c2id
from onebour, country
where onebour.cid = country.cid;

create view endingone as
select c1id, c1name, c2id, country.cname as c2name
from withname1, country
where withname1.c2id = country.cid
order by c1name ASC;


Insert into Query3 (c1id, c1name, c2id, c2name) 
select *
from endingone;

drop view endingone;
drop view withname1;
drop view onebour cascade;
drop view twoplus cascade;
drop view lockedbour cascade;
drop view landlocked cascade;


-- Query 4 statements

create view indirectaccess as
select neighbour.country as cid, oid
from neighbour, oceanAccess
where neighbour.neighbor = oceanAccess.cid;

create view oceancountries as
select cid, oid
from oceanAccess
UNION
select cid, oid
from indirectaccess;

create view oceancname as
select cname, oid
from oceancountries join country
on oceancountries.cid = country.cid; 

create view endresult as
select cname, oname
from oceancname join ocean
on oceancname.oid = ocean.oid
order by cname ASC, oname DESC;

Insert into Query4 (cname, oname)
select *
from endresult;


drop view endresult cascade;
drop view oceancname cascade;
drop view oceancountries cascade;
drop view indirectaccess cascade;

-- Query 5 statements

create view avgcid as
select cid, AVG(hdi_score) as avghdi
from hdi
where year > 2008
and year < 2014
group by cid;

create view endresult as
select avgcid.cid as cid, cname, avghdi
from avgcid, country
where avgcid.cid = country.cid
order by avghdi DESC
limit 10;

Insert into Query5 (cid, cname, avghdi)
select * 
from endresult;

drop view endresult cascade;
drop view avgcid;

-- Query 6 statements

create view thirtwelve as
select hdi.cid, dup.hdi_score
from hdi 
join hdi as dup
on hdi.cid = dup.cid
and hdi.hdi_score < dup.hdi_score
and dup.year = 2013;

create view tweleven as
select hdi.cid, hdi.hdi_score
from thirtwelve 
join hdi
on hdi.cid = thirtwelve.cid
and hdi.hdi_score < thirtwelve.hdi_score
and hdi.year = 2012;

create view teneleven as
select hdi.cid, hdi.hdi_score
from tweleven 
join hdi
on hdi.cid = tweleven.cid
and hdi.hdi_score < tweleven.hdi_score
and hdi.year = 2011;

create view nineten as
select hdi.cid, hdi.hdi_score
from teneleven 
join hdi
on hdi.cid = teneleven.cid
and hdi.hdi_score < teneleven.hdi_score
and hdi.year = 2010;

create view ninelowest as
select hdi.cid, hdi.hdi_score
from nineten 
join hdi
on hdi.cid = nineten.cid
and hdi.hdi_score < nineten.hdi_score
and hdi.year = 2009;

create view endresult as
select ninelowest.cid, cname
from ninelowest, country
where ninelowest.cid = country.cid
order by cname ASC;

Insert into Query6 (cid, cname)
select * 
from endresult;

drop view endresult cascade;
drop view ninelowest cascade;
drop view nineten cascade;
drop view teneleven cascade;
drop view tweleven cascade;
drop view thirtwelve cascade;



-- Query 7 statements

create view rel as
select rid, rname, SUM(rpercentage * population) as followers
from religion, country
where religion.cid = country.cid
group by rid, rname
order by followers ASC;

Insert into Query7 (rid, rname, followers)
select * 
from rel;


drop view rel cascade;



-- Query 8 statements

create view new1 as

select lang1.cid as c1id, lang2.cid as c2id, lang1.lname 
from language as lang1
join language as lang2
on lang1.lid = lang2.lid and lang1.cid <> lang2.cid;

create view other as
select cname as c1name, c2id, lname
from country join new1
on new1.c1id = country.cid;

Insert Into Query8
select c1name, cname as c2name, lname
from country join other
on other.c2id = country.cid;


drop view other cascade;
drop view new1 cascade;



-- Query 9 statements



-- Query 10 statements

create view bordercount as
select country, SUM(length) as borderslength
from neighbour
group by country;

create view answerq as
select cname, borderslength
from bordercount, country
where bordercount.country = country.cid
order by borderslength DESC
LIMIT 1;

Insert into Query10 (cname, borderslength)
select * 
from answerq;

drop view answerq cascade;
drop view bordercount cascade;

