-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.
set search_path to A2;
-- Query 1 statements
insert into Query1(select a.cid as c1id, a.cname as c1name, b.cid as c2id, b.cname as c2name from country as a cross join country as b where (a.cid, b.height) in (select c1.cid, max(c2.height) from country as c1 cross join country as c2 where c1.cid <> c2.cid and exists(select * from neighbour where neighbor = c2.cid and country = c1.cid) group by c1.cid order by c1.cid) order by c1name ASC);

-- Query 2 statements
insert into Query2(select a.cid as cid, a.cname as cname from country as a where not exists(select * from country natural join oceanaccess where a.cid=cid) order by cname ASC);

-- Query 3 statements
CREATE VIEW view1 as (select a.cid as cid, a.cname as cname from country as a where not exists(select * from country natural join oceanaccess where a.cid=cid) order by cname ASC);
insert into Query3(select c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name from country as c1 cross join country as c2 where (c1.cid) in  (select country from neighbour group by country having count(country) = 1) and exists (select country, neighbor from neighbour where c1.cid = country and c2.cid = neighbor) and (c1.cid, c1.cname) in (select * from view1) order by c1name ASC);
drop view view1;
-- Query 4 statements
create view access as (select country, cid, oid from neighbour cross join oceanaccess where neighbor=cid order by country ASC, oid DESC);
insert into Query4(select distinct cname, oname from access as a cross join country as b cross join ocean as c where (a.cid=b.cid or a.country=b.cid) and c.oid=a.oid order by cname ASC, oname DESC);drop view access;
-- Query 5 statements
create view ahdi as (select cid, avg(hdi_score) from hdi where year >= 2009 and year <= 2013 group by cid order by avg(hdi_score) DESC limit 10);
insert into Query5(select a.cid, b.cname, avg as avghdi from ahdi as a cross join country as b where a.cid=b.cid order by avghdi DESC);
drop view ahdi;

-- Query 6 statements
create view increasing as (select a.cid from hdi as a cross join hdi as b cross join hdi as c cross join hdi as d cross join hdi as e where a.cid = b.cid and b.cid = c.cid and c.cid = d.cid and d.cid = e.cid and a.year = 2009 and b.year = 2010 and c.year = 2011 and d.year = 2012 and e.year = 2013 and a.hdi_score < b.hdi_score and b.hdi_score < c.hdi_score and c.hdi_score < d.hdi_score and d.hdi_score < e.hdi_score);
insert into Query6(select a.cid, b.cname from increasing as a cross join country as b where a.cid = b.cid order by b.cname ASC);
drop view increasing;
-- Query 7 statements
create view religions as (select b.rname,sum(rpercentage*population) as followers from country as a cross join religion as b where a.cid = b.cid group by b.rname );
insert into Query7(select distinct b.rid, a.rname, a.followers from religions as a cross join religion as b where a.rname = b.rname order by followers DESC);
drop view religions;

-- Query 8 statements
create view maxPercent as (select cid, max(lpercentage) from language group by cid);
create view maxLanguage as (select a.cid, b.lid from maxPercent as a cross join language as b where a.cid=b.cid and a.max=b.lpercentage order by a.cid);
create view idToName as (select a.country, a.neighbor, m1.lid from maxLanguage as m1 cross join maxLanguage as m2 cross join neighbour as a where a.country = m1.cid and a.neighbor = m2.cid and m1.lid=m2.lid order by m1.lid ASC, a.country DESC);
insert into Query8(select distinct a.cname as c1name, b.cname as c2name, c.lname from country as a cross join country as b cross join language as c cross join idToName as d where d.country=a.cid and d.neighbor=b.cid and d.lid=c.lid order by lname ASC, c1name DESC);
drop view idToName;
drop view maxLanguage;
drop view maxPercent;

-- Query 9 statements
create view noOcean as (select a.cname as cname, a.height as totalspan from country as a where not exists(select * from country natural join oceanaccess where a.cid=cid) order by cname ASC);
create view withOcean as (select cname, max(abs(depth-height)) as totalspan from oceanaccess natural join ocean natural join country group by cname order by cname ASC);
create view finalData as ((select * from withOcean) union (select * from noOcean) order by totalspan DESC);
insert into Query9(select * from finalData where totalspan in (select totalspan from finalData limit 1) order by cname ASC);
drop view finalData;
drop view withOcean;
drop view noOcean;

-- Query 10 statements
create view borderLengths as (select country as cid, sum(length) as borderslength from neighbour group by cid order by borderslength DESC);
insert into Query10(select cname, borderslength from borderLengths as b cross join country as a where a.cid=b.cid and borderslength in (select borderslength from borderLengths limit 1));
drop view borderLengths;