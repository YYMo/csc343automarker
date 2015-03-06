-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

create view n1 as select country.cname as c1name, neighbour.country as c1id, neighbour.neighbor as c2id from country join neighbour on country.cid = neighbour.country order by c1name;

create view h1 as select c1name, c1id, country.cname as c2name, country.cid as c2id, country.height as h from n1 join country on n1.c2id = country.cid;

create view q1 as select h1.c1name, c1id, c2name, c2id from (select c1name, max(h) from h1 group by c1name) as h2 join h1 on h2.c1name = h1.c1name where h1.h = h2.max;

insert into Query1 select c1id, c1name, c2id, c2name from q1 order by c1name asc;

drop view n1 cascade;



-- Query 2 statements

insert into Query2 select cid, cname from country where not exists (select * from oceanAccess where country.cid = oceanAccess.cid) order by cname asc;

-- Query 3 statements

create view only1 as select country from (select * from (select country from neighbour group by country having count(*) = 1) o1) as once where not exists (select * from oceanAccess where once.country = oceanAccess.cid) order by country;

create view only1name as select only1.country as c1id, country.cname as c1name from only1 join country on only1.country = country.cid;

create view o1 as select only1name.c1id, only1name.c1name, neighbour.neighbor from only1name join neighbour on only1name.c1id = neighbour.country;

create view q3 as select c1id, c1name, neighbor as c2id, cname as c2name from o1 join country on o1.neighbor = country.cid;

insert into Query3 select c1id, c1name, c2id, c2name from q3 order by c1name asc;

drop view only1 cascade;


-- Query 4 statements

create view n1 as select country.cname, ocean.oname from oceanAccess join neighbour on oceanAccess.cid = neighbour.country join ocean on ocean.oid = oceanAccess.oid join country on country.cid = neighbour.neighbor;

create view n2 as select country.cname, ocean.oname from oceanAccess join country on country.cid = oceanAccess.cid join ocean on ocean.oid = oceanAccess.oid;

insert into query4 select * from n1 order by cname asc, oname desc;
insert into query4 select * from n2 order by cname asc, oname desc;

drop view n1 cascade;
drop view n2 cascade;


-- Query 5 statements

create view h1 as select cid, avg(hdi_score) from hdi where year >= 2009 and year <= 2013 group by cid order by avg desc limit 10;

insert into query5 select h1.cid, country.cname, h1.avg as avghdi from h1 join country on country.cid = h1.cid order by avghdi desc;

drop view h1;

-- Query 6 statements

create view hdi09 as select * from hdi where year = 2009;
create view hdi10 as select * from hdi where year = 2010;
create view hdi11 as select * from hdi where year = 2011;
create view hdi12 as select * from hdi where year = 2012;
create view hdi13 as select * from hdi where year = 2013;

create view allH as select hdi09.cid, hdi09.year as year9, hdi09.hdi_score as score9, hdi10.year as year10, hdi10.hdi_score as score10, hdi11.year as year11, hdi11.hdi_score as score11,hdi12.year as year12, hdi12.hdi_score as score12, hdi13.year as year13, hdi13.hdi_score as score13 from hdi09 join hdi10 on hdi09.cid = hdi10.cid join hdi11 on hdi11.cid = hdi10.cid join hdi12 on hdi12.cid = hdi11.cid join hdi13 on hdi13.cid = hdi12.cid;

create view rising as select cid from allH where score9 < score10 and score10 < score11 and score11 < score12 and score12 < score13;

insert into query6 select rising.cid, country.cname from rising join country on rising.cid = country.cid order by cname asc; 

drop view hdi09 cascade;
drop view hdi10 cascade;
drop view hdi11 cascade;
drop view hdi12 cascade;
drop view hdi13 cascade;

-- Query 7 statements

create view r1 as select rid, rname, (rpercentage*country.population) as followers from religion join country on religion.cid = country.cid;

create view r2 as select rid, sum(followers) as followers from r1 group by rid order by rid;

insert into query7 select r2.rid, religion.rname, followers from r2 join religion on r2.rid = religion.rid order by followers desc;

drop view r1 cascade;


-- Query 8 statements

create view l1 as select cid, max(lpercentage) from language group by cid order by max;

create view l2 as select l1.cid, language.lid, l1.max from l1 join language on l1.cid = language.cid where l1.max = language.lpercentage;

create view l4 as select neighbour.country, l3.lid as lidc, l3.max as maxc, neighbour.neighbor, l2.lid as lidn, l2.max as maxn from l2 join neighbour on l2.cid = neighbour.neighbor join l2 as l3 on l3.cid = neighbour.country order by country;

insert into query8 select distinct country.cname as c1name, country2.cname as c2name, language.lname from l4 join country on l4.country = country.cid join country as country2 on l4.neighbor = country2.cid join language on l4.lidc = language.lid where lidc = lidn order by lname asc, c1name desc;

drop view l1 cascade;

-- Query 9 statements

create view o1 as select oceanAccess.cid, oceanAccess.oid, country.height, ocean.depth from oceanAccess join country on oceanAccess.cid = country.cid join ocean on oceanAccess.oid = ocean.oid;

create view o2 as select max(abs), cid from ((select abs(depth - height) as abs, cid from o1) union (select height as abs, cid from country)) as o2 group by cid;

create view o3 as select max(max) from o2;

create view o4 as select country.cname, o4.max as totalspan from (select cid, o3.max from o2 join o3 on o2.max = o3.max) as o4 join country on o4.cid = country.cid;

insert into query9 select * from o4;

drop view o1 cascade;

-- Query 10 statements

create view b1 as select country, sum(length) from neighbour group by country order by country;

create view b2 as select max(sum) from b1;

create view b3 as select country, sum from b1 join b2 on b1.sum = b2.max;

create view b4 as select cname, sum as borderslength from b3 join country on country.cid = b3.country;

insert into query10 select * from b4;

drop view b1 cascade;

