-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
create view c3 as (select neighbour.country as cid, neighbour.neighbor as neighbor,country.cname as cname, country.height as height from country join neighbour on neighbor=cid);
create view mx as (select cid, max(height) as mx from c3 group by  c3.cid);
create view c2 as (select c3.cid as cid,neighbor, cname from c3 join mx on c3.cid=mx.cid and c3.height=mx);
INSERT INTO Query1 (select country.cid as c1id,country.cname as c1name, c2.neighbor as c2id,c2.cname as c2name from c2,country where country.cid=c2.cid order by c1name asc); 
DROP view IF EXISTS c3 CASCADE;
DROP view IF EXISTS mx CASCADE;
DROP view IF EXISTS c2 CASCADE;

-- Query 2 statements
create view c1 as ((select country.cid as cid, country.cname as cname from country order by cname asc) except (select country.cid,country.cname from country natural join oceanAccess order by cname asc));
INSERT INTO Query2 (select * from c1 order by cname asc);
DROP view IF EXISTS c1 CASCADE;


-- Query 3 statements
create view c1 as (select * from Query2 join neighbour on Query2.cid=neighbour.country);
create view landlock as (select c1.cid as c1id, c1.cname as c1name, c1.neighbor as c2id, country.cname as c2name from c1 join country on c1.neighbor=country.cid);
create view a1 as (select * from country join neighbour on country.cid=neighbour.country);
create view neighbor1 as (select a1.cid as c1id, a1.cname as c1name, a1.neighbor as c2id, country.cname as c2name from a1 join country on a1.neighbor=country.cid);
create view neighbor2 as(select * from neighbor1);
create view notoneneighbor as (select neighbor1.c1id, neighbor1.c1name, neighbor1.c2id, neighbor1.c2name from neighbor1 join neighbor2 on neighbor1.c1id=neighbor2.c1id and neighbor1.c2id != neighbor2.c2id);
create view answer as (select * from landlock where landlock.c1id not in (select c1id from notoneneighbor) order by c1name ASC);
INSERT INTO Query3(select * from answer);
DROP view IF EXISTS c1 CASCADE;
DROP view IF EXISTS landlock CASCADE;
DROP view IF EXISTS a1 CASCADE;
DROP view IF EXISTS neighbor1 CASCADE;
DROP view IF EXISTS neighbor2 CASCADE;
DROP view IF EXISTS notoneneighbor CASCADE;
DROP view IF EXISTS answer CASCADE;

-- Query 4 statements
create view acco as (select neighbour.country as cid, oceanAccess.oid as oid from neighbour,oceanAccess where neighbour.country=oceanAccess.cid or neighbour.neighbor = oceanAccess.cid);
create view a1 as (select country.cname as cname, acco.oid as oid from acco natural join country);
create view answer as (select cname, ocean.oname as oname from a1 natural join ocean);
INSERT INTO Query4 (select Distinct * from answer order by cname ASC,oname DESC);
DROP view IF EXISTS acco CASCADE;
DROP view IF EXISTS answer CASCADE;
DROP view IF EXISTS a1 CASCADE;


-- Query 5 statements
create view c1 as (select cid ,cname, hdi_score, hdi.year from hdi natural join country where hdi.year>=2009 and hdi.year<=2013);
create view c2 as (select cid, cname, avg(hdi_score) as avghdi from c1 group by cid,cname );
create view mx as (select cid, cname, avghdi from c2 order by avghdi DESC limit 10);
INSERT INTO Query5 (select * from mx);
DROP view IF EXISTS c1 CASCADE;
DROP view IF EXISTS c2 CASCADE;
DROP view IF EXISTS mx CASCADE;


-- Query 6 statements
create view a1 as (select * from hdi);
create view a2 as (select * from hdi);
create view c1 as (select a1.cid as cid from a1, a2 where a1.cid =a2.cid and a1.year>a2.year and a1.hdi_score< a2.hdi_score);
create view answer as ((select cid,cname from country) except (select cid,cname from c1 natural join country order by cname ASC ));
INSERT INTO Query6 (select * from answer);
DROP view IF EXISTS a1 CASCADE;
DROP view IF EXISTS a2 CASCADE;
DROP view IF EXISTS c1 CASCADE;
DRop view IF EXISTS answer CASCADE;

-- Query 7 statements
create view a1 as (select cid, rid , rname, population*rpercentage as follower from country natural join religion);
create view a2 as (select rid, rname, sum(follower) as followers from a1 group by rid, rname order by followers DESC);
INSERT INTO Query7 (select * from a2);
Drop view IF EXISTS a1 CASCADE;
DROP view IF EXISTS a2 CASCADE;


-- Query 8 statements
create view a1 as (select max(lpercentage) as mx, cid from language group by cid);
create view a2 as (select cid,lid,lname from a1 natural join language where lpercentage=mx );
create view a3 as (select * from a2);
create view c1 as (select a2.cid as c1id,a3.cid as c2id,a2.lname as lname from a2,a3 where (a2.cid,a3.cid) in (select country,neighbor from neighbour) and a2.lid=a3.lid);
create view c2 as (select country.cname as c1name, c2id, lname from c1,country where c1id=cid);
create view c3 as (select c1name, country.cname as c2name, lname from c2,country where c2id=cid order by lname ASC, c1name DESC);
INSERT INTO Query8 (select * from c3);
DROP view if exists a1 CASCADE;
drop view if exists a2 CASCADE;
drop view if exists a3 CASCADE;
drop view if exists c1 CASCADE;
drop view if exists c2 CASCADE;
drop view if exists c3 CASCADE;


-- Query 9 statements
create view a1 as (select cid,cname, height from country where cid in (select cid from oceanAccess));
create view a2 as (select cid,cname,height from country where cid not in (select cid from oceanAccess));
create view c1 as (select cid,cname, height as totalspan from a2);
create view a3 as (select cid,cname, oid,height from a1 natural join oceanAccess);
create view c2 as (select cid, cname, height+depth as totalspan from a3 natural join ocean);
create view ca as ((select * from c1) union (select * from c2));
create view mx as (select max(totalspan) as mx from ca);
INSERT INTO Query9 (select cname, totalspan from ca,mx where totalspan=mx.mx);
drop view if exists a1 CASCADE;
drop view if exists a2 CASCADE;
drop view if exists c1 CASCADE;
drop view if exists c2 CASCADE;
drop view if exists ca CASCADE;
drop view if exists mx CASCADE;


-- Query 10 statements
create view a1 as (select neighbour.country as cid, sum(neighbour.length) as borderslength from neighbour group by neighbour.country);
create view mx as (select max(borderslength) as mx from a1);
create view c1 as (select cid,borderslength from a1,mx where a1.borderslength=mx.mx);
create view c2 as (select cname,borderslength from c1 natural join country);
INSERT INTO Query10 (select * from c2);
drop view if exists a1 CASCADE;
drop view if exists mx CASCADE;
drop view if exists c1 CASCADE;
drop view if exists c2 CASCADE;
