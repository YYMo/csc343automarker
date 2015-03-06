-- Add below your SQL statements.
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
delete from query1;

create view neighborHeight as select country, neighbor, height from neighbour n join country c on n.neighbor=c.cid;
create view notHighest as select n1.country, n1.neighbor from neighborHeight n1 join neighborHeight n2 on n1.country=n2.country and n1.height<n2.height;
create view highest as select country, neighbor from neighbour n where not exists (select * from notHighest no where n.country=no.country and n.neighbor=no.neighbor);
insert into query1 (select country as c1id, country.cname as c1name, neighbor as c2id, c2.cname as c2name from highest join country on country=cid join country c2 on neighbor=c2.cid order by c1id);

drop view neighborHeight cascade;

-- Query 2 statements
delete from query2;

insert into query2 (select cid, cname from country c where not exists (select * from oceanaccess o where c.cid=o.cid) order by cname);

-- Query 3 statements
delete from query3;

create view llNeighbor as select * from query2 join neighbour on cid=country;
create view notLone as select l.cid from llNeighbor l join llNeighbor ll on l.cid=ll.cid and l.neighbor<ll.neighbor;
create view lonecountry as select * from llNeighbor where not exists (select * from notlone n where llNeighbor.cid=n.cid);
insert into query3 (select l.cid, l.cname, c.cid, c.cname from lonecountry l join country c on l.neighbor=c.cid order by l.cname);

drop view llNeighbor cascade;

-- Query 4 statements
delete from query4;

create view fullAccess as select * from neighbour n join oceanaccess o on n.neighbor=o.cid;
create view suchCountries as (select country as cid, oid from fullAccess) union (select cid, oid from oceanaccess);
insert into query4 (select c.cname, o.oname from suchcountries s join country c on s.cid=c.cid join ocean o on s.oid=o.oid order by cname, oname desc);

drop view fullAccess cascade;

-- Query 5 statements
delete from query5;

create view intime as select * from hdi where year>2008 and year<2014;
create view top10 as select cid,avg(hdi_score) as avghdi from intime group by cid order by avghdi desc limit 10;
insert into query5 (select country.cid,cname,avghdi from top10 join country on top10.cid=country.cid order by avghdi desc);

drop view intime cascade;

-- Query 6 statements
delete from query6;

create view intime as select * from hdi where year>2008 and year<2014;
create view failed as select i.cid from intime i join intime ii on i.cid=ii.cid and i.year<ii.year and i.hdi_score>ii.hdi_score;
create view success as select * from intime i where not exists (select * from failed f where f.cid=i.cid);
insert into query6 (select c.cid, c.cname from country c where exists (select * from success s where s.cid=c.cid) order by c.cname);

drop view intime cascade;

-- Query 7 statements
delete from query7;

create view total as select rid, rname, rpercentage*population as followers from religion r join country c on r.cid=c.cid;
insert into query7 (select rid, rname, sum(followers) as followers from total group by rid, rname order by followers desc);

drop view total cascade;

-- Query 8 statements
delete from query8;

create view notMostPop as select l.cid, l.lid, l.lname from language l join language ll on l.cid=ll.cid and l.lpercentage<ll.lpercentage;
create view mostPop as select * from language l where not exists (select * from notMostPop n where l.cid=n.cid and l.lid=n.lid);
create view lneighbor as select country, neighbor, m.lid as lid1, mm.lid as lid2 from neighbour n join mostPop m on n.country=m.cid join mostPop mm on n.neighbor=mm.cid;
create view nameLanguage as select c.cname as c1name, cc.cname as c2name, lid1 from (select * from lneighbor where lid1=lid2) as same join country c on same.country=c.cid join country cc on same.neighbor=cc.cid;
insert into query8 (select c1name, c2name, lname from nameLanguage n join (select lid, lname from language group by lid, lname) as ls on n.lid1=ls.lid order by lname, c1name desc);

drop view notMostPop cascade;

-- Query 9 statements
delete from query9;

create view seaDepth as select c.cid, c.height, c.cname, case when oc.depth is NULL then 0 else oc.depth end from oceanaccess o join ocean oc on o.oid=oc.oid right outer join country c on o.cid=c.cid;
create view notDeepest as select s.cid, s.depth from seaDepth s join seaDepth ss on s.cid=ss.cid and s.depth<ss.depth;
create view deepest as select * from seaDepth s where not exists (select * from notDeepest n where s.cid=n.cid and s.depth=n.depth);
insert into query9 (select cname, (height+depth) as totalspan from deepest);

drop view seaDepth cascade;

-- Query 10 statements
delete from query10;

create view totalLength as select country, sum(length) from neighbour group by country;
create view notLongest as select t.country from totalLength t join totalLength tt on t.sum<tt.sum;
create view longest as select * from totalLength t where not exists (select * from notLongest n where t.country=n.country);
insert into query10 (select cname, sum as borderslength from longest join country on country=cid);

drop view totalLength cascade;