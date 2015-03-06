-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
create view allneighbours as select c1id, cname as c1name, c2id, c2name, h from (select country as c1id, neighbor as c2id, cname as c2name, height as h from country, neighbour where cid=neighbor) a, (select cid, cname from country) b where cid=c1id;
create view highest as select country, max(height) as tallest from country, neighbour where cid=neighbor group by country;
insert into Query1 (select c1id, c1name, c2id, c2name from allneighbours, highest where c1id=country and tallest=h order by c1name);
drop view allneighbours;
drop view highest;


-- Query 2 statements
insert into Query2(select cid, cname from country where cid not in(select cid from oceanAccess) order by c1name);


-- Query 3 statements
create view oneneighbour as select country from neighbour group by country having count(neighbor)=1;
create view landlock as select cid, cname from country where cid not in(select cid from oceanAccess);
create view temp as select cid as c1id, cname as c1name from oneneighbour, landlock where country=cid;
insert into Query3 (select c1id, c1name, c2id, c2name from temp, (select country, neighbor as c2id, cname as c2name from neighbour, country where cid=neighbor) a where country=c1id order by c1name);
drop view temp;
drop view oneneighbour;
drop view landlock;


-- Query 4 statements
create view indirect as select country as cid, oid from neighbour, oceanAccess where neighbor=cid;
create view temp as select * from oceanAccess union select * from indirect;
create view onames as select cid, oname from temp, ocean where temp.oid=ocean.oid;
create view names as select cname, oname from onames, country where onames.cid=country.cid;
create view result as select * from names order by cname ASC, oname DESC;
insert into Query4(cname, oname) select * from result;
drop view result;
drop view onames;
drop view names;
drop view temp;
drop view indirect;


-- Query 5 statements
create view inrange as select cid, hdi_score from hdi where year<=2013 and year>=2009;
create view average as select cid, avg(hdi_score) as avghdi from inrange group by cid;
create view result as select average.cid as cid, cname, avghdi from average, country where average.cid=country.cid order by avghdi DESC limit 10;
insert into Query5(cid, cname, avghdi) select * from result;
drop view result;
drop view average;
drop view inrange;


-- Query 6 statements
create view temp as select b.cid, b.hdi_score from (select cid, hdi_score from hdi where year=2009) a, (select cid, hdi_score from hdi where year=2010) b where a.cid=b.cid and a.hdi_score < b.hdi_score;
create view temp2 as select b.cid, b.hdi_score from temp a, (select cid, hdi_score from hdi where year=2011) b where a.cid=b.cid and a.hdi_score < b.hdi_score;
create view temp3 as select b.cid, b.hdi_score from temp2 a, (select cid, hdi_score from hdi where year=2012) b where a.cid=b.cid and a.hdi_score < b.hdi_score;
create view temp4 as select b.cid, b.hdi_score from temp3 a, (select cid, hdi_score from hdi where year=2013) b where a.cid=b.cid and a.hdi_score < b.hdi_score;
insert into Query6(select country.cid, cname from temp4, country where country.cid=temp4.cid order by cname);
drop view temp4;
drop view temp3;
drop view temp2;
drop view temp;


-- Query 7 statements
create view following as select rid, sum(population*rpercentage) as followers from country c, religion r where c.cid=r.cid group by rid;
insert into Query7(select  distinct r.rid, rname, followers from religion r, following f where r.rid=f.rid order by followers DESC);
drop view following;


-- Query 8 statements
create view mostspoken as select l.cid, l.lname from language l where l.lpercentage>=ALL(select lpercentage from language where l.cid=cid);
create view mostspokenpairs as select m.cid as c1id, s.cid as c2id, m.lname from mostspoken m, mostspoken s where m.lname=s.lname and m.cid<>s.cid;
create view languageneighbours as select c1id, c2id, lname from mostspokenpairs, neighbour where c1id=country and c2id=neighbor;
create view temp as select c1id, cname as c2name, lname from languageneighbours, country where cid=c2id;
insert into Query8(select cname as c1name, c2name, lname from temp, country where cid=c1id order by lname ASC, c1name DESC);
drop view temp;
drop view languageneighbours;
drop view mostspokenpairs;
drop view mostspoken;


-- Query 9 statements
create view deepest as select cid, max(depth) as deep from ocean o, oceanAccess a where o.oid=a.oid group by cid;
create view distance as select cname, (height+deep) as totalspan from deepest d, country c where c.cid=d.cid and (height+deep) >= ALL(select (height+deep) from deepest d, country c where c.cid=d.cid);
create view highest as select cname, height as totalspan from country where height >= ALL(select height from country);
insert into Query9(select * from distance where totalspan >= ALL(select totalspan from highest));
insert into Query9(select * from highest where totalspan >= ALL(select totalspan from distance));
drop view highest;
drop view distance;
drop view deepest;


-- Query 10 statements
create view sumlength as select country, sum(length) as borderslength from neighbour group by country;
create view maxlength as select country, borderslength from sumlength where borderslength >= ALL(select borderslength from sumlength);
insert into Query10(select cname, borderslength from maxlength, country where country=cid);
drop view maxlength;
drop view sumlength;

