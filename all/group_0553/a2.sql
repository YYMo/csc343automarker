-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

--
-- PostgreSQL port of the MySQL "World" database.
--
-- The sample data used in the world database is Copyright Statistics 
-- Finland, http://www.stat.fi/worldinfigures.
--

Begin;

-- Query 1 statements
INSERT INTO Query1(select country.cid c1id, country.cname c1name, neighbour.country c2id, (select cname from country where neighbour.country=cid) c2name
from country  join neighbour on country.cid=neighbour.neighbor
order by c1name ASC);


-- Query 2 statements

create or replace view landlock1 as ((select cid  from country) except (select cid from oceanAccess));
INSERT INTO Query2(select country.cid, cname from landlock1 join country on landlock1.cid=country.cid order by cname ASC);
drop view landlock1 cascade; --Delect the view landlock

-- Query 3 statements

create or replace view landlock2 as ((select cid  from country) except (select cid from oceanAccess));

create or replace view landlockInfo as (select country.cid, cname from landlock2 join country on landlock2.cid=country.cid order by cname ASC);

create or replace view land as (select landlockInfo.cid, count(neighbour.country) 
from landlockInfo join neighbour on landlockInfo.cid=neighbour.neighbor group by landlockInfo.cid order by landlockInfo.cid);

create or replace view landWithOneNeighbour as (select cid from land where count=1);

INSERT INTO Query3(select landWithOneNeighbour.cid c1id, (select cname from country where landWithOneNeighbour.cid=cid) c1name, neighbour.country c2id, (select cname from country where cid=neighbour.country) c2name 
from landWithOneNeighbour join neighbour on landWithOneNeighbour.cid=neighbour.neighbor 
order by c1name ASC);

drop view landlock2 cascade;

-- Query 4 statements

--Temp1:
create view oceans as select ocean.oid, ocean.oname, oceanAccess.cid from
   ocean join oceanAccess on ocean.oid = oceanAccess.oid;

create view temp1 as select oceans.oid, country.cname from oceans join country on oceans.cid = country.cid;

create view temp2 as select temp1.cname, ocean.oname from temp1 join
   ocean on temp1.oid = ocean.oid;

--Temp3:

create view temp3 as select * from neighbour join oceans on neighbour.neighbor
   = oceans.cid;

create view temp4 as select country.cname, temp3.oname from country join temp3 on
   temp3.country = country.cid;

--result:
create view result as select * from temp2 UNION select * from temp4;

insert into Query4 (select * from result order by cname, oname desc);

drop view oceans, temp1, temp2, temp3, temp4, result cascade;

-- Query 5 statements
create view topTen as (select cid, avg(hdi_score) as "avghdi" from hdi where 2009<=year and year<=2013  group by cid limit 10);


insert into Query5(select topTen.cid, country.cname, topTen.avghdi 
from topTen join country on topTen.cid=country.cid order by topTen.avghdi desc);

-- Query 6 statements

-- Countries within the years 2009 - 2013 (inclusive).
create view countryWithinYear as (select * from hdi where 2009<=year and year<=2013);

insert into Query6 (select c1.cid, country.cname from countryWithinYear c1 join countryWithinYear c2 on c1.cid=c2.cid 
join countryWithinYear c3 on c2.cid=c3.cid join countryWithinYear c4 on c3.cid=c4.cid join 
countryWithinYear c5 on c4.cid=c5.cid join country on c1.cid=country.cid where c1.year=2009 and 
c2.year=2010 and c3.year=2011 and c4.year=2012 and c5.year=2013 and c1.hdi_score<c2.hdi_score and 
c2.hdi_score<c3.hdi_score and c3.hdi_score<c4.hdi_score and c4.hdi_score<c5.hdi_score 
order by country.cname ASC);

-- Query 7 statements

create view orderedReligion as (select rid, rname, population followers 
from country join religion on country.cid=religion.cid order by religion.rid);


-- get the total number of the population that follow each religion. 

 insert into Query7 (select rid, rname, sum(followers) followers from orderedReligion 
 group by rid, rname order by followers);

-- Query 8 statements

create view pop1 as select country.cid, cname, lname, (population
  * lpercentage)/100 as tpopulation from country join language on country.cid
   = language.cid;

create view pop2 as select pop1.cid, cname, lname, neighbor,
   tpopulation from pop1 join neighbour on cid = country;

create view pop3 as select pop1.cid, cname, lname, neighbor,
   tpopulation from pop1 join neighbour on cid = neighbor;

select pop2.cname as c1name, pop3.cname as c2name, pop2.lname as lname
from pop2 join pop3 on pop2.neighbor = pop3.neighbor and
pop2.tpopulation = pop3.tpopulation order by lname ASC, c1name DESC;

insert into Query8 (select pop2.cname as c1name, pop3.cname as c2name, pop2.lname as lname
from pop2 join pop3 on pop2.neighbor = pop3.neighbor and
pop2.tpopulation = pop3.tpopulation order by lname ASC, c1name DESC);

-- Query 9 statements
create or replace view table1 as select ocean.oid, ocean.oname, oceanAccess.cid from ocean
   join oceanAccess on ocean.oid = oceanAccess.oid;

create view partA as select table1.oid, country.cname from table1 join country
   on table1.cid = country.cid;

create or replace view oceanInfo as select partA.cname, ocean.oname, ocean.depth from
   partA join ocean on partA.oid = ocean.oid;

create or replace view oceanInfoWith as select cname, max(depth) from oceanInfo group by cname;

create view countriesOceanAccess as select distinct cname from partA;

create or replace view oceanInfoWithMax as select oceanInfoWith.cname, oceanInfoWith.max, country.height from
   oceanInfoWith join country on oceanInfoWith.cname = country.cname;

create or replace view countriesNoOceanAccess as select cname from country where cname
   not in(select * from countriesOceanAccess);


create or replace view res as select countriesNoOceanAccess.cname, 0 as max,
   country.height from countriesNoOceanAccess join country on country.cname
    = countriesNoOceanAccess.cname;

--combine:
create or replace view semiResult as select * from res union select * from oceanInfoWithMax;

-- final answer.
create or replace view output as select semiResult.cname, semiResult.height+semiResult.max
   as Difference from semiResult;

insert into Query9 (select cname, difference from output where difference
   >= All (select max(difference) from output));

drop view table1, partA, oceanInfo, oceanInfoWith, countriesOceanAccess, oceanInfoWithMax, countriesNoOceanAccess, res,semiResult, oceanInfoWithMax, output cascade;

-- Query 10 statements

create  or replace view sR as select * from country join neighbour on country.cid = neighbour.country;

create or replace view tA as (select sR.cname, sum(sR.length) as totallength from sR group by sR.cname);


insert into Query10 (select cname, totallength from tA where totallength >= All (select max(totallength) from tA));

drop view sR,tA cascade;

COMMIT;
