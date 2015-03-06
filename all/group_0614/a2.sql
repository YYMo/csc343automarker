
-- Query 1 statements
create view b as select * from country join neighbour on cid = neighbor;

insert into Query1 (select country.cid, country.cname,  b.cid, b.cname from
  country, b where b.cname != country.cname and country.cid = b.country and
   b.height in (select max(b.height) from country, b where b.cname !=
    country.cname and country.cid = b.country group by country.cid));

drop view b cascade;

-- Query 2 statements
create view a as select country.cid from oceanAccess join country on
   oceanAccess.cid = country.cid;
create view b as select cid from country;
create view c as select * from b EXCEPT select * from a;
insert into Query2 (select c.cid, country.cname from c join country on
   c.cid = country.cid order by country.cname);

drop view a, b, c cascade;

-- Query 3 statements
create view a as select country.cid from oceanAccess join country on
   oceanAccess.cid = country.cid;
create view b as select cid from country;
create view c as select * from b EXCEPT select * from a;
create view landlockedcountries as select c.cid, country.cname from c join
   country on c.cid = country.cid;
--select * from landlockedcountries join neighbour on landlockedcountries.cid
-- = neighbour.country;
create view landlockedneighbors as select * from landlockedcountries join
  neighbour on landlockedcountries.cid = neighbour.country;
create view result as select cid, cname, count(neighbor) from
   landlockedneighbors group by cid, cname having count(neighbor) < 2;
create view result2 as select * from result join neighbour on result.cid
   = neighbour.country;

insert into Query3 (select country.cid, result2.cname, result2.cid as
   neighbor, country.cname from result2 join country on result2.neighbor
    = country.cid);

drop view a, b, c, landlockedcountries, landlockedneighbors, result, result2 cascade;

-- Query 4 statements
--Result A:
create view oceans as select ocean.oid, ocean.oname, oceanAccess.cid from
   ocean join oceanAccess on ocean.oid = oceanAccess.oid;
create view resultA as select oceans.oid, country.cname from oceans join
   country on oceans.cid = country.cid;
create view resultD as select resultA.cname, ocean.oname from resultA join
   ocean on resultA.oid = ocean.oid;

--Result B:

create view b as select * from neighbour join oceans on neighbour.neighbor
   = oceans.cid;

create view resultB as select country.cname, b.oname from country join b on
   b.country = country.cid;

--final result:
create view finalResult as select * from resultD UNION select * from resultB;

insert into Query4 (select * from finalResult order by cname, oname desc);

drop view oceans, resultA, resultD, b, resultB cascade;

-- Query 5 statements

--select country.cid, hdi.year, hdi.hdi_score from country join hdi on hdi.cid = country.cid;

create view a as select country.cid, hdi.year, hdi.hdi_score from country join
   hdi on hdi.cid = country.cid where hdi.year >= 2009 and hdi.year <= 2013;

create view b as select cid, avg(hdi_score) from a group by cid;

create view c as select * from b limit 10;

insert into Query5 (select c.cid, country.cname, c.avg from country join c on
   country.cid = c.cid order by avg desc);

drop view a, b, c cascade;


-- Query 6 statements
create or replace view table1 as select a.cid from hdi a, hdi b, hdi c, hdi d,
  hdi e where a.cid = b.cid and a.cid = c.cid and a.cid = d.cid and a.cid =
   e.cid and a.year between 2009 and 2013 and b.year between 2009 and 2013 and
    c.year between 2009 and 2013 and d.year between 2009 and 2013 and e.year
     between 2009 and 2013 and a.year < b.year and b.year < c.year and c.year
      < d.year and d.year < e.year and a.hdi_score < b.hdi_score and
       b.hdi_score < c.hdi_score and c.hdi_score < d.hdi_score and
        d.hdi_score < e.hdi_score;

insert into Query6 (select country.cid as cid, cname from country join table1
   on country.cid = table1.cid order by cname asc);

drop view table1;

-- Query 7 statements
create or replace view table1 as select cname, rid, rname, (rpercentage
   * population)/100 as follow from country join religion on country.cid
    = religion.cid;

create or replace view table2 as select rname, sum(follow) as followers
   from table1 group by rname;

insert into Query7 (select distinct table1.rid as rid, table2.rname as rname, table2.followers
 from table2 join table1 on table2.rname = table1.rname order by followers desc);

drop view table1, table2 cascade;

-- Query 8 statements
create or replace view table2 as select country.cid, cname, lname, (population
  * lpercentage)/100 as tpopulation from country join language on country.cid
   = language.cid;

create or replace view table3 as select table2.cid, cname, lname, neighbor,
   tpopulation from table2 join neighbour on cid = country;

create or replace view table4 as select table2.cid, cname, lname, neighbor,
   tpopulation from table2 join neighbour on cid = neighbor;

insert into Query8 (select table3.cname as c1name, table4.cname as c2name, table3.lname as lname
 from table3 join table4 on table3.neighbor = table4.neighbor and
 table3.tpopulation = table4.tpopulation order by lname ASC, c1name DESC);

drop view table2, table3, table4 cascade;

-- Query 9 statements
create view oceans as select ocean.oid, ocean.oname, oceanAccess.cid from ocean
   join oceanAccess on ocean.oid = oceanAccess.oid;

create view resultA as select oceans.oid, country.cname from oceans join country
   on oceans.cid = country.cid;

create view resultE as select resultA.cname, ocean.oname, ocean.depth from
   resultA join ocean on resultA.oid = ocean.oid;

create view resultD as select cname, max(depth) from resultE group by cname;

create view countriesOceanAccess as select distinct cname from resultA;

create view resultL as select resultD.cname, resultD.max, country.height from
   resultD join country on resultD.cname = country.cname;

create view countriesNoOceanAccess as select cname from country where cname
   not in(select * from countriesOceanAccess);


create view zero as select countriesNoOceanAccess.cname, 0 as max,
   country.height from countriesNoOceanAccess join country on country.cname
    = countriesNoOceanAccess.cname;

--combine:
create view semiResult as select * from zero union select * from resultL;

--result:
create view result as select semiResult.cname, semiResult.height+semiResult.max
   as Difference from semiResult;

insert into Query9 (select cname, difference from result where difference
   >= All (select max(difference) from result));

drop view oceans, resultA, resultE, resultD, countriesOceanAccess, resultL, countriesNoOceanAccess, zero, semiResult, result cascade;
-- Query 10 statements
create view semiResult as select * from country join neighbour on country.cid = neighbour.country;

create view resultA as select semiResult.cname, sum(semiResult.length) as totallength from semiResult group by semiResult.cname;

insert into Query10 (select cname, totallength from resultA where totallength >= All (select max(totallength) from resultA));