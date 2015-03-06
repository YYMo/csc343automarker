-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

create view countryAndNeighborHeight as
select neighbour.country as c1id, neighbour.neighbor as c2id,
country.cname as c2name, country.height as neighborHeight
from country
right join neighbour
on neighbour.neighbor = country.cid;

create view countryWithMaxHeight as
select c1id, max(neighborHeight) as maxHeight
from countryAndNeighborHeight
group by c1id;

create view countryAndMaxNeighborHeight as
select countryWithMaxHeight.c1id, countryAndNeighborHeight.c2id,
countryAndNeighborHeight.c2name
from countryWithMaxHeight
left join countryAndNeighborHeight
on countryWithMaxHeight.c1id = countryAndNeighborHeight.c1id
where countryWithMaxHeight.maxHeight = countryAndNeighborHeight.neighborHeight;

insert into Query1(
select countryAndMaxNeighborHeight.c1id, country.cname as c1name, 
countryAndMaxNeighborHeight.c2id, countryAndMaxNeighborHeight.c2name
from countryAndMaxNeighborHeight
left join country
on country.cid = countryAndMaxNeighborHeight.c1id
order by c1name ASC);

drop view countryAndMaxNeighborHeight;
drop view countryWithMaxHeight;
drop view countryAndNeighborHeight;

-- Query 2 statements

create view countryAndOcean as
select count(oceanAccess.oid) as numOcean, country.cid
from oceanAccess
natural right join country
group by cid;

insert into Query2(
select countryAndOcean.cid, country.cname
from countryAndOcean 
join country
on country.cid = countryAndOcean.cid
where countryAndOcean.numOcean = 0
order by cname ASC);

drop view countryAndOcean;

-- Query 3 statements

create view countryAndOcean as
select count(oceanAccess.oid) as numOcean, country.cid
from oceanAccess
natural right join country
group by cid;

create view landlocked as
select countryAndOcean.cid
from countryAndOcean 
join country
on country.cid = countryAndOcean.cid
where countryAndOcean.numOcean = 0;

create view countryAndNumNeighbor as
select count(neighbor) as numNeighbor, country 
from neighbour
group by country;

create view countryAndOneNeighbor as
select countryAndNumNeighbor.country as c1id, neighbour.neighbor as c2id
from countryAndNumNeighbor
join neighbour
on neighbour.country = countryAndNumNeighbor.country
where numNeighbor = 1;

create view landlockedAndOneNeighbor as
select countryAndOneNeighbor.c1id, countryAndOneNeighbor.c2id
from landlocked
inner join countryAndOneNeighbor
on countryAndOneNeighbor.c1id = landlocked.cid;

create view Query3Withc2name as
select landlockedAndOneNeighbor.c1id, landlockedAndOneNeighbor.c2id,
country.cname as c2name
from country
join landlockedAndOneNeighbor
on country.cid = c2id;

insert into Query3(
select Query3Withc2name.c1id, country.cname as c1name,
Query3Withc2name.c2id, Query3Withc2name.c2name
from country
join Query3Withc2name
on country.cid = c1id
order by c1name ASC);

drop view Query3Withc2name;
drop view landlockedAndOneNeighbor;
drop view countryAndOneNeighbor;
drop view countryAndNumNeighbor;
drop view landlocked;
drop view countryAndOcean;

-- Query 4 statements

create view indirectOcean as
select neighbour.country as cid, oceanAccess.oid
from neighbour
inner join oceanAccess
on neighbour.neighbor = oceanAccess.cid;

create view accessOcean as 
select cid, oid
from indirectOcean
union 
select cid, oid
from oceanAccess;

insert into Query4(
select country.cname, ocean.oname
from accessOcean
join country
on country.cid = accessOcean.cid
join ocean
on ocean.oid = accessOcean.oid
order by cname ASC, oname DESC);

drop view accessOcean;
drop view indirectOcean;

-- Query 5 statements

create view avg_hdi as
select country.cid, avg(hdi.hdi_score) as avghdi
from hdi
natural join country
where year < 2014 and year >2008
group by country.cid
order by avghdi DESC
limit 10;

insert into Query5 (
select avg_hdi.cid, country.cname, avg_hdi.avghdi
from avg_hdi
natural join country
order by avghdi DESC);

drop view avg_hdi;

-- Query 6 statements

create view increasingYears as
select country1.cid, country1.year
from hdi as country1, hdi as country2
where country1.cid = country2.cid
and country1.hdi_score < country2.hdi_score
and country1.year = country2.year - 1
and country1.year >2008 and country1.year<2014
and country2.year >2008 and country2.year<2014;

create view numIncreasing as
select cid, count(year) as numIncrease
from increasingYears
group by cid;

insert into Query6(
select numIncreasing.cid, country.cname
from numIncreasing
natural join country
where numIncreasing.numIncrease = 4
order by cname ASC);

drop view numIncreasing;
drop view increasingYears;

-- Query 7 statements

insert into Query7(
select rid, rname, rpercentage*population as followers 
from religion 
join country 
using (cid) 
order by followers desc);

-- Query 8 statements

create view popularity as
select cid, max(lpercentage) as mostPop
from language
group by cid;

create view popularLang as 
select language.cid, language.lid, language.lname
from popularity
join language
on language.cid = popularity.cid
where language.lpercentage = popularity.mostPop;

create view NeighboursLang as
select neighbour.country as c1id, neighbour.neighbor as c2id,
popularLang.lid, popularLang.lname, country.cname as c2name
from neighbour
join popularLang
on neighbour.neighbor = popularLang.cid
join country
on country.cid = popularLang.cid;

create view languageTuple as
select NeighboursLang.c1id, NeighboursLang.c2id, 
NeighboursLang.c2name, popularLang.lname
from popularLang 
join NeighboursLang
on popularLang.cid = NeighboursLang.c1id
where popularLang.lid = NeighboursLang.lid;

insert into Query8(
select country.cname as c1name, languageTuple.c2name,
languageTuple.lname
from country
join languageTuple
on languageTuple.c1id = country.cid
order by languageTuple.lname ASC, country.cname DESC);

drop view languageTuple;
drop view NeighboursLang;
drop view popularLang;
drop view popularity;

-- Query 9 statements

create view deepestOcean as
select oceanAccess.cid, max(ocean.depth) as deepest
from oceanAccess
join ocean
on oceanAccess.oid = ocean.oid
group by oceanAccess.cid;

create view toZero as
select country.cid, country.cname, 
coalesce(deepestOcean.deepest, 0) as deepest1
from country
left join deepestOcean
on country.cid = deepestOcean.cid;

insert into Query9(
select toZero.cname, (toZero.deepest1+country.height) as totalspan
from toZero
join country
on toZero.cid = country.cid);

drop view toZero;
drop view deepestOcean;

-- Query 10 statements

create view longest as
select country, sum(length) as borderslength
from neighbour
group by country
order by borderslength DESC
limit 1;

insert into Query10(
select country.cname, longest.borderslength
from longest
join country
on longest.country = country.cid);

drop view longest;
