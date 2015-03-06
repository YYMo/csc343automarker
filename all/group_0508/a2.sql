-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

create view T1 as
select country as c1id, neighbor as c2id, height
from country, neighbour
where neighbor = cid;

create view T2 as
select c1id, c2id
from T1
where height >= all(select height from T1 T1a  where T1a.c1id = T1.c1id);

create view GetName1 as
select c1id, cname as c1name, c2id
from T2, country
where T2.c1id = country.cid;

create view GetName2 as
select c1id, c1name, c2id, cname as c2name
from GetName1, country
where GetName1.c2id = country.cid;

insert into Query1(select * from GetName2 order by c1name asc);
drop view GetName2 cascade;
drop view GetName1 cascade;
drop view T2 cascade;
drop view T1 cascade;

-- Query 2 statements

create view AllCountries as
select cid
from country;

create view OceanBorder as
select cid
from oceanAccess;

create view GetNames as
select Landlocked.cid, cname
from ((select * from AllCountries) except (select * from OceanBorder)) Landlocked, country
where country.cid = Landlocked.cid;

insert into Query2(select * from GetNames order by cname ASC);
drop view GetNames cascade;
drop view OceanBorder cascade;
drop view AllCountries cascade;

-- Query 3 statements

create view AllCountries as
select cid 
from country;

create view OceanBorder as
select cid
from oceanAccess;

create view Exactly1NeighborCountry as 
select country as c1id
from Neighbour
group by country
having count(neighbor) = 1;
    
create view Exactly1Neighbor as
select c1id, neighbor as c2id
from Neighbour, Exactly1NeighborCountry
where country = c1id;

create view LandlockedCountry as
select Landlocked.cid, cname
from ((select * from AllCountries) except (select * from OceanBorder)) Landlocked, country
where country.cid = Landlocked.cid;

create view T1 as
select c1id, c2id
from ((select c1id as cid from Exactly1Neighbor) INTERSECT (select cid from LandlockedCountry)) WithoutNames, exactly1neighbor
where exactly1neighbor.c1id = withoutnames.cid;

create view GetName1 as
select c1id, cname as c1name, c2id
from T1, country
where T1.c1id = country.cid;

create view GetName2 as
select c1id, c1name, c2id, cname as c2name
from GetName1, country
where GetName1.c2id = country.cid;

create view answer as
select distinct c1id, c1name, c2id, c2name from GetName2;

insert into Query3(select * from answer order by c1name asc); 

drop view answer cascade;
drop view GetName2 cascade;
drop view GetName1 cascade;
drop view T1 cascade;
drop view LandlockedCountry cascade;
drop view Exactly1Neighbor cascade;
drop view Exactly1NeighborCountry cascade;
drop view OceanBorder cascade;
drop view AllCountries cascade;

-- Query 4 statements
create view DirectAccess as 
select cname, oname 
from country c, ocean o, oceanAccess oa
where c.cid = oa.cid and oa.oid = o.oid;

create view IndirectAccess as 
select cname, oname
from country c, neighbour n, ocean o, oceanAccess oa
where c.cid = n.country and n.neighbor = oa.cid and oa.oid = o.oid;

create view Answer as 
(select * from DirectAccess) union (select * from IndirectAccess);

insert into Query4 (select * from Answer order by cname asc, oname desc);

drop view Answer cascade;
drop view IndirectAccess cascade;
drop view DirectAccess cascade;

-- Query 5 statements
create view HighestHDI as
select c.cid, cname, avg(hdi_score) as avghdi
from country c, hdi
where c.cid = hdi.cid and (year >= 2009 and year <= 2013)
group by c.cid, cname
limit 10;

insert into Query5 (select * from HighestHDI order by avghdi desc);

drop view HighestHDI cascade;

-- Query 6 statements
create view answer as
select distinct c.cid, cname 
from country c, hdi h1, hdi h2, hdi h3, hdi h4, hdi h5 
where c.cid = h1.cid and h1.cid = h2.cid and h2.cid = h3.cid and h3.cid = h4.cid and h4.cid = h5.cid and h1.hdi_score < h2.hdi_score and h2.hdi_score < h3.hdi_score and h3.hdi_score < h4.hdi_score and h4.hdi_score < h5.hdi_score and h1.year = 2009 and h2.year = 2010 and h3.year = 2011 and h4.year = 2012 and h5.year = 2013
;
insert into Query6 (select * from answer order by cname asc);

drop view answer cascade;

-- Query 7 statements

create view ReligionFollowers as
select rid, rname, sum(population*rpercentage) followers
from country c, religion r
where c.cid = r.cid 
group by rid, rname
;
insert into Query7 (select * from ReligionFollowers) order by followers desc;

drop view ReligionFollowers cascade;

-- Query 8 statements
create view MostPopularLanguage as 
select cname, c.cid, lname
from country c, language l
where c.cid = l.cid and lpercentage >= all
	(select lpercentage
	from country c1, language l1
	where c1.cid = l1.cid and c.cid = c1.cid);

create view NeighbourMatch as
select distinct t1.cname as c1name, t2.cname as c2name, t1.lname as lname
from neighbour n, MostPopularLanguage as t1, MostPopularLanguage as t2
where t1.lname = t2.lname and t1.cid = n.country and t2.cid = n.neighbor
;
insert into Query8  (select * from NeighbourMatch order by lname asc, c1name desc);

drop view NeighbourMatch cascade;
drop view MostPopularLanguage cascade;

-- Query 9 statements

create view Oceans as
select cid, depth
from ocean, oceanAccess
where ocean.oid = oceanAccess.oid;

create view AllCountries as
select cid
from country;

create view NoOceans as
select cid, 0 as depth
from ((select * from AllCountries) EXCEPT (select cid from Oceans)) t1;

create view answer as
select AllOceans.cid, cname, (height + depth) as totalspan
from ((select * from Oceans) UNION (select * from NoOceans)) AllOceans, country
where AllOceans.cid = country.cid
order by (height+depth) desc
limit 1;

insert into Query9(select cname, totalspan from answer);

drop view answer cascade;
drop view NoOceans cascade;
drop view AllCountries cascade;
drop view Oceans cascade;
-- Query 10 statements

create view TotalBorder as
select country, sum(length) as totalborder
from neighbour
group by country
order by sum(length) DESC
limit 1;

create view GetNames as
select cname, totalborder
from TotalBorder, country
where TotalBorder.country = country.cid;

insert into Query10(select * from GetNames);

drop view getnames cascade;
drop view TotalBorder cascade;
