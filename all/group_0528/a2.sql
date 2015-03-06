-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
create view countryneighbour as
(select neighbour.country as c1id, neighbour.neighbor as c2id, country.cname as c2name, country.height
 from country join neighbour on country.cid = neighbour.neighbor);

create view maxheight as
(select c1id, max(height) as maxelevation from countryneighbour group by c1id);

create view matchheight as
(select countryneighbour.c1id, countryneighbour.c2id, countryneighbour.c2name 
 from countryneighbour join maxheight on countryneighbour.c1id = maxheight.c1id 
 and countryneighbour.height = maxheight.maxelevation);

create view addcountryname as
(select matchheight.c1id, country.cname as c1name, matchheight.c2id, matchheight.c2name 
 from matchheight join country on matchheight.c1id = country.cid);

insert into Query1
(select c1id, c1name, c2id, c2name from addcountryname order by c1name asc);

drop view if exists addcountryname cascade;
drop view if exists matchheight cascade;
drop view if exists maxheight cascade;
drop view if exists countryneighbour cascade;

-- Query 2 statements
create view noocean as
(select cid from country) except (select cid from oceanaccess);

insert into Query2
(select noocean.cid, country.cname 
 from noocean join country on noocean.cid = country.cid order by country.cname asc);

drop view if exists noocean cascade;

-- Query 3 statements
create view surroundby1 as
(select country as c1id from neighbour group by c1id having count(neighbor) = 1);

create view surroundby1neighbour as
(select surroundby1.c1id, neighbour.neighbor as c2id 
 from surroundby1 join neighbour on surroundby1.c1id = neighbour.country); 

create view landlocked as
(select cid from country) except (select cid from oceanaccess);

create view landlockedsurroundby1 as
(select surroundby1neighbour.c1id, surroundby1neighbour.c2id 
 from surroundby1neighbour join landlocked on surroundby1neighbour.c1id = landlocked.cid);

create view addc1name as
(select landlockedsurroundby1.c1id, country.cname as c1name, landlockedsurroundby1.c2id 
 from landlockedsurroundby1 join country on landlockedsurroundby1.c1id = country.cid);

create view addc2name as
(select addc1name.c1id, addc1name.c1name, addc1name.c2id, country.cname as c2name
 from addc1name join country on addc1name.c2id = country.cid);

insert into Query3
(select c1id, c1name, c2id, c2name from addc2name order by c1name asc);

drop view if exists addc2name cascade;
drop view if exists addc1name cascade;
drop view if exists landlockedsurroundby1 cascade;
drop view if exists landlocked cascade;
drop view if exists surroundby1neighbour cascade;
drop view if exists surroundby1 cascade;

-- Query 4 statements
create view direct as
(select country.cid as oacid, oceanAccess.oid as oaoid from country, oceanAccess 
 where country.cid = oceanAccess.cid);

create view notdirect as
(select neighbour.country as oacid, oceanAccess.oid as oaoid from neighbour, oceanAccess 
 where neighbour.neighbor = oceanAccess.cid); 

create view countryaccessoceans as
((select * from direct) union (select * from notdirect));

insert into Query4
(select country.cname, ocean.oname from countryaccessoceans, country, ocean 
 where countryaccessoceans.oacid = country.cid and countryaccessoceans.oaoid = ocean.oid 
 order by country.cname asc, ocean.oname desc);

drop view if exists countryaccessoceans cascade;
drop view if exists notdirect cascade;
drop view if exists direct cascade;

-- Query 5 statements
create view highest10 as
(select cid, avg(hdi_score) as avghdi from hdi where year >= 2009 and year <= 2013
 group by cid order by avghdi desc limit 10);

insert into Query5
(select highest10.cid, country.cname, highest10.avghdi 
 from highest10 join country on highest10.cid = country.cid order by avghdi desc);

drop view if exists highest10 cascade;

-- Query 6 statements
create view fiveyearhdi as
(select H1.cid, H1.hdi_score as hdi2009, H2.hdi_score as hdi2013
 from hdi as H1, hdi as H2
 where H1.year = 2009 and H2.year = 2013 and H1.cid = H2.cid);

create view difference as
(select cid, (hdi2013 - hdi2009) as diff from fiveyearhdi);

create view increasehdi as
(select cid from difference where diff > 0);

insert into Query6
(select increasehdi.cid, country.cname 
 from increasehdi join country on increasehdi.cid = country.cid order by cname asc);

drop view if exists increasehdi cascade;
drop view if exists difference cascade;
drop view if exists fiveyearhdi cascade;

-- Query 7 statements
insert into Query7
(select religion.rid, religion.rname, sum(religion.rpercentage * country.population) as followers
 from country join religion on country.cid = religion.cid 
 group by religion.rid, religion.rname
 order by followers desc);

-- Query 8 statements
create view popularlanguage as 
(select cid, max(lpercentage) as mostpopularlanguage from language group by cid);

create view popularlanguagename as
(select popularlanguage.cid, language.lname from popularlanguage, language 
 where popularlanguage.cid = language.cid and popularlanguage.mostpopularlanguage = language.lpercentage);

create view matching as
(select c1.cid as c1id, c2.cid as c2id, c1.lname 
 from popularlanguagename as c1, popularlanguagename as c2 
 where c1.lname = c2.lname and c1.cid <> c2.cid);

create view neighmatching as
(select matching.c1id, matching.c2id, matching.lname 
 from matching, neighbour where matching.c1id = neighbour.country and matching.c2id = neighbour.neighbor);

create view addcountryname as
(select country.cname as c1name, neighmatching.c2id, neighmatching.lname 
 from neighmatching, country where neighmatching.c1id = country.cid);

create view addneighbourname as
(select addcountryname.c1name, country.cname as c2name, addcountryname.lname
 from addcountryname, country where addcountryname.c2id = country.cid);

insert into Query8
(select c1name, c2name, lname from addneighbourname order by lname asc,lname desc);

drop view if exists addneighbourname cascade;
drop view if exists addcountryname cascade;
drop view if exists neighmatching cascade;
drop view if exists matching cascade;
drop view if exists popularlanguagename cascade;
drop view if exists popularlanguage cascade;

-- Query 9 statements
create view noocean as
(select cid from country) except (select cid from oceanaccess);

create view nooceandiff as
(select country.cname, country.height as totalspan from noocean join country on noocean.cid = country.cid);

create view hasocean as
(select oceanaccess.cid, max(ocean.depth) as maxdepth from ocean join oceanaccess on ocean.oid = oceanaccess.oid group by cid);

create view hasoceanandiff as
(select country.cname, (country.height + hasocean.maxdepth) as totalspan from hasocean join country on hasocean.cid = country.cid);

create view maxdiffeachcountry as
(select unite.cname, unite.totalspan as totalspan from ((select * from nooceandiff) union (select * from hasoceanandiff)) as unite);

insert into Query9
(select maxdiffeachcountry.cname, maxdiffeachcountry.totalspan from maxdiffeachcountry 
 where maxdiffeachcountry.totalspan = (select max(maxdiffeachcountry.totalspan) from maxdiffeachcountry));

drop view if exists maxdiffeachcountry cascade;
drop view if exists hasoceanandiff cascade;
drop view if exists hasocean cascade;
drop view if exists nooceandiff cascade;
drop view if exists noocean cascade;

-- Query 10 statements
create view totallength as
(select country, sum(length) as borderslength from neighbour group by country);

create view longest as
(select country, borderslength from totallength where borderslength = (select max(borderslength) from totallength));

insert into Query10
(select country.cname, longest.borderslength from longest join country on longest.country = country.cid);

drop view if exists longest cascade;
drop view if exists totallength cascade;
