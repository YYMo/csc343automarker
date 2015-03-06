-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
create view elevation as
select height, cid
from country;

create view neighbours as
select country, neighbor
from neighbour;

create view country_with_neighbour as
select country, neighbor, height
from neighbours cross join elevation
where neighbor = cid;

create view max_elevation as
select country as c1id, neighbor as c2id
from country_with_neighbour
where height =
      (select max(height) from country_with_neighbour group by country);

create view country_name as
select c1id, cname as c1name, c2id
from  max_elevation cross join country
where c1id = cid;

create view answer as
select c1id, c1name, c2id, cname as c2name
from country_name cross join country
where c2id = cid;

insert into Query1(c1id, c1name, c2id, c2name)
select * from answer;

drop view answer;
drop view country_name;
drop view max_elevation;



-- Query 2 statements
create view land_locked_countries as
select cid as ocid
from ((select cid from country) except (select cid from oceanAccess))countries;

create view answer as
select distinct cid
from country cross join land_locked_countries
where ocid = cid;

insert into Query2(cid)
select * from answer;

drop view answer;
drop view land_locked_countries;

-- Query 3 statements
create view neighbour_count as
select country, count(*) as neighbours
from neighbour
group by country;

create view one_neighbour as
select country as cid
from neighbour_count
where neighbours = 1
intersect
select cid
from Query2;

create view land_locked_and_one_neighbour as
select cid as c1id, cname as c1name
from one_neighbour natural join country;

create view find_neighbour as
select c1id, c1name, neighbor as c2id
from (land_locked_and_one_neighbour cross join neighbour)f_neig
where c1id = country;

create view answer as
select c1id, c1name, c2id, cname as c2name
drop view answer;
drop view land_locked_countries;

-- Query 3 statements
create view neighbour_count as
select country, count(*) as neighbours
from neighbour
group by country;

create view one_neighbour as
select country as cid
from neighbour_count
where neighbours = 1
intersect
select cid
from Query2;

create view land_locked_and_one_neighbour as
select cid as c1id, cname as c1name
from one_neighbour natural join country;

create view find_neighbour as
select c1id, c1name, neighbor as c2id
from (land_locked_and_one_neighbour cross join neighbour)f_neig
where c1id = country;

create view answer as
select c1id, c1name, c2id, cname as c2name
from find_neighbour cross join country
where c2id = cid;

insert into Query3(c1id, c1name, c2id, c2name)
select * from answer;
drop view answer;
drop view find_neighbour;
drop view land_locked_and_one_neighbour;
drop view one_neighbour;
drop view neighbour_count;

-- Query 4 statements

create view accessible_oceans as
select distinct country, oid as oceanid
from ((select country, oid from (neighbour cross join oceanAccess)nei_ocean where nei_ocean.neighbor = nei_ocean.cid) union (select cid as country, oid from oceanAccess)) acc_ocean;

create view country_name as
select cname, oceanid
from country cross join accessible_oceans
where country = cid;

create view ocean_name as
select cname, oname
from country_name cross join ocean
where oceanid = oid;

insert into Query4(cname, oname)
select * from ocean_name;

drop view ocean_name;
drop view country_name;
drop view accessible_oceans;

-- Query 5 statements
create view "2009-2013_hdi" as
select cid,year,hdi_score
from hdi
where 2009 <= year and year <= 2013;

create view avg_hdi as
select cid, avg(hdi_score) as avghdi
from "2009-2013_hdi"
group by cid;

create view ans as
select cid, cname, avghdi
from avg_hdi natural join country;

insert into query5(cid,cname, avghdi)
select * from ans;
drop view ans;
drop view avg_hdi;
drop view "2009-2013_hdi";

-- Query 6 statements

create view hdi1 as
select cid as cid1, year as year1, hdi_score as hdi_score1
from hdi
where (2009 <= year) and (year <= 2013);

create view hdi2 as
select cid as cid2, year as year2, hdi_score as hdi_score2
from hdi
where (2009 <= year) and (year <= 2013);

create view hdi3 as
select cid as cid3, year as year3, hdi_score as hdi_score3
from hdi
where (2009 <= year) and (year <= 2013);

create view hdi4 as
select cid as cid4, year as year4, hdi_score as hdi_score4
from hdi
where (2009 <= year) and (year <= 2013);

create view hdi5 as
select cid as cid5, year as year5, hdi_score as hdi_score5
from hdi
where (2009 <= year) and (year <= 2013);

create view complete_hdi as
select distinct cid1
from ((((hdi1 cross join hdi2) cross join hdi3) cross join hdi4) cross join hdi5)comp_hdi
where ((cid1 = cid2) and (cid2 = cid3) and (cid3 = cid4) and (cid4 = cid5)) and (((((year1 = 2009 and year2 = 2010) and year3 = 2011) and year3 = 2012) and year4 = 2013) and year5 = 2013) and ((hdi_score1 < hdi_score2) and (hdi_score2 < hdi_score3) and (hdi_score3 < hdi_score4) and (hdi_score4 < hdi_score5));

create view country_name as
select cid, cname
from complete_hdi cross join country
where cid1 = cid;

insert into Query6(cid, cname)
select * from country_name;
drop view country_name;
drop view complete_hdi;
drop view hdi5;
drop view hdi4;
drop view hdi3;
drop view hdi2;
drop view hdi1;
-- Query 7 statements
create view population_and_religion as
select cid, rid, rname, population, rpercentage
from country natural join religion;

create view country_religion as
select cid, rid, rname, population*(rpercentage/100) as rpopulation
from population_and_religion;

create view answer as
select rid, rname, sum(rpopulation) as followers
from country_religion
group by rid, rname
order by followers desc;

insert into Query7(rid, rname, followers)
select * from answer;

drop view answer;
drop view country_religion;
drop view population_and_religion;



-- Query 8 statements
create view most_popular_language as
select cid as countryid, max(lpercentage) as max_percentage
from language
group by cid;

create view lname as
select distinct countryid, lname
from most_popular_language cross join language
where cid = countryid and max_percentage = lpercentage;

create view con_language as
select country, neighbor, lname as lname1
from lname cross join neighbour
where countryid = country;

create view nei_language as
select country, neighbor, lname1, lname as lname2
from con_language cross join lname
where neighbor = countryid;

create view nei_language1 as
select country, neighbor, lname1
from nei_language
where lname1 = lname2;

create view country_name as
select cname as c1name, neighbor, lname1 as lname
from nei_language1 cross join country
where cid = country;

create view neighbor_name as
select c1name, cname as c2name, lname
from country_name cross join country
where neighbor = cid;

insert into Query8(c1name, c2name, lname)
select * from neighbor_name;

drop view neighbor_name;
drop view country_name;
drop view nei_language1;
drop view nei_language;
drop view con_language;
drop view lname;
drop view most_popular_language;
-- Query 9 statements

create view depth as
select cid as depthcid, depth
from ocean cross join oceanAccess
where ocean.oid = oceanAccess.oid;

create view depth_and_height as
select depthcid, depth, height, cname
from depth cross join country
where depthcid = cid;

create view height as
(select cname, height
from country
where cid not in (select cid from oceanAccess))
union
(select distinct cname, (height-depth) as height
from depth_and_height);

create view height1 as
select distinct cname, height
from height;

create view max_span as
select cname, height as totalspan
from height1
where height = (select max(height) from height1);

insert into Query9(cname, totalspan)
select * from max_span;
drop view max_span;
drop view height1;
drop view height;
drop view depth_and_height;
drop view depth;

-- Query 10 statements
create view neighbour_coun as
select cid, cname, neighbour, length
from country cross join neighbour
where cid = country;

create view total_border_length as
select cid, cname, sum(length) as borderslength
from neighbour_coun
group by cid, cname;

create view answer as
select cname, borderslength
from total_border_length
where borderslength = (select max(borderslength) from total_border_length);

insert into Query10(cname, borderslength)
select * from answer;

drop view answer;
drop view total_border_length;
drop view neighbour_coun;


