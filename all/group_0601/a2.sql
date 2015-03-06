-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.
drop view if exists proto1 CASCADE;
drop view if exists proto2 CASCADE;
drop view if exists proto3 CASCADE;
drop view if exists proto4 CASCADE;
drop view if exists proto5 CASCADE;
drop view if exists proto6 CASCADE;
drop view if exists proto7 CASCADE;
drop view if exists proto8 CASCADE;
drop view if exists proto9 CASCADE;


-- Query 1 statements
--drop view proto1 CASCADE;

--These are all the neighboring countries of each country--
CREATE OR REPLACE VIEW cross_country AS 
	select t1.cid as c1id, t1.cname as c1name, t1.population as c1population,
	t2.cid as c2id, t2.cname as c2name, t2.height as c2height, t2.population as c2population
	from country as t1, country as t2;

create or replace view neighboring_countries as
	select c1id, c1name, c2id, c2name, c2height
	from cross_country, neighbour
	where c1id=country and c2id=neighbor;

create or replace view tallest_neighbour as
	select c1id as country, max(c2height) as tallest_neighbour
	from neighboring_countries
	group by c1id;

create or replace view proto1 as
	select c1id, c1name, c2id, c2name
	from neighboring_countries, tallest_neighbour
	where c2height=tallest_neighbour and c1id=country
	order by c1name asc;

INSERT INTO Query1 (SELECT * FROM proto1);

--select * from proto1;
--NEED TO LEARN HOW TO INJECT ANSWER INTO QUERY1

-- Query 2 statements

create or replace view does_not_touch_water as
	select cid
	from country except (select cid from oceanAccess);

create or replace view proto2 as
	select country.cid, country.cname
	from country inner join does_not_touch_water
	on country.cid = does_not_touch_water.cid
	order by cname asc;

INSERT INTO Query2 (SELECT * FROM proto2);

--select * from proto2;

-- Query 3 statements

create or replace view does_not_touch_water1 as
	select cid
	from country except (select cid from oceanAccess);

create or replace view t1 as
	select country.cid, country.cname
	from country inner join does_not_touch_water1
	on country.cid = does_not_touch_water1.cid
	order by cname asc;

create or replace view singular_country as
	select cid 
	from t1 inner join neighbour
	on t1.cid=neighbour.country
	group by cid
	having count(neighbour) < 2;

create or replace view landlocked_neighbor_id as
	select cid as c1id, cname, neighbour.country, neighbour.neighbor
	from ((singular_country natural join country) cross join neighbour)
	where cid=country;

create or replace view proto3 as
	select c1id, landlocked_neighbor_id.cname as c1name, neighbor as c2id, country.cname as c2name 
	from landlocked_neighbor_id cross join country
	where country.cid=landlocked_neighbor_id.neighbor
	order by c1name asc;

INSERT INTO Query3 (SELECT * FROM proto3);

	
	--select * from proto3;
	
-- Query 4 statements

create or replace view indirect_access as
	select neighbour.neighbor as cid, oid 
	from oceanaccess, neighbour
	where oceanAccess.cid = neighbour.country;

	--select * from indirect_access;

create or replace view complete_oceanaccess as
	(Select * from oceanaccess)  union (select * from indirect_access)
	order by cid;
	
create or replace view proto4 as
	select cname, oname
	from complete_oceanaccess natural join country natural join ocean
	order by cname asc,oname desc;

INSERT INTO Query4 (SELECT * FROM proto4);
	
	--select * from proto4; 

-- Query 5 statements

create or replace view valid_years as
	select cid, hdi_score
	from hdi
	where year<2014 and year>2008;

--	select * from valid_years;

create or replace view hdi_avg as
	select valid_years.cid, avg(hdi_score)
	from valid_years
	group by valid_years.cid
	order by cid asc;

--	select * from hdi_avg;

create or replace view proto5 as
	select cid, cname, avg as avghdi
	from hdi_avg natural join country
	order by avghdi desc
	limit 10;

INSERT INTO Query5 (SELECT * FROM proto5);

	--select * from proto5;


-- Query 6 statements

create or replace view within_range as
	select cid, year, hdi_score
	from hdi
	where year<2014 and year>2008;

create or replace view increasing_hdi as
	select t1.cid, t1.year, t1.hdi_score
	from within_range as t1, within_range as t2, within_range as t3, within_range as t4, within_range as t5
	where t1.year < t2.year
	and t2.year < t3.year
	and t3.year < t4.year
	and t4.year < t5.year
	and t1.hdi_score < t2.hdi_score
	and t2.hdi_score < t3.hdi_score
	and t3.hdi_score < t4.hdi_score
	and t4.hdi_score < t5.hdi_score
	and t1.cid = t2.cid
	and t2.cid = t3.cid
	and t3.cid = t4.cid
	and t4.cid = t5.cid;

create or replace view proto6 as	
	select increasing_hdi.cid, country.cname
	from increasing_hdi natural join country
	where country.cid=increasing_hdi.cid
	order by cname asc;

INSERT INTO Query6 (SELECT * FROM proto6);


-- Query 7 statements

--drop view country_follower cascade;

create or replace view country_follower as
	select country.cid, cname, population, rid, rname, cast(rpercentage*100 as integer) as rpercentage
	from country cross join religion
	where country.cid = religion.cid;

	--select * from country_follower;
	--select sum(population) from country_follower;

create or replace view proto7 as
	select rid, rname, sum((population*rpercentage)/100) as followers
	from country_follower
	group by rid,rname
	order by followers desc;

INSERT INTO Query7 (SELECT * FROM proto7);

	--select * from proto7;


-- Query 8 statements

create or replace view top_language as
	select *
	from language
	where lpercentage=(select max(lpercentage) from language as t1 where t1.cid=language.cid);

	--select * from top_language;

create or replace view top_language_neighbour as
	select t1.cid as c1id, t2.cid as c2id, t1.lname
	from top_language as t1 cross join neighbour cross join top_language as t2
	where t1.lid=t2.lid and
	t1.lname=t2.lname and
	t1.cid=neighbour.country and 
	t2.cid=neighbour.neighbor;

create or replace view proto8 as
	select t2.cname as c1name, t3.cname as c2name, lname
	from top_language_neighbour as t1 cross join country as t2 cross join country as t3
	where t1.c1id=t2.cid and
	t1.c2id=t3.cid
	order by lname asc, c1name desc;

INSERT INTO Query8 (SELECT * FROM proto8);

--select * from proto8;

-- Query 9 statements

create or replace view landlocked as
	select cid, 0 as totalspan
	from ((select cid from country) except (select cid from oceanaccess)) as t1;

create or replace view diff as
	select cid, height+depth as totalspan
	from oceanaccess natural join country natural join ocean;
	
create or replace view all_diff as
	select *
	from ((select * from landlocked) union (select * from diff)) as t1;
	
create or replace view proto9 as
	select country.cname, totalspan
	from all_diff cross join country
	where all_diff.totalspan=(select max(totalspan) from all_diff)
	and all_diff.cid=country.cid;

INSERT INTO Query9 (SELECT * FROM proto9);

/*IS THIS THE RIGHT OUTPUT READ QUESTION WORDING!*/
	--select * from proto9;

-- Query 10 statements

create or replace view border_sum as
	select country.cname, sum(length) as borderslength
	from neighbour,country
	where neighbour.country=country.cid
	group by country.cname
	order by borderslength desc;
	--select * from border_sum;
	
create or replace view proto10 as
	select *
	from border_sum
	where border_sum.borderslength=(select max(borderslength) from border_sum);

	--select * from proto10;

INSERT INTO Query10 (SELECT * FROM proto10);

--DROP VIEW neighboring_countries
