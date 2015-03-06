-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

insert into Query1 (
	select n2.c1id as c1id, c1name, c2.cid as c2id, c2.cname as c2name
	from country c join 
	(select c1.cid as c1id, c1.cname as c1name, max(c2.height) as max_height
	from country c1 join neighbour n on c1.cid = n.country
	join country c2 on n.neighbor = c2.cid
	group by c1.cid, c1.cname) n2
	on c.cid = n2.c1id 
	join country c2 on n2.max_height = c2.height and c2.cid <> n2.c1id
	order by c1name asc);



-- Query 2 statements

insert into Query2  (select cid, cname
from country
where cid not in (select cid from oceanAccess)
order by cname asc);





-- Query 3 statements

DROP VIEW IF EXISTS "landlocked_neighbours" CASCADE;
DROP VIEW IF EXISTS "landlocked_countries" CASCADE;

create view landlocked_countries as 
	select cid, cname
	from country
	where cid not in (select cid from oceanAccess)
	order by cname asc;

--Create view of all landlocked countries with their neighbors
create view landlocked_neighbours as 
	select c1.cid as c1id , c1.cname as c1name, c2.cid as c2id, c2.cname as c2name 
	from neighbour n
	join landlocked_countries c1 on c1.cid = n.country
	join landlocked_countries c2 on c2.cid = n.neighbor;

--Selects landlocked countries that only have one neighbor


insert into Query3
	(select c1id, c1name, c2id, c2name
		from landlocked_neighbours ln 
		join 
			( select c.cid as cid, c.cname as cname
				from country c 
				join neighbour n on c.cid = n.country
				group by c.cid, c.cname
				having count(n.neighbor)=1) n2
on c1id = n2.cid
order by c1name asc);

DROP VIEW IF EXISTS "landlocked_neighbours" CASCADE;
DROP VIEW IF EXISTS "landlocked_countries" CASCADE;


-- Query 4 statements

DROP VIEW IF EXISTS "neighbour_pair" CASCADE;
DROP VIEW IF EXISTS "direct_access" CASCADE;
DROP VIEW IF EXISTS "indrect_access" CASCADE;

--country w/ direct access to ocean(s) with their ocean(s) names
create view direct_access as
select cname, oname
from oceanAccess oa join country c on oa.cid = c.cid
join ocean o on oa.oid = o.oid
order by cname asc, oname desc;

--find all neighbour pairs

create view neighbour_pair as

select c1.cid as c1cid, c1.cname as c1name, c2.cid as c2cid, c2.cname as c2name
	from neighbour n join country c1 on c1.cid = n.country
	join country c2 on n.neighbor = c2.cid;

--countries w/ indirect access to oceans
create view indirect_access as 
select c1name as cname, oname
from neighbour_pair np join oceanaccess oa on np.c2cid = oa.cid
join ocean o on oa.oid = o.oid
order by c1name asc, oname desc;

insert into Query4 (select * from indirect_access union select * from direct_access
order by cname asc, oname desc);


DROP VIEW IF EXISTS "neighbour_pair" CASCADE;
DROP VIEW IF EXISTS "direct_access" CASCADE;
DROP VIEW IF EXISTS "indrect_access" CASCADE;

-- Query 5 statements

insert into Query5 
	(select c.cid as cid, c.cname as cname, n1.avghdi as avghdi 
	from
	--inner subquery gets cids of top 10 countries w/ highest avg hdis from 2009-2013
	 (select cid, avg(hdi_score) avghdi
	from hdi
	where year >=2009 and year <= 2013
	group by cid
	order by avg(hdi_score) desc limit 10) n1

	join country c on n1.cid = c.cid);



-- Query 6 statements

drop view if exists "hdi_2010" cascade;
drop view if exists "hdi_2011" cascade;
drop view if exists "hdi_2012" cascade;
drop view if exists "hdi_2013" cascade;


create view hdi_2010 as
select h1.cid, h2.hdi_score as score_2010
from hdi h1 join hdi h2 
on h1.cid = h2.cid and h1.year = 2009 and h2.year = 2010
where h2.hdi_score - h1.hdi_score > 0;

select * from hdi_2010;


--hdi increase from 2010 to 2011

create view hdi_2011 as 
select h1.cid, h2.hdi_score as score_2011
from hdi_2010 h1
join hdi h2
on h1.cid = h2.cid and h2.year = 2011
where h2.hdi_score - h1.score_2010 > 0;

--hdi from 2011 to 2012
create view hdi_2012 as
select h1.cid, h2.hdi_score as score_2012
from hdi_2011 h1
join hdi h2 
on h1.cid = h2.cid and h2.year = 2012
where h2.hdi_score - h1.score_2011 > 0;

create view hdi_2013 as
select h1.cid, h2.hdi_score as score_2013
from hdi_2012 h1
join hdi h2 
on h1.cid = h2.cid and h2.year = 2013
where h2.hdi_score - h1.score_2012 > 0;

insert into query6 (select h.cid, c.cname
from hdi_2013 h
join country c
on h.cid = c.cid
order by cname asc);

drop view if exists "hdi_2010" cascade;
drop view if exists "hdi_2011" cascade;
drop view if exists "hdi_2012" cascade;
drop view if exists "hdi_2013" cascade;



-- Query 7 statements

insert into query7 
	(select rid, rname, cast(sum(rpercentage * population) as int) as followers
	from country c join religion r on c.cid = r.cid
	group by rid, rname
	order by followers desc);




-- Query 8 statements

DROP VIEW IF EXISTS "neighbour_pair" CASCADE;
DROP VIEW IF EXISTS "top_languages" CASCADE;


--gets table of country names and their most popular language
create view top_languages as (
select c1.cname country_name , l1.lname language_name, l1.lid language_id
from country c1 join 

	(select l.cid as cid, max(l.lpercentage) as top_language
	from language l
	join country c on l.cid = c.cid
	group by l.cid , c.cname) n2

on c1.cid = n2.cid
join language l1 on l1.lpercentage = n2.top_language);


create view neighbour_pair as 

	select c1.cid as c1cid, c1.cname as c1name, c2.cid as c2cid, c2.cname as c2name
	from neighbour n join country c1 on c1.cid = n.country
	join country c2 on n.neighbor = c2.cid;

insert into Query8(
select c1name, c2name, tl2.language_name as lname
from neighbour_pair np 
join top_languages tl1 on np.c1name = tl1.country_name
join top_languages tl2 on np.c2name = tl2.country_name
where tl1.language_name = tl2.language_name
order by tl2.language_name asc, c1name desc);



DROP VIEW IF EXISTS "neighbour_pair" CASCADE;
DROP VIEW IF EXISTS "top_languages" CASCADE;




-- Query 9 statements

DROP VIEW IF EXISTS "totalspan_no_ocean" CASCADE;
DROP VIEW IF EXISTS "max_totalspan" CASCADE;
DROP VIEW IF EXISTS "totalspan_ocean" CASCADE;


--countries w/ direct ocean access
create view totalspan_ocean as
select f.cname, coalesce(f.depth,0) + coalesce(f.height,0) as totalspan from
(
select cname, oa.oid, n.depth, height
from oceanaccess oa
join ocean o on oa.oid = o.oid
join (select cid ,max(depth) as depth
	from oceanaccess oa join ocean o on o.oid = oa.oid
	group by cid) n on n.cid = oa.cid 
join country c on c.cid = oa.cid) f
group by f.cname, f.depth, f.height;

create view totalspan_no_ocean as
select c.cname as cname, c.height as totalspan
from country c
where c.cid not in (select cid from oceanaccess);

create view maxtotalspan as 

select * from totalspan_ocean union select * from totalspan_no_ocean;

insert into Query9
	 (select cname, totalspan from maxtotalspan
where totalspan = (select max(totalspan) from maxtotalspan));



DROP VIEW IF EXISTS "totalspan_ocean" CASCADE;
DROP VIEW IF EXISTS "totalspan_no_ocean" CASCADE;
DROP VIEW IF EXISTS "max_totalspan" CASCADE;


-- Query 10 statements

DROP VIEW IF EXISTS "neighbour_pair" CASCADE;
DROP VIEW IF EXISTS "border_length" CASCADE;


create view neighbour_pair as

select c1.cid as c1cid, c1.cname as c1name, c2.cid as c2cid, c2.cname as c2name, n.length as length
	from neighbour n join country c1 on c1.cid = n.country
	join country c2 on n.neighbor = c2.cid;

create view border_length as
	select c1name as cname, sum(length) as borderslength
	from neighbour_pair as np
	group by c1name;

insert into query10(
select bl.cname, bl.borderslength
from border_length bl
where bl.borderslength >= all (select borderslength from border_length));



DROP VIEW IF EXISTS "neighbour_pair" CASCADE;
DROP VIEW IF EXISTS "border_length" CASCADE;
