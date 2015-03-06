-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

set search_path to a2;

-- #############################################################################
-- Query 1 statements
-- #############################################################################
delete from query1;

-- since concept of neighbors can be both way
-- we create this union just in case
create or replace view neighbors as
	select 
		distinct(friends.c1), friends.c2 
	from (
			select 
				n.country as c1, n.neighbor as c2 
			from neighbour n 
			union
			select 
				n.neighbor as c1, n.country as c2 
			from neighbour n) as friends 
				where not friends.c1 = friends.c2 	
				group by friends.c1, friends.c2;

-- just a list of cid, along with the tallest height of neighbor of cid
create or replace view tallest_friend_height as
	select 
		countries.cid as cid, max(n.height) as height from
	country countries 
	inner join neighbors on neighbors.c1 = countries.cid
	inner join country n on n.cid = neighbors.c2
	group by countries.cid;


insert into query1 (
select
	c1.cid as c1cid,
	c1.cname as c1cname,
	c2.cid as c2cid,
	c2.cname as c2cname
from 
	country c1 
	inner join tallest_friend_height on tallest_friend_height.cid = c1.cid
	inner join neighbors n on n.c1 = c1.cid
	inner join country c2 on c2.cid = n.c2 
			and c2.height = tallest_friend_height.height
			order by c1.cname);
	

drop view tallest_friend_height;
drop view neighbors;



-- #############################################################################
-- Query 2 statements
-- #############################################################################
delete from query2;

insert into query2(
	select 
		c.cid, c.cname 
	from 
		country c where not exists 
			(select null from oceanAccess a where a.cid = c.cid) 
	order by c.cname);



-- #############################################################################
-- Query 3 statements
-- #############################################################################
delete from query3;

-- set of all neighbor tuples arranged for inner join
create or replace view neighbors as
	select 
		distinct(friends.c1), friends.c2 
	from (
			select 
				n.country as c1, n.neighbor as c2 
			from neighbour n 
			union
			select 
				n.neighbor as c1, n.country as c2 
			from neighbour n) as friends 
				where not friends.c1 = friends.c2 	
				group by friends.c1, friends.c2;

-- like Query2
create or replace view land_locked as
	select 
		c.cid, c.cname 
	from country c 
	where not exists 
	(select null from oceanAccess a where a.cid = c.cid) order by c.cname;

insert into query3(
select 
	c1.cid as c1cid,
	c1.cname as c1cname,
	c2.cid as c2cid,
	c2.cname as c2cname
from 
	country c1 
	inner join land_locked ll on c1.cid = ll.cid
	inner join 
	(select c1 as cid, count(c2) as cnt from neighbors group by c1) ncount 
	on ncount.cid = c1.cid and cnt = 1
	inner join neighbors n on n.c1 = c1.cid 
	inner join country c2 on c2.cid = n.c2 order by c1.cname asc);

drop view land_locked;
drop view neighbors;



-- #############################################################################
-- Query 4 statements
-- #############################################################################
delete from query4;

-- joinable list of neighbors	
create or replace view neighbors as
	select 
		distinct(friends.c1), friends.c2 
	from (
			select 
				n.country as c1, n.neighbor as c2 
			from neighbour n 
			union
			select 
				n.neighbor as c1, n.country as c2 
			from neighbour n) as friends 
				where not friends.c1 = friends.c2 	
				group by friends.c1, friends.c2;

insert into query4(
	select x.cname, o.oname from
	(
		select 
			c.cid, c.cname, oa.oid from
		country c 
			inner join oceanAccess oa on oa.cid = c.cid
		union
		select 
			c.cid, c.cname, oa.oid from
		country c 
			inner join neighbors ns on ns.c1 = c.cid
			inner join oceanAccess oa on oa.cid = ns.c2
	) x 
		inner join ocean o on o.oid = x.oid
		group by x.cid, x.cname, o.oname
		order by x.cname asc, o.oname desc);


drop view neighbors;




-- #############################################################################
-- Query 5 statements
-- #############################################################################
delete from query5;

insert into query5(
	select cid, cname, avghdi from 
		(select 
				c.cid, c.cname, avg(h.hdi_score) as avghdi 
			from country c 
		inner join hdi h on c.cid = h.cid 
			where h.year >= 2009 and h.year <= 2013
		group by c.cid) x order by x.avghdi desc limit 10);



-- #############################################################################
-- Query 6 statements
-- #############################################################################
delete from query6;

-- note: this query doesn't care if the trend
-- is not increasing on years other than mentioned 2009-2013
--
-- logic: select all take away those who decreased (like Assignment 1)
insert into query6(
	select 
		c.cid, c.cname from country c 
		where not c.cid in (
			select 
				distinct(t1.cid) 
			from hdi t1 
				inner join hdi t2 on 
					t1.cid = t2.cid and 
					t1.hdi_score > t2.hdi_score and  t1.year < t2.year
					and t1.year >= 2009 and t1.year < 2013
					and t2.year >= 2010 and t2.year <= 2013
		group by t1.cid) 
		
	order by c.cname asc);




-- #############################################################################
-- Query 7 statements
-- #############################################################################
delete from query7;

-- assuming rid/rname uniqueness as per:
-- https://piazza.com/class/hzpnvo6ud1l2lt?cid=358
insert into query7(
	select rid, rname, sum(npeople) as followers from 
	(
		select 
			rid, rname, rpercentage*c.population as npeople 
		from religion rg 
		inner join country c on c.cid = rg.cid
	) x group by x.rid, x.rname order by followers desc);


-- #############################################################################
-- Query 8 statements
-- #############################################################################
delete from query8;

create or replace view neighbors as
	select 
		distinct(friends.c1), friends.c2 
	from (
			select 
				n.country as c1, n.neighbor as c2 
			from neighbour n 
			union
			select 
				n.neighbor as c1, n.country as c2 
			from neighbour n) as friends 
				where not friends.c1 = friends.c2 	
				group by friends.c1, friends.c2;

create or replace view major_languages as
	select cid, lid, lname, max(lpercentage) 
	from language group by cid, lid, lname;

insert into query8(
select c1.cname as c1name, c2.cname as c2name, m.lname  from 
	country c1 inner join 
	neighbors n on n.c1 = c1.cid
    inner join major_languages m on m.cid = c1.cid
	inner join country c2 on n.c2 = c2.cid
	where exists 
	(select null from 
		major_languages m2 
		where m2.lid = m.lid and m2.cid = n.c2) 
	order by m.lname asc, c1name desc);

drop view major_languages;
drop view neighbors;




-- #############################################################################
-- Query 9 statements
-- #############################################################################
-- assuming "largest" vs "larger" (i.e. typo in assignment handout)
delete from query9;

create or replace view spans as
	select 
		c.cname, 
		case when x.lowest_point is null 
			then height 
			else height + x.lowest_point 
		end as span
	from country c inner join 
	(
		select oa.cid, max(o.depth) as lowest_point from  oceanaccess oa
		inner join ocean o on o.oid = oa.oid 
		group by oa.cid) x on c.cid = x.cid;

insert into query9(
	select 
		cname, span as totalspan 
	from spans s where s.span = (select max(span) from spans));

drop view spans;



-- #############################################################################
-- Query 10 statements
-- #############################################################################


delete from query10;

create or replace view neighbors as
	select 
		distinct(friends.c1), friends.c2 
	from (
			select 
				n.country as c1, n.neighbor as c2 
			from neighbour n 
			union
			select 
				n.neighbor as c1, n.country as c2 
			from neighbour n) as friends 
				where not friends.c1 = friends.c2 
				group by friends.c1, friends.c2;

create or replace view country_border_lengths as
select c.cid, c.cname, sum(n.length) borderslength from
country c inner join neighbors ns on c.cid = ns.c1
inner join neighbour n on (n.country = ns.c2 or n.neighbor = ns.c2)
group by c.cid, c.cname;

-- least takes care of bigint overflow edge case
insert into query10(
	select cname, least(+2147483647, borderslength) 
	from country_border_lengths cbl where 
	borderslength = (select max(borderslength) from country_border_lengths));

drop view country_border_lengths;
drop view neighbors;

