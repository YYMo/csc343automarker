-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
create view q1_view as 
	select n.country c1id, c1.cname c1name,  max(c2.height) height 
	from neighbour n 
	inner join country c1 on n.country = c1.cid 
	inner join country c2 on n.neighbor = c2.cid 
	group by c1id, c1name;

insert into query1
	(select q1.c1id ci1d, q1.c1name c1name, c.cid c2id, c.cname c2name from
	q1_view q1
	inner join country c on c.height = q1.height order by c1name asc);

drop view q1_view;


-- Query 2 statements
insert into query2
select c.cid cid, c.cname cname
	from country c
	where c.cid not in (select cid from oceanAccess)
	order by cname asc;


-- Query 3 statements
create view landlocked as 
select c.cid cid, c.cname cname
	from country c
	where c.cid not in (select cid from oceanAccess)
	order by cname asc;

create view landlocked2 as
select c.cid cid, c.cname cname
	from neighbour n
	inner join country c on n.country = c.cid
	where n.country in (select cid from landlocked)
	group by c.cid, c.cname
	having count(*) = 1;

insert into query3
select l2.cid c1id, l2.cname c1name, c2.cid c2id, c2.cname c2name
	from landlocked2 l2
	inner join neighbour n on l2.cid = n.country
	inner join country c2 on n.neighbor = c2.cid
	order by c1name asc;

drop view landlocked2;
drop view landlocked;

-- Query 4 statements
create view directAccess as
	select c.cname cname, o.oname oname
	from oceanAccess oa
	inner join country c on c.cid = oa.cid
	inner join ocean o on o.oid = oa.oid;

create view indirectAccess as
	select c1.cname cname, dn.oname oname
	from neighbour n
	inner join country c1 on c1.cid = n.country
	inner join country c2 on c2.cid = n.neighbor
	inner join directAccess dn on dn.cname = c2.cname;
	
insert into query4
select * from directAccess
union
select * from indirectAccess
order by cname asc, oname desc;

drop view indirectAccess;
drop view directAccess;

-- Query 5 statements
create view hdi_range as
	select * from hdi
	where year >= 2009
	and year <= 2013;

insert into query5
select c.cid cid, c.cname cname, AVG(h.hdi_score) avghdi
	from country c
	inner join hdi_range h on h.cid = c.cid
	group by c.cid, c.cname
	order by avghdi desc
	limit 10;

drop view hdi_range;

-- Query 6 statements
create view inc20092010 as
	select c.cid cid, c.cname cname
	from country c
	inner join hdi h1 on h1.cid = c.cid
	inner join hdi h2 on h2.cid = c.cid
	where h1.year = 2009
	and h2.year = 2010
	and h1.hdi_score < h2.hdi_score;

create view inc20102011 as
	select c.cid cid, c.cname cname
	from country c
	inner join hdi h1 on h1.cid = c.cid
	inner join hdi h2 on h2.cid = c.cid
	where h1.year = 2010
	and h2.year = 2011
	and h1.hdi_score < h2.hdi_score
	and c.cid in (select cid from inc20092010);

create view inc20112012 as
	select c.cid cid, c.cname cname
	from country c
	inner join hdi h1 on h1.cid = c.cid
	inner join hdi h2 on h2.cid = c.cid
	where h1.year = 2011
	and h2.year = 2012
	and h1.hdi_score < h2.hdi_score
	and c.cid in (select cid from inc20102011);

create view inc20122013 as
	select c.cid cid, c.cname cname
	from country c
	inner join hdi h1 on h1.cid = c.cid
	inner join hdi h2 on h2.cid = c.cid
	where h1.year = 2012
	and h2.year = 2013
	and h1.hdi_score < h2.hdi_score
	and c.cid in (select cid from inc20112012);

insert into query6
	select * from inc20122013
	order by cname asc;

drop view inc20122013;
drop view inc20112012;
drop view inc20102011;
drop view inc20092010;

-- Query 7 statements
insert into query7
	select r.rid rid, r.rname rname, SUM(c.population * r.rpercentage) followers
	from religion r
	inner join country c on r.cid = c.cid
	group by rid, rname
	order by followers desc;

-- Query 8 statements
create view popularLanguages as
	select c.cid cid, MAX(lpercentage) lpercent
	from country c
	inner join language l
	on l.cid = c.cid
	group by c.cid;

create view pl2 as
	select pl.cid cid, l.lid lid
	from language l
	inner join popularLanguages pl on pl.lpercent = l.lpercentage
	where l.cid = pl.cid;

insert into query8
select distinct c1.cname c1name, c2.cname c2name, l.lname lname
	from pl2
	inner join pl2 pl22 on pl2.lid = pl22.lid
	inner join country c1 on c1.cid = pl2.cid
	inner join country c2 on c2.cid = pl22.cid
	inner join language l on pl2.lid = l.lid
	where c1.cid <> c2.cid
	order by lname asc, c1name desc;
	
drop view pl2;
drop view popularLanguages;

-- Query 9 statements
create view directAccess as
	select c.cname cname, MAX(o.depth) depth
	from oceanAccess oa
	inner join country c on c.cid = oa.cid
	inner join ocean o on o.oid = oa.oid
	group by cname;

create view noAccess as
	select c.cname, 0 depth
	from country c
	where c.cname not in (select cname from directAccess);

create view cDepth as
	select * from noAccess
	union
	select * from directAccess;

insert into query9
select c.cname cname, (c.height + cd.depth) totalspan
from country c
inner join cDepth cd on c.cname = cd.cname;

drop view cDepth;
drop view noAccess;
drop view directAccess;

-- Query 10 statements

create view borderLengths as
	select n.country, SUM(length) totalLength
	from neighbour n
	group by country;

insert into query10
	select c.cname cname, bl.totalLength borderslength
	from borderLengths bl
	inner join country c on bl.country = c.cid
	order by bl.totalLength desc
	limit 1;

drop view borderLengths;

