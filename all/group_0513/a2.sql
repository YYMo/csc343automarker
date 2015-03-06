-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
insert into Query1(
select c1.cid c1id, c1.cname c1name, c2.cid c2id, c2.cname c2name 
	from neighbour, country c1, country c2
	where country = c1.cid and	neighbor = c2.cid
	and c2.height >= all
		(select c.height from neighbour n, country c
		where	n.country = c1.cid and
			n.neighbor = c.cid
		)
	order by c1.cname asc);
	

-- Query 2 statements
insert into Query2(
select cid, cname	
	from country
	where not exists
		(select * from oceanAccess o
			where country.cid = o.cid)
	order by cname asc);


-- Query 3 statements
insert into Query3(
select c2.cid c1id, c2.cname c1name, c1.cid c2id, c1.cname c2name
	from neighbour, country c1, country c2
	where country = c1.cid and	neighbor = c2.cid
	and not exists
		(select * from oceanAccess o
			where c2.cid = o.cid)
	and not exists
		(select * from neighbour
			where (not neighbor = c1.cid)
			and country = c2.cid)
	order by c2.cname asc);


-- Query 4 statements
insert into Query4(
select	distinct c1.cname,	o2.oname
	from country c1, oceanAccess o1, ocean o2
	where (o1.oid = o2.oid and c1.cid = o1.cid)
	or exists
		(select * from neighbour n, oceanAccess o3
			where n.country = c1.cid
			and n.neighbor = o3.cid
			and o3.oid = o2.oid)
	order by c1.cname asc,o2.oname desc)
	;



-- Query 5 statements
insert into Query5(
select c.cid, c.cname, avg(h.hdi_score) avghdi
	from country c, hdi h
	where year >= 2009
	and year <= 2013
	and c.cid = h.cid
	group by c.cid
	order by avg(h.hdi_score) desc
	limit 10);



-- Query 6 statements
insert into Query6(
select c.cid, c.cname
	from country c, hdi h1
	where c.cid = h1.cid
	and h1.year = 2013
	and exists
	(select * from hdi h2
	where h2.cid = c.cid
	and h2.year = 2012
	and h2.hdi_score < h1.hdi_score
	and exists
	(select * from hdi h3
	where h3.cid = c.cid
	and h3.year = 2011
	and h3.hdi_score < h2.hdi_score
	and exists
	(select * from hdi h4
	where h4.cid = c.cid
	and h4.year = 2010
	and h4.hdi_score < h3.hdi_score
	and exists
	(select * from hdi h5
	where h5.cid = c.cid
	and h5.year = 2009
	and h5.hdi_score < h4.hdi_score
	)
	)
	)
	)
	order by c.cname asc)
	;
	
	


-- Query 7 statements
insert into Query7(
select r.rid, r.rname, sum(r.rpercentage*c.population) followers 
from religion r, country c
where r.cid = c.cid
group by r.rid, r.rname
order by followers desc)
;

-- Query 8 statements
drop view if exists toplang CASCADE;
create view toplang as
select cid, lname from language l1
where lpercentage >= all
(select l2.lpercentage from language l2
where l1.cid = l2.cid);

drop view if exists tlpair CASCADE;
create view tlpair as
select t1.cid c1, t2.cid c2, t1.lname
from toplang t1, toplang t2, neighbour
where t1.lname = t2.lname
and t1.cid = country
and t2.cid = neighbor;

insert into Query8(
select c1.cname c1name, c2.cname c2name, lname
from country c1, country c2, tlpair
where c1.cid = c1
and c2.cid = c2
order by c1.cname asc, lname desc
);

-- Query 9 statements
drop view if exists withsea cascade;
create view withsea as
select c.cname, (o2.depth + c.height) totalspan
from country c natural join oceanAccess o1 natural join ocean o2;

insert into Query9(
select cname, totalspan from withsea
where totalspan >= all
(select totalspan from withsea)
and totalspan >= all
(select height from country)
);

insert into Query9(
select cname, height totalspan from country
where height >= all
(select totalspan from withsea)
and height >= all
(select height from country)
);

-- Query 10 statements
drop view if exists length cascade;
create view length as
select c.cname, sum(n.length) slength
from country c, neighbour n
where c.cid = n.country
group by c.cname;


insert into Query10(
select l1.cname, l1.slength borderslength
from length l1
where l1.slength >= all
(select l2.slength
from length l2)
);

