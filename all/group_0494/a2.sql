-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

create view joint as
select c1.cid as c1cid, c1.cname as c1cname, c1.height as c1height, c2.cid as c2cid, c2.cname as c2cname, c2.height as c2height
from  neighbour, country c1, country c2
where c1.cid = neighbour.country and c2.cid = neighbour.neighbor;

INSERT INTO Query1 (
select c1cid as c1id, c1cname as c1name, c2cid as c2id, c2cname as c2name
from joint j1
where (c1cid, c2height) in (select c1cid, max(c2height) from joint j2 group by j2.c1cid)
order by c1cname asc);

drop view joint;

-- Query 2 statements

INSERT INTO Query2 (
select c1.cid as cid, c1.cname as cname
from country c1
where not c1.cid in (
	select o1.cid
	from oceanAccess o1
)
order by c1.cname
);

-- Query 3 statements?

create VIEW v1 as
select q1.cid as cid, q1.cname as cname
from
(select c1.cid, c1.cname
from country c1
where not c1.cid in (
	select o1.cid
	from oceanAccess o1
)
intersect
select c1.cid, c1.cname
from country c1
where 1 = (select count(*) from neighbour where c1.cid = country)) as q1
order by q1.cname;

INSERT INTO Query3 (
select v1.cid as c1id, v1.cname as c1name, c1.cid as c2id, c1.cname as c2name
from v1, neighbour n1, country c1
where v1.cid = n1.country and c1.cid = n1.neighbor
order by v1.cname asc
);

drop VIEW v1;

-- Query 4 statements
INSERT INTO Query4 (
select q1.cname as cname, q1.oname as oname
from
	(select cname, oname
	from oceanAccess natural join country natural join ocean
	union
	select c1.cname, oname
	from country c1, neighbour, oceanAccess natural join ocean
	where c1.cid = neighbour.country and neighbour.neighbor = oceanAccess.cid) as q1
order by q1.cname asc, q1.oname desc);

-- Query 5 statements

create VIEW avg_hdi as
	select cid, avg(hdi_score) as avghdi
	from hdi
	where year >= 2009 and year <= 2013
	group by cid;

INSERT INTO Query5 (
select cid as cid, cname as cname, avghdi as avghdi
from avg_hdi natural join country
order by avghdi desc
limit 10);

drop view avg_hdi;

-- Query 6 statements

create VIEW hdi_increase as
	select h1.cid, count(*) as incyr
	from hdi h1, hdi h2
	where h1.cid = h2.cid and h1.year = h2.year - 1 and h1.year >= 2009 and h1.year <= 2012 and h2.hdi_score > h1.hdi_score
	group by h1.cid;
INSERT INTO Query6 (
select cid as cid, cname
from hdi_increase natural join country
where incyr = 4
order by cname);

drop View hdi_increase;

-- Query 7 statements
INSERT INTO Query7 (
select distinct rid, rname, followers
from religion natural join
	(select r.rid, sum(r.rpercentage * c.population) as followers
	from religion r natural join country c
	group by r.rid) as q1
order by followers desc);

-- Query 8 statements

create VIEW mostpopular as 
	(select cid,lid
	from language natural join
		(select cid, max(lpercentage) as lpercentage
		from language
		group by cid) as q1);
INSERT INTO Query8 (
select distinct c1.cname as c1name, c2.cname as c2name, l1.lname as lname
from 
	(select p1.cid as cid1, p2.cid as cid2, p1.lid as lid
	from neighbour, mostpopular p1, mostpopular p2
	where neighbour.country = p1.cid and neighbour.neighbor = p2.cid and p1.lid = p2.lid) as q1, country c1, country c2, language l1
where q1.cid1 = c1.cid and q1.cid2 = c2.cid and q1.lid = l1.lid
order by l1.lname asc, c1.cname desc);

drop VIEW mostpopular;

-- Query 9 statements

create VIEW deepest as
	select cid, max(depth) as depth
	from oceanAccess natural join ocean
	group by cid;
INSERT INTO Query9 (
select  country.cname as cname, country.height + deepest.depth as totalspan
from deepest natural join country
where country.height + deepest.depth = (select max(country.height + deepest.depth) from deepest natural join country));

drop VIEW deepest;

-- Query 10 statements
create VIEW v1 as
select n1.country, sum(n1.length) as length
from neighbour n1
group by n1.country
having sum(n1.length) >= all (
        select sum(n2.length)
        from neighbour n2
        group by n2.country
        );

INSERT INTO Query10 (
select cname, length as borderslength
from v1 join country on v1.country = country.cid);

drop View v1;

