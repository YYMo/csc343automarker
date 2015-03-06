-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
create view temp as
	select cid as c1id, cname as c1name, neighbor as c2id 
	from country, neighbour 
	where country.cid = country;
	
create view temp2 as
	SELECT c1id, c1name, c2id, country.cname as c2name, height
	from temp, country
	where c2id = country.cid;
	
INSERT INTO Query1 (
select temp2.c1id, c1name, c2id, c2name from (SELECT c1id, max(height)
	from temp2
	group by c1id) maxh, temp2
where temp2.height = maxh.max and temp2.c1id = maxh.c1id
order by c1name
);

drop view if exists temp cascade;
drop view if exists temp2 cascade;

-- Query 2 statements
INSERT INTO Query2 (
select c1.cid, c1.cname from country c1
where not exists(select c1.cid from oceanAccess where c1.cid = cid)
order by c1.cname
);

-- Query 3 statements
create view temp as
	select c1.cid as c1id, c1.cname as c1name from country c1
	where not exists(select c1.cid from oceanAccess where c1.cid = cid);
	
create view newneighbour as
	select n.country as cid, n.neighbor as nid, c.cname as nname from country c, neighbour n
	where n.neighbor = c.cid;
	
create view temp2 as	
select c1id, c1name, nid as c2id, nname as c2name from temp, newneighbour
where c1id = cid;

create view temp3 as	
select c1id, count(c2id) from temp2 group by c1id;

INSERT INTO Query3 (
select t2.c1id, t2.c1name, t2.c2id, t2.c2name from temp2 t2, temp3 t3
where t2.c1id = t3.c1id and t3.count = 1
order by t2.c1name)
;

drop view if exists temp cascade;
drop view if exists newneighbour cascade;
drop view if exists temp2 cascade;
drop view if exists temp3 cascade;
-- Query 4 statements
create view newneighbour as
	select n.country as cid, n.neighbor as nid, c.cname as nname from country c, neighbour n
	where n.neighbor = c.cid;

INSERT INTO Query4 (
select distinct cname, oname from
(select c.cname, o.oname from country c, ocean o, oceanAccess oa
where o.oid = oa.oid and c.cid = oa.cid
union
select c.cname, o.oname from country c, newneighbour nn, ocean o, oceanAccess oa
where o.oid = oa.oid and c.cid = nn.cid and (c.cid = oa.cid or nn.nid = oa.cid)) u
order by cname, oname desc
);

drop view if exists newneighbour cascade;
-- Query 5 statements
INSERT INTO Query5 (
select c.cid, c.cname, avg(h.hdi_score) as avghdi from country c, hdi h 
where c.cid = h.cid and h.year <= 2013 and h.year >= 2009
group by c.cid, c.cname
order by avghdi desc limit 10
);

-- Query 6 statements
INSERT INTO Query6 (
select distinct c.cid, c.cname from country c, hdi h 
where c.cid = h.cid and h.year <= 2013 and h.year >= 2009 and 
not exists(select c.cid from hdi h2 where h.cid = h2.cid and h.year < h2.year and h.hdi_score >= h2.hdi_score)
order by cname
);

-- Query 7 statements
INSERT INTO Query7 (
select rid, rname, sum(rpercentage * 0.01 * population) as followers from country c, religion r
where c.cid = r.cid
group by rid, rname order by followers desc
);

-- Query 8 statements
create view newneighbour as
	select n.country as cid, n.neighbor as nid, c.cname as nname from country c, neighbour n
	where n.neighbor = c.cid;

create view mlanguage as
select c.cid, c.cname, l.lid, l.lname, l.lpercentage from country c, language l
where c.cid = l.cid and 
not exists (select lname from language l2 where l.cid = l2.cid and l.lid <> l2.lid and l.lpercentage < l2.lpercentage);

INSERT INTO Query8 (
select m1.cname as c1name, m2.cname as c2name, m1.lname from mlanguage m1, mlanguage m2 
where m1.cname <> m2.cname and m1.lname = m2.lname
order by lname, c1name desc
);

drop view if exists newneighbour cascade;
drop view if exists mlanguage cascade;

-- Query 9 statements
INSERT INTO Query9 (
select cname, totalspan from
((select c.cname, max(c.height+o.depth) as totalspan from country c, ocean o, oceanAccess oa
where c.cid = oa.cid and o.oid = oa.oid group by c.cname) 
union 
(select c1.cname, c1.height as totalspan from country c1
where not exists(select c1.cid from oceanAccess oa2 where c1.cid = oa2.cid))) temp
order by totalspan desc limit 1
);


-- Query 10 statements
INSERT INTO Query10 (
select c.cname , sum(n.length) as borderslength from country c, neighbour n
where c.cid = n.country group by c.cid order by borderslength desc limit 1
);

