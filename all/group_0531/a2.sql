-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

Create view Neighbouring as Select c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name, c2.height as height
from country c1 inner join neighbour n on c1.cid = n.country inner join country c2 on c2.cid = n.neighbor 
order by c1.cid;

Create view TallestNeighbour as Select a.c1id as c1id, max(height) as tallest 
from Neighbouring a 
group by c1id;


insert into Query1(Select n.c1id, n.c1name, n.c2id, n.c2name
from TallestNeighbour t inner join Neighbouring n on t.c1id=n.c1id and t.tallest = n.height
order by c1name ASC);

drop view TallestNeighbour;

drop view Neighbouring;

-- Query 2 statements

insert into Query2(Select c.cid as cid, c.cname as cname
from country c
where not exists (Select * from oceanAccess oA where oA.cid = c.cid) 
order by cname ASC);


-- Query 3 statements

create view Landlocked as Select c.cid as cid, c.cname as cname
from country c
where not exists (Select * from oceanAccess oA where oA.cid = c.cid) 
order by cname ASC;

Create view Surrounded as Select c.cid as c1id, count(n.neighbor) as neigh
from Landlocked c inner join neighbour n on c.cid = n.country inner join country c2 on c2.cid = n.neighbor
group by c.cid
having count(n.neighbor) = 1;

insert into Query3 (Select c1.cid as c1id, c1.cname as c1name, c2.cid as c2, c2.cname as c2name
from Surrounded s inner join country c1 on c1.cid = s.c1id inner join neighbour n on c1.cid = n.country inner join country c2 on c2.cid = n.neighbor
order by c1name ASC);

drop view Surrounded;

drop view Landlocked;


-- Query 4 statements

create view DirecttoOcean as Select c.cid as cid, c.cname as cname, oA.oid as oid from country c inner join oceanAccess oA on c.cid = oA.cid; 

create view ProxytoOcean as Select c.cid as cid, c.cname as cname, DtO.oid as oid from country c inner join neighbour n on c.cid = n.country inner join DirecttoOcean DtO on n.neighbor = DtO.cid;

insert into Query4 (select naval.cname as cname, o.oname as oname
from (select cid, cname, oid from DirecttoOcean union select cid, cname, oid from ProxytoOcean) naval inner join ocean o on naval.oid = o.oid
order by cname ASC, oname DESC);

drop view ProxytoOcean;
drop view DirecttoOcean;

-- Query 5 statements

insert into Query5 (select c.cid as cid, c.cname as cname, avg(hdi_score) as avghdi
from country c inner join hdi h on h.cid = c.cid
where h.year >= 2009 and h.year <= 2013
group by c.cid
order by avghdi DESC limit 10);

-- Query 6 statements

CREATE VIEW hdiincrease as Select c.cid as cid, c.cname as cname, sum(h1.year) as s1, sum(h2.year) as s2
from country c inner join hdi h1 on h1.cid = c.cid
inner join hdi h2 on h2.cid = h1.cid
where h1.year - h2.year = 1 and h1.year >= 2009 and h1.year <= 2013 and h2.year >= 2009 and h2.year <= 2013 and h1.hdi_score - h2.hdi_score > 0
group by c.cid
having sum(h1.year) = 8046 and sum(h2.year) = 8042;

insert into Query6 (select cid, cname from hdiincrease);

DROP VIEW hdiincrease;

-- Query 7 statements - not ready

Create view followerspercountry as Select r.rid as rid, r.rname as rname, (r.rpercentage * c.population * 0.01) as followers, r.cid as cid
from country c inner join religion r on r.cid = c.cid;

Create view totalfollowers as select f.rid as rid, sum(f.followers) as followers from followerspercountry f group by f.rid;

insert into Query7 (select f.rid as rid, r.rname as rname, f.followers as followers from totalfollowers f inner join (select distinct rid, rname from religion) r on f.rid = r.rid order by followers DESC);

DROP VIEW totalfollowers;
DROP VIEW followerspercountry;



-- Query 8 statements

Create view mostpopularlanguage as Select m.cid as cid, m.cname as cname, l.lname as lname, l.lid as lid
from( Select c.cid as cid, c.cname as cname, max(l.lpercentage) as largest
from country c inner join language l on c.cid = l.cid
group by c.cid) m inner join language l on l.cid = m.cid where l.lpercentage = m.largest;

insert into Query8 (select l1.cname as c1name, l2.cname as c2name, l1.lname
from mostpopularlanguage l1 inner join neighbour n on l1.cid = n.country inner join mostpopularlanguage l2 on l2.cid = n.neighbor 
where l1.lid = l2.lid
order by lname ASC, c1name DESC);

drop view mostpopularlanguage;

-- Query 9 statements

Create view landlocktotalspan as Select c.cname as cname, c.height as totalspan 
from country c where not exists (Select * from oceanAccess oA where c.cid = oA.cid) and c.height = (Select max(height) from country);


Create view totalspanbycountry as Select c.cname as cname, (c.height + o.depth) as totalspan 
from country c inner join oceanAccess oA on c.cid = oA.cid inner join ocean o on o.oid = oA.oid
where c.height + o.depth = (Select max(o.depth + c.height) from country c inner join oceanAccess oA on c.cid = oA.cid inner join ocean o on o.oid = oA.oid);

insert into Query9 (Select cname, totalspan from (Select * from totalspanbycountry union Select * from landlocktotalspan) together
where together.totalspan = (Select max(combine.totalspan) from (Select cname, totalspan from totalspanbycountry union Select cname, totalspan from landlocktotalspan) combine));

DROP VIEW landlocktotalspan;

DROP VIEW totalspanbycountry;


-- Query 10 statements

create view borderslengths as Select c.cname as cname, sum(length) as borderlength
from country c inner join neighbour n on c.cid = n.country
group by c.cid
order by borderlength DESC;

insert into Query10(Select * from borderslengths where borderlength = (Select max(borderlength) from borderslengths));

drop view borderslengths;


