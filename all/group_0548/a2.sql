-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.


CREATE VIEW landlocked AS
select *
from country
where cid not in (select cid 
     from oceanAccess);

-- Query 1 statements

INSERT INTO Query1 (
  select cC.cid as c1id, cC.cname as c1name, cN.cid as c2id, cN.cname as c2name
  from neighbour as n 
    join country as cN on n.neighbor=cN.cid 
    join country as cC on n.country=cC.cid
  group by cC.cid, cN.cid
  having cN.height = max(cN.height)
  order by c1name ASC
  );

-- Query 2 statements
INSERT INTO Query2 (
  select cid, cname
  from landlocked
  order by cname asc
);

-- Query 3 statements
INSERT INTO Query3 (
  select l.cid as c1id, l.cname as c1name, c.cid as c2id, c.cname as c2name
  from landlocked as l
  join neighbour as n on l.cid = n.country
  join country as c on c.cid = n.neighbor
  group by l.cid, l.cname
  having count(n.neighbor)=1
  order by c1name ASC
);
drop view landlocked;

-- Query 4 statements

CREATE VIEW direct AS
select c.*, o.*
from country as c
join OceanAccess as oA on c.cid = oA.cid
join ocean as o on o.oid = oA.oid
group by c.cid, oA.cid, oA.oid, o.oid;


CREATE VIEW indirect AS
select c.cname, o.oname
from direct as d 
join ocean as o on d.oid = o.oid
join neighbour as n on d.cid = n.country
join country as c on n.neighbor = c.cid
group by c.cid, o.oname;

INSERT INTO Query4 (  select cname,oname from direct
  union
  select cname,oname from indirect as i
  group by cname, i.oname
  order by cname asc, oname desc 
); 

drop view indirect;
drop view direct;

-- Query 5 statements
INSERT INTO Query5 (
  select c.cid, c.cname, avg(hdi.hdi_score) as avghdi
  from hdi
  join country as c on c.cid = hdi.cid and hdi.year >= 2009 and hdi.year <= 2013
  group by hdi.cid, c.cid, c.cname
  order by avghdi desc
  limit 10
);

-- Query 6 statements

CREATE VIEW hdi_diffs AS
select h1.cid, h1.year as year, h2.hdi_score - h1.hdi_score as hdi_diff
from hdi as h1
 join hdi as h2 on h1.year = h2.year - 1 and h1.cid=h2.cid;

INSERT INTO Query6 (
  select c.cid as cid, c.cname as cname from hdi_diffs as h 
   join country as c on h.cid = c.cid
  where year>=2009 and year <= 2013 and hdi_diff>0
  group by c.cid
  having count(year)=5
  order by cname ASC
 );
drop view hdi_diffs;

-- Query 7 statements

INSERT INTO Query7 (
  select r.rid,r.rname, sum(r.rpercentage*c.population) as followers
  from religion as r 
  join country as c on r.cid = c.cid
  group by r.rid,r.rname
  order by followers desc
);

-- Query 8 statements
CREATE VIEW most_popular_language AS
select * from language
  group by cid, language.lid
  having lpercentage = max(lpercentage);

INSERT INTO Query8 (
  select c1.cname as c1name, c2.cname as c2name, m1.lname as lname
   from neighbour as n 
   join most_popular_language as m1 on m1.cid = n.country
   join most_popular_language as m2 on m2.cid = n.neighbor
   join country as c1 on c1.cid = n.country
   join country as c2 on c2.cid = n.neighbor
  where m1.lname = m2.lname and c1.cname < c2.cname
  order by lname ASC, c1name desc
);

drop view most_popular_language;

-- Query 9 statements
INSERT INTO Query9 (
  select cname,  max(coalesce(depth, 0) + height) as totalspan
  from country c
  left outer join oceanaccess oa on c.cid = oa.cid
  left outer join ocean o on oa.oid = o.oid
  group by c.cname
);


-- Query 10 statements
INSERT INTO Query10 (
  select c.cname, sum(length) as borderslength
  from neighbour as n
  join country as c on n.country = c.cid
  group by(country), c.cname
);
