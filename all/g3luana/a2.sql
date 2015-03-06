-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
create view  highestelevpoint as
select country, max(height) as maxheightofneighb
from neighbour, country
where neighbor = cid
group by country;

create view neighbelevpt as
select h.country as country, c.cid as neighbcountry
from highestelevpoint h, country c
where h.maxheightofneighb = c.height;

INSERT INTO Query1(select n.country as c1id, c1.cname as c1name, n.neighbcountry as c2id, c2.cname as c2name
from neighbelevpt n, country c1, country c2
where n.country=c1.cid and n.neighbcountry=c2.cid
order by c1name);

drop view neighbelevpt;
drop view highestelevpoint;

-- Query 2 statements
create view landcountriescid as
(select cid from country) 
EXCEPT 
(select cid from oceanAccess);

INSERT INTO Query2(select landcountriescid.cid as cid, country.cname as cname
from landcountriescid, country
where landcountriescid.cid=country.cid
order by cname);

drop view landcountriescid;

-- Query 3 statements
create view landcountriescid as
(select cid from country)
EXCEPT
(select cid from oceanAccess);

create view oneneighb as
select country
from neighbour
group by country
having count(neighbor) = 1;

INSERT INTO Query3(select oneneighb.country as c1id, c1.cname as c1name, neighbour.neighbor as c2id, c2.cname as c2name
from oneneighb, country c1, neighbour, country c2
where oneneighb.country = c1.cid and oneneighb.country = neighbour.country and neighbour.neighbor = c2.cid
order by c1name);

drop view oneneighb;
drop view landcountriescid;

-- Query 4 statements
create view neighboroceanaccess as
select neighbor, oid
from oceanAccess, neighbour
where cid = country;

create view alloceanaccess as
(select neighbor as cid, oid from neighboroceanaccess)
UNION
(select * from oceanAccess);

INSERT INTO Query4(select country.cname as cname, ocean.oname as oname
from alloceanaccess, country, ocean
where alloceanaccess.cid = country.cid and alloceanaccess.oid = ocean.oid
order by cname ASC, oname DESC);

drop view alloceanaccess;
drop view neighboroceanaccess;

-- Query 5 statements
create view years as
select * 
from hdi 
where year=2009 or year=2010 or year=2011 or year=2012 or year=2013;

create view highesthdi as
select cid, avg(hdi_score) as avghdi
from years
group by cid
order by avg(hdi_score) DESC
limit 10;

INSERT into Query5(select h.cid as cid, c.cname as cname, h.avghdi
from highesthdi h join country c on h.cid=c.cid);

drop view highesthdi;
drop view years;

-- Query 6 statements

create view two009incrto2010 as
select h1.cid
from hdi h1, hdi h2
where h1.cid=h2.cid and h1.year=2009 and h2.year=2010 and h1.hdi_score<h2.hdi_score;

create view two010incrto2011 as
select h1.cid
from hdi h1, hdi h2
where h1.cid=h2.cid and h1.year=2010 and h2.year=2011 and h1.hdi_score<h2.hdi_score;

create view two011incrto2012 as
select h1.cid
from hdi h1, hdi h2
where h1.cid=h2.cid and h1.year=2011 and h2.year=2012 and h1.hdi_score<h2.hdi_score;

create view two012incrto2013 as
select h1.cid
from hdi h1, hdi h2
where h1.cid=h2.cid and h1.year=2012 and h2.year=2013 and h1.hdi_score<h2.hdi_score;

create view incrhdi as
(select * from two009incrto2010) 
INTERSECT 
(select * from two010incrto2011) 
INTERSECT 
(select * from two011incrto2012) 
INTERSECT 
(select * from two012incrto2013);

INSERT into Query6(select incrhdi.cid as cid, country.cname as cname
from incrhdi, country
where incrhdi.cid=country.cid
order by cname);

drop view incrhdi;
drop view two012incrto2013;
drop view two011incrto2012;
drop view two010incrto2011;
drop view two009incrto2010;

-- Query 7 statements

create view religionpops as
select religion.cid, rid, rname, cast((rpercentage*population) as int) as rpop
from religion join country on religion.cid=country.cid;

INSERT into Query7(select rid, rname, sum(rpop) as followers
from religionpops
group by rid, rname
order by followers DESC);

drop view religionpops;


-- Query 8 statements

create view mostpopular as
select cid, max(lpercentage) as maxp
from language
group by cid;

create view withlname as
select mostpopular.cid, language.lname as lname
from mostpopular, language
where mostpopular.cid = language.cid and mostpopular.maxp = language.lpercentage; 

create view answercid as
select w1.cid as c1cid, w2.cid as c2cid, w1.lname as lname
from withlname w1, withlname w2, neighbour n
where w1.cid <> w2.cid and w1.lname = w2.lname and w1.cid = n.country and w2.cid = n.neighbor and w1.cid < w2.cid;

INSERT INTO Query8(elect c1.cname as c1name, c2.cname as c2name, a.lname as lname
from answercid a, country c1, country c2
where a.c1cid = c1.cid and a.c2cid = c2.cid 
order by lname ASC, c1name DESC);

drop view answercid;
drop view withlname;
drop view mostpopular;

-- Query 9 statements

create view landc as
(select cid from country)
EXCEPT
(select cid from oceanAccess);

create view landelev as
select landc.cid as cid, country.height as diff
from landc join country on landc.cid=country.cid;

create view oceanelev as
select country.cid, (country.height+ocean.depth) as diff
from country, ocean, oceanAccess
where country.cid=oceanAccess.cid and ocean.oid=oceanAccess.oid; 

create view allelev as
(select * from landelev) UNION (select * from oceanelev);

INSERT INTO Query9(select country.cname, diff as totalspan
from allelev join country on allelev.cid=country.cid
where diff = (select max(diff)
    from allelev));

drop view allelev;
drop view oceanelev;
drop view landelev;
drop view landc;

-- Query 10 statements

create view alllengths as
select cname, sum(length) as borderslength
from neighbour join country on country=cid
group by country, cname;

INSERT INTO Query10(select cname, borderslength
from alllengths
where borderslength = (select max(borderslength)
    from alllengths));

drop view alllengths;

