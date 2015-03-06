-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

create view highestElevation as
select country, max(height) as height
from country join neighbour on neighbor=cid
group by country;

create view preformat as
select country as c1id, cid as c2id, cname as c2name
from country join neighbour on neighbor = cid
where (country, height) in (select * from highestElevation);

INSERT INTO Query1 (
select c1id, cname as c1name, c2id, c2name
from preformat join country on c1id=cid
);

DROP VIEW preformat;
DROP VIEW highestElevation;

-- Query 2 statements

INSERT INTO Query2(
select cid, cname
from country
where cid NOT IN (
	select cid
	from country natural join oceanAccess)
order by cname
);

-- Query 3 statements

create view oneNeighbour as 
select country as cid
from neighbour
group by country
having count(neighbor) = 1;

create view landlocked as
select cid, cname
from country
where cid NOT IN (
        select cid
        from country natural join oceanAccess);

create view preformat as
select cid as c1id, cname as c1name, neighbor
from landlocked join neighbour on cid=country
where cid in (select cid from oneNeighbour);

INSERT INTO Query3 (
select c1id, c1name, neighbor as c2id, cname as c2name
from preformat join country on neighbor=cid
order by c1name
);

DROP VIEW preformat;
DROP VIEW landlocked;
DROP VIEW oneNeighbour;

-- Query 4 statements

create view directAccess as
select cid, oid
from oceanAccess;

create view indirectAccess as
select country as cid, oid
from oceanAccess join neighbour on neighbor=cid;

create view allAccess as
(select * from directAccess) UNION (select * from indirectAccess);

INSERT INTO Query4 (
select cname, oname
from allAccess natural join country natural join ocean
order by cname, oname DESC
);

DROP VIEW allAccess;
DROP VIEW indirectAccess;
DROP VIEW directAccess;


-- Query 5 statements

create view betweenYears as
select cid, hdi_score
from hdi
where year >= 2009 and year <= 2013;

create view avghdipreformat as
select cid, avg(hdi_score) as avghdi
from betweenYears
group by cid;

INSERT INTO Query5(
select cid, cname, avghdi 
from avghdipreformat natural join country 
order by avghdi DESC limit 10
);

DROP VIEW avghdipreformat;
DROP VIEW betweenYears;

-- Query 6 statements

create view betweenYears as 
select * 
from hdi 
where year <= 2013 and year >= 2009;

create view preformat as
select b1.cid, b1.hdi_score, b1.year 
from betweenYears b1, betweenYears b2
where b1.cid = b2.cid and (b1.year - b2.year) = 1
and b1.hdi_score > b2.hdi_score;

INSERT INTO Query6(
select DISTINCT cid, cname
from preformat natural join country
);

DROP VIEW preformat;
DROP VIEW betweenYears;

-- Query 7 statements

create view popofcountry as
select cid, population
from country;

INSERT INTO Query7 (
select rid, rname, sum(rpercentage*population) as followers
from religion natural join popofcountry
group by rid, rname
order by followers DESC
);

DROP VIEW popofcountry;

-- Query 8 statements

create view countryMaxlperc as
select cid, max(lpercentage) as lpercentage
from language
group by cid;

create view mostPopLang as
select cid, lid, lname
from language
where (cid, lpercentage) in (select * from countryMaxlperc);

create view mostPopLangNeighbours as
select cid, cname, lid, lname, neighbor
from mostPopLang join neighbour on country = cid natural join country;

INSERT INTO Query8 (
select m1.cname as c1name, m2.cname as c2name, m1.lname as lname          
from mostPopLangNeighbours m1,  mostPopLangNeighbours m2
where m1.cid = m2.neighbor AND m1.lname = m2.lname
order by lname, c1name DESC
);

DROP VIEW mostPopLangNeighbours;
DROP VIEW mostPopLang;
DROP VIEW countryMaxlperc;


-- Query 9 statements

create view tspanhasocean as
select cid, cname, height - depth as totalspan
from country natural join oceanAccess
natural join ocean;

create view tspanlandlocked as
select cid, cname, height as totalspan
from country
where cid NOT IN (
        select cid
        from country natural join oceanAccess);

create view countrytspan as
(select * from tspanlandlocked) union (select * from tspanhasocean);

create view maxcountrytspan as
select cname, max(totalspan) as totalspan
from countrytspan
group by cname;

INSERT INTO Query9 (
select *
from maxcountrytspan
where totalspan in (select max(totalspan) as totalspan from maxcountrytspan)
);

DROP VIEW maxcountrytspan;
DROP VIEW countrytspan;
DROP VIEW tspanlandlocked;
DROP VIEW tspanhasocean;

-- Query 10 statements

create view sumBorders as 
select country, sum(length) as borderslength
from neighbour
group by country;

INSERT INTO Query10 (
select cname, borderslength
from sumBorders join country on country=cid
where borderslength in (
select max(borderslength) from sumBorders)
);

DROP VIEW sumBorders;


