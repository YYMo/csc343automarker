-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

set search_path to a2;

-- Query 1 statements
-- Add the heights of each neighbour to the neighbour table
create view NeighbourHeight as
select N.country, N.neighbor, C.height, C.cname as nName
from neighbour N join country C on N.neighbor = C.cid;

insert into Query1
(select C.cid as c1id, C.cname as c1name, NH.neighbor as c2id, NH.nName as c2name 
from NeighbourHeight NH join country C on NH.country = C.cid
where NH.height >= all(select NH.height from NeighbourHeight NH where NH.country = C.cid)
order by c1name ASC);

drop view NeighbourHeight;

-- Query 2 statements
insert into Query2
(select ID.cid, C1.cname
from
(select C.cid 
from country C
except
select OA.cid 
from oceanAccess OA) ID
join country C1 on ID.cid = C1.cid
order by C1.cname ASC);


-- Query 3 statements
create view Landlocked as
(select ID.cid, C1.cname
from
(select C.cid 
from country C
except
select OA.cid 
from oceanAccess OA) ID
join country C1 on ID.cid = C1.cid
order by C1.cname ASC);

create view ExactlyOne as select country
from neighbour
group by country
having count(neighbor) = 1;

insert into Query3
(select L1.cid as c1id, L1.cname as c1name, N.neighbor as c2id,C1.cname as c2name
from neighbour N join
(select Q2.cid, Q2.cname
from Landlocked Q2 join
ExactlyOne Ex1
on Q2.cid = Ex1.country) L1
on N.country = L1.cid join country C1
on C1.cid = N.neighbor
order by L1.cname ASC);

Drop view ExactlyOne;
Drop view Landlocked;

-- Query 4 statements
create view BorderingCountries as
select N.country as cid, OA.oid
from oceanAccess OA join neighbour N on OA.cid = N.neighbor;

create view AccessibleOcean as 
select BC.cid, BC.oid
from BorderingCountries BC
Union select * from oceanAccess OA;

insert into Query4
(select C.cname, OC.oname
from AccessibleOcean AO
join ocean OC on AO.oid = OC.oid join country C on C.cid = AO.cid
order by C.cname ASC, OC.oname DESC);

drop view AccessibleOcean;

drop view BorderingCountries;

-- Query 5 statements
insert into Query5
(select hdi.cid, CO.cname, avg(hdi_score) as avghdi
from hdi join country CO on hdi.cid = CO.cid
where hdi.year between 2009 and 2013
group by hdi.cid, CO.cname
order by avghdi DESC
limit 10);

-- Query 6 statements
create view hdiByYear as
(select h1.cid, h1.hdi_score as "2009", h2.hdi_score as "2010", h3.hdi_score as "2011", h4.hdi_score as "2012", h5.hdi_score as "2013"
from hdi h1 join hdi h2 on h1.cid = h2.cid join hdi h3 on h2.cid = h3.cid  join hdi h4 on h3.cid = h4.cid
join hdi h5 on h4.cid = h5.cid
where h1.year = 2009 and h2.year = 2010 and h3.year = 2011 and h4.year = 2012 and h5.year = 2013);

insert into Query6
(select hd.cid, co.cname from hdiByYear hd join country co on hd.cid = co.cid 
where "2010">"2009" and "2011">"2010" and "2012">"2011" and "2013">"2012"
order by co.cname ASC);

drop view hdiByYear;

-- Query 7 statements
insert into Query7
(select R.rid, R.rname, sum(R.rpercentage*CO.population) as followers
from country CO join religion R on CO.cid = R.cid
group by R.rid, R.rname
order by followers DESC);

-- Query 8 statements
--Get the most popular language for each country
create view MostPopLang as
select cid, lname, lpercentage from language L1  
where lpercentage >= ALL(select lpercentage from language L2 where L1.cid = L2.cid);

--Get the pairs of neighbouring countries with the most popular language of the first country
create view MostPopCountry as
select N.country, N.neighbor, MPL.lname
from MostPopLang MPL join neighbour N on MPL.cid = N.country;

--Get the pairs of neighbouring countries with the most popular language of the neighbour
create view MostPopNeighbour as
select N.country, N.neighbor, MPL.lname
from MostPopLang MPL join neighbour N on MPL.cid = N.neighbor;

insert into Query8
(select C1.cname as c1name, C2.cname as c2name, MPC.lname
from MostPopCountry MPC join country C1 on MPC.country = C1.cid join country C2 on MPC.neighbor = C2.cid
where (MPC.country, MPC.neighbor, MPC.lname) in (
select MPN.country, MPN.neighbor, MPN.lname
from MostPopNeighbour MPN)
order by lname ASC, c1name DESC);

drop view MostPopNeighbour;
drop view MostPopCountry;
drop view MostPopLang;


-- Query 9 statements
-- Get the depth of each country's deepest ocean (if the country has access to an ocean)
create view DeepestOcean as
select OA.cid, max(O.depth) as MaxDepth
from oceanAccess OA join ocean O on OA.oid = O.oid
group by OA.cid;

-- Get the difference between highest elevation and depth of deepest ocean for all countries
-- If country does not have access to an ocean, its depth will be NULL, so use coalesce function to set it to 0 instead
create view HeightDepth as
select C.cname, C.height+coalesce(doc.MaxDepth,0) as totalspan
from DeepestOcean DOC right outer join country C on DOC.cid = C.cid;

-- Find the country with the largest distance
insert into Query9
(select cname, totalspan
from HeightDepth
where totalspan >=ALL(select totalspan from HeightDepth));

drop view HeightDepth;
drop view DeepestOcean;

-- Query 10 statements
create view BorderLength as
(select country as cname, sum(length) as borderslength
from neighbour
group by country);

insert into Query10
(select cname, borderslength
from BorderLength
where borderslength >= all(select borderslength from BorderLength));

drop view BorderLength;

