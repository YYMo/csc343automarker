-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
-- For each country, find its neighbor country with the highest elevation point. 
-- Report the id and name of the country and the id and name of its neighboring country. 
create or replace view neighborInfo as
select N.cname as n_name, N.neighbor as n_cid, country.cname as c_name, N.country as c_id 
	from (country join neighbour on country.cid = neighbour.neighbor) N join country on N.country = country.cid;

create or replace view maxneighbor as
select y.country, y.neighbor 
	from 
		(select country, max(height) from neighbour join country on neighbour.neighbor = country.cid group by country) x,
		(select country, height, neighbor from country join neighbour on country.cid = neighbour.neighbor) y
	where 
		x.country = y.country and x.max = y.height;

create or replace view answer as 
select c_id as c1id, c_name as c1name, n_cid as c2id, n_name as c2name 
	from maxneighbor join neighborInfo on maxneighbor.country = c_id and maxneighbor.neighbor = n_cid
		order by c1name ASC;

INSERT INTO Query1 (SELECT * from answer); 

DROP view IF EXISTS neighborInfo CASCADE;
DROP view IF EXISTS maxneighbor CASCADE;
DROP view IF EXISTS answer CASCADE;

-- Query 2 statements
-- Find the landlocked countries. A landlocked country is a country 
-- entirely enclosed by land (e.g., Switzerland). Report the id(s) 
-- and name(s) of the landlocked countries.

INSERT INTO Query2 (
select distinct country as cid, cname
	from neighbour n join country on n.country = country.cid
		where n.country not in (
			select cid from oceanAccess
		)
	order by cname ASC
);

-- Query 3 statements
-- Find the landlocked countries which are surrounded by exactly one country. 
-- Report the id and name of the landlocked country, followed by the id and 
-- name of the country that surrounds it.

create or replace view landlocked as
select distinct country as cid, cname
	from neighbour n join country on n.country = country.cid
		where n.country not in (
			select cid from oceanAccess
		);

create or replace view lonelandlocked as
select distinct country
	from landlocked join neighbour
		on landlocked.cid = neighbour.country
		group by country
			having count(neighbor) = 1;

create or replace view lonelandlockedandNeighbor as
select lonelandlocked.country, neighbor
	from (lonelandlocked join neighbour on lonelandlocked.country = neighbour.country);

INSERT INTO Query3 (
select z.cid as c1id, z.cname as c1name, z.neighbor as c2id, country.cname as c2name
	from (lonelandlockedandNeighbor x join country y on x.country=y.cid) z 
		join country on z.neighbor = country.cid
		order by c1name ASC
);

DROP view IF EXISTS landlocked CASCADE;
DROP view IF EXISTS lonelandlocked CASCADE;
DROP view IF EXISTS lonelandlockedandNeighbor CASCADE;

-- Query 4 statements
-- Find the accessible ocean(s) of each country. An ocean is accessible by a country if 
-- either the country itself has a coastline on that ocean (direct access to the ocean
-- ) or the country is neighboring another country that has a coastline on that ocean
-- (indirect access). Report the name of the country and the name of the accessible ocean(s).

create or replace view NeighborLand as
select cname, cid, neighbor
	from country join neighbour on  country.cid = neighbour.country
		where cid not in (
			select cid from oceanAccess
		) and
		neighbor in (
			select cid as neighbor from oceanAccess
		);

INSERT INTO Query4 (
select cname, ocean.oname
	from (NeighborLand join oceanAccess on neighbor = oceanAccess.cid) x
		join ocean on x.oid = ocean.oid 
UNION
select cname, oname
from(
(select cname, oid
	from
	((select cname, neighbor
		from ((select cid, neighbor from ((select distinct cid from oceanAccess) x 
		join neighbour on x.cid = neighbour.country) 
			where neighbor in (select cid as neighbour from oceanAccess)) info 
				join country on info.cid = country.cid )) y 
					join oceanAccess on y.neighbor = oceanAccess.cid)) z 
						join ocean on z.oid = ocean.oid)
UNION 
select cname, oname
	from (oceanAccess join country on oceanAccess.cid = country.cid) join ocean on oceanAccess.oid=ocean.oid
	 order by cname ASC, oname desc
);

DROP view IF EXISTS NeighborLand CASCADE;

-- Query 5 statements
-- Find the top-10 countries with the highest average Human Development 
-- Index (HDI) over the 5-year period of 2009-2013 (inclusive).

create or replace view hdiTop10 as
select cid, sum(hdi_score), avg(hdi_score)
	from hdi where year >= 2009 and year <= 2013 group by cid order by sum(hdi_score) DESC limit 10;

INSERT INTO Query5 (
select hdiTop10.cid, cname, hdiTop10.avg as avghdi
	from hdiTop10 join country on hdiTop10.cid = country.cid order by avghdi DESC
);

DROP view IF EXISTS hdiTop10 CASCADE;

-- Query 6 statements
-- Find the countries for which their Human Development Index (HDI) is
-- constantly increasing over the 5-year period of 2009-2013 (inclusive). 
-- Constantly increasing means that from year to year there is a positive 
-- change (increase) in the country’s HDI

create or replace view increasingNotcomplete as
select *
	from hdi x
	 where x.year >= 2009 and x.year <= 2013 and
			x.hdi_score > all(select y.hdi_score from hdi y where x.cid = y.cid and y.year < x.year);

create or replace view increasingcid as
select cid
	from increasingNotcomplete group by cid having count(cid) = 
		(select count(cid) from hdi x where increasingNotcomplete.cid = x.cid and x.year >= 2009 and x.year <= 2013);

INSERT INTO Query6 (
select increasingcid.cid, cname
	from increasingcid join country on increasingcid.cid = country.cid order by cname
);

DROP view IF EXISTS increasingNotcomplete CASCADE;
DROP view IF EXISTS increasingcid CASCADE;

-- Query 7 statements
-- Find the total number of people in the world that follow each religion. Report the
-- id of the religion, the name of the religion and the respective number of people
-- that follow it.

create or replace view religionInfo as
select religion.cid, rid, rname, (rpercentage * population) as foll
	from religion join country on religion.cid = country.cid;

create or replace view religionSum as
select rid, sum(foll)
	from religionInfo group by rid;

INSERT INTO Query7 (
select distinct religionSum.rid, rname, sum as followers
	from religionSum join religion on religionSum.rid=religion.rid order by followers DESC
);

DROP view IF EXISTS religionInfo CASCADE;
DROP view IF EXISTS religionSum CASCADE;

-- Query 8 statements
-- Find all the pairs of neighboring countries that have the same most popular language. 
-- For example,<Canada, USA, English> is one example tuple because in both countries, 
-- English is the most popular language; <Chile, Argentina, Spanish> can be another
-- tuple, and so on. Report the names of the countries and the name of the language.

create or replace view maxlanguage as
select	cid, max(lpercentage)
	from language group by cid order by cid;

create or replace view maxlanguageInfo as
select language.cid, lid, lname, lpercentage, max
	from language join maxlanguage on language.cid = maxlanguage.cid
		where max = lpercentage;

create or replace view maxlanguageInfoC as
select * from
(select country as c, neighbor as n, lid as l, lname 
	from neighbour join maxlanguageInfo on neighbour.country = maxlanguageInfo.cid) X
	where x.l = any(select lid as l from maxlanguageInfo where x.n = maxlanguageInfo.cid);

INSERT INTO Query8 (
select x.cname as c1name, country.cname as c2name, lname
	from (maxlanguageInfoC join country on maxlanguageInfoC.c = country.cid) x
		join country on x.n = country.cid order by lname ASC, c1name DESC
);

DROP view IF EXISTS maxlanguage CASCADE;
DROP view IF EXISTS maxlanguageInfo CASCADE;
DROP view IF EXISTS maxlanguageInfoC CASCADE;

-- Query 9 statements
-- Find the country with the larger difference between the country's highest elevation point
-- and the depth of its deepest ocean, among those oceans it has direct access to. 
-- If a country has no direct access to an ocean, you should consider is the depth of 
-- its deepest ocean to be 0. Report the name of the country and the total difference.

create or replace view oceanland as
select cid, cname, height
	from country where cid in (
		select cid from oceanAccess
	);

create or replace view landDiff as
select cname, (height + depth) as diff
	from (oceanAccess join oceanland on oceanAccess.cid = oceanland.cid) x
	join ocean on x.oid = ocean.oid
UNION
select cname, height as diff
	from country where cid not in (select cid from oceanland);

INSERT INTO Query9 (
select cname, diff as totalspan
	from landDiff where diff = any(select max(diff) as diff from landDiff)
);

DROP view IF EXISTS oceanland CASCADE;
DROP view IF EXISTS landDiff CASCADE;


-- Query 10 statements
-- Find the country with the longest total border length (with all its neighborin
-- g countries). Report the country and the total length of its borders.

create or replace view landInfo as
select country as cname, sum(length) as borderslength
	from neighbour group by country;

INSERT INTO Query10 (
 select country.cname, borderslength
 	from landInfo join country on landInfo.cname = country.cid where 
 		borderslength = 
 			any (select max(borderslength) as borderslength from landInfo)
 );

DROP view IF EXISTS landInfo CASCADE;