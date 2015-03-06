-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
-- For each country, find its neighbor country with the highest elevation point. Report the id 
--and name of the country and the id and name of its neighboring country. 


insert into Query1 (
	select c1id, c1name, c2id, country.cname as c2name 

	from 
	(
		(
		select c1.cid as c1id, c1.cname as c1name,  c2.country as c2id,  c1.height as height
		from neighbour as c2 join country as c1  on c2.neighbor=c1.cid
		group by c2.country
		) as t1
	join
		(
		select c1.cid as c1id, c1.cname as c1name, c2.country as c2id, MAX(c1.height) as maxheight
		from neighbour as c2 join country as c1  on c2.neighbor=c1.cid
		group by c2.country
	) as t2 on t1.c1id=c2.c1id and t1.height =t2.maxheight 
	)as t3 join country on country.cid=t3.c2id


	order by c1name ASC
);
-- Query 2 statements
-- Find the landlocked countries. A landlocked country is a country entirely enclosed by land 
--(e.g., Switzerland). Report the id(s) and name(s) of the landlocked countries.


insert into Query2 (

	select cid, cname
	from country
	where not exists (select country.cid, country.cname from oceanAccess join country on oceanAccess.cid=country.cid)

	order by cname ASC
);


-- Query 3 statements
-- Find the landlocked countries which are surrounded by exactly one country. Report the id 
--and name of the landlocked country, followed by the id and name of the country that surrounds it.

insert into Query3 (

	select cname as c1name, country.cid as c2id, country.cname as c2name
	from (
	select cid, cname, neighbor
		from (
			select cid, cname, count(neighbor), neighbor
			from (select c.cid, c.cname from country as c join neighbour as n on c.cid=neighbour.country)
			where not exists(select country.cid, country.cname from oceanAccess join country on oceanAccess.cid=country.cid)
			group by cid
		) 
		where count(neighbor) = 1
	) as t1 join country on t1.neighbor=country.cid
	order by: c1name ASC
);

-- Query 4 statements
-- Find the accessible ocean(s) of each country. An ocean is accessible by a country if either the 
--country itself has a coastline on that ocean (direct access to the ocean) or the country is neighboring 
--another country that has a coastline on that ocean (indirect access). Report the name of the country 
--and the name of the accessible ocean(s).

insert into Query4 (

	select cname, oname
	from(
		select country, oname
		from(
			(select country, oid
			from neighbour as n join oceanAccess as o on oceanAccess.cid=neighbour.neighbor)
				union
			(select cid, oid
			from oceanAccess)) 
		as t join ocean on t.oid=ocean.oid
	) as t2 join country on t2.country=country.cid


order by cname ASC

);


-- Query 5 statements
-- Find the top-10 countries with the highest average Human Development Index (HDI) over the 
--5-year period of 2009-2013 (inclusive).


insert into Query5(

	select cname, avg(hdi_score) limit 10
	from 
	(select cid, avg(hdi_score)
	from hdi
	where year=2009 or year=2010 or year=2011 or year=2012 or year=2013
	group by cid) as t1 join country on country.cid=t1.cid

	order by avg(hdi_score) DESC
);


-- Query 6 statements
-- Find the countries for which their Human Development Index (HDI) is constantly increasing 
--over the 5-year period of 2009-2013 (inclusive). Constantly increasing means that from year to year 
--there is a positive change (increase) in the country’s HDI.

--[5 marks] Find the countries for which their Human Development Index (HDI) is constantly increasing 
--over the 5-year period of 2009-2013 (inclusive). Constantly increasing means that from year to year 
--there is a positive change (increase) in the country’s HDI.
--Output Table: Query6
--Attributes: cid (country id) [INTEGER]
--cname (country name) [VARCHAR(20)]
--Order by: cname ASC

--insert into Query6(


--select cid, 
--from hdi
--where year=2009 or year=2010 or year=2011 or year=2012 or year=2013

--hdi_score

--);


-- Query 7 statements
-- Find the total number of people in the world that follow each religion. Report the id of the 
--religion, the name of the religion and the respective number of people that follow it.

insert into Query7(

	select r.cid as rid, rname, rpercentage*population as followers
	from religion as r join country as c on r.cid=c.cid
	order by followers DESC
);

-- Query 8 statements
-- Find all the pairs of neighboring countries that have the same most popular language. For 
--example, <Canada, USA, English> is one example tuple because in both countries, English is the most 
--popular language; <Chile, Argentina, Spanish> can be another tuple, and so on. Report the names of 
--the countries and the name of the language.

--get max popular language for each country
--join with neighbour and join condition to c1 
--join with popular table so that c2 and c1 have the same popualr language
--join with country and replace c1id with c1name
--join wtih country and replace c2id with c2name

insert into Query8(
	select t3.c1name as c1name, c2.cname as c2name, t3.lname as lname, --c1name, c2name, lname
	from country as c2
	join (
		select t2.c2id as c2id, t2.lname as lname, c.cname as c1name --c1name, lanem, c2id
		from country as c 
		join (
			select t.cid as cid, t.lid as lid, t.lname as lname, t.neighbor as c2id --cid, lid, lname, c2id
			from 
			(select l.cid as cid, l.lid as lid, l.lname as lname           --popular languages table
					from language as l
					join(
					    select cid, max(lpercentage) as lp
					    from language
					    group by cid
					) as p on l.id = p.id and l.lpercentage = p.lp
			)as p3 
			join(
				select p2.cid as cid, p2.lid as lid, p2.lname as lname, n.neighbor as neighbor --join neighbor
				from neighbor as n
				join(
					select l.cid as cid, l.lid as lid, l.lname as lname           --popular languages table
					from language as l
					join(
					    select cid, max(lpercentage) as lp
					    from language
					    group by cid
					) as p on l.id = p.id and l.lpercentage = p.lp
				) as p2 on n.country=p2.cid
			)as t on t.neighbor=p3.cid and t.lid=p3.lid
		) as t2 on t2.cid=country.cid 
	) as t3 on t3.c2id=c2.cid
);

-- Query 9 statements
--Find the country with the larger difference between the country's highest elevation point 
--and the depth of its deepest ocean, among those oceans it has direct access to. If a country has no 
--direct access to an ocean, you should consider is the depth of its deepest ocean to be 0. Report the 
--name of the country and the total difference.


-- Query 10 statements
-- Find the country with the longest total border length (with all its neighboring countries). 
--Report the country and the total length of its borders.



insert into Query10(
	select c.cname as cname, t.borderslength as borderslength
	from country as c
	join (
		select n.country as cid, sum(n.length) as borderslength
		from neighbour as n
		group by n.country
	) as t on c.cid=t.cid
	limit 1
);