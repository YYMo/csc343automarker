-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

	-- [5 marks] For each country, find its neighbor country with the highest elevation point. Report the id and name of the 
	-- country and the id and name of its neighboring country. 

	-- Return the following Attributes:
			-- c1id 		(country id)		 		[INTEGER]
			-- c1name		(country name) 				[VARCHAR(20)]
			-- c2id 		(neighbor country id) 		[INTEGER]
			-- c2name 		(neighbor country name)		[VARCHAR(20)]

			-- Order by: 	c1name 						ASC

CREATE VIEW countryandneighbour 
AS SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name, c2.height
	FROM (
		SELECT c1.cid as cid, MAX(c2.height) as maxHeight
		FROM neighbour AS n
			JOIN country AS c1  ON c1.cid=n.country 
			JOIN country AS c2  ON c2.cid=n.neighbor
		WHERE c1.cid <> c2.cid
		group by c1.cid) 
	AS n JOIN country as c1 on c1.cid=n.cid JOIN country as c2 on c2.height=n.maxHeight;


INSERT INTO Query1 (SELECT c1id, c1name, c2id, c2name 
					FROM countryandneighbour order by c1name ASC);

DROP VIEW countryandneighbour;

-- Query 2 statements

	-- [5 marks] Find the landlocked countries. A landlocked country is a country entirely enclosed by land (e.g., Switzerland). 
	-- Report the id(s) and name(s) of the landlocked countries.

	-- Return the following Attributes:
		-- cid 		(landlocked country id)				[INTEGER]
		-- cname 		(landlocked country name)		[VARCHAR(20)]

		-- order by: 	cname 							ASC

INSERT INTO Query2 (
	SELECT cid, cname
	FROM country
	WHERE cid NOT IN (SELECT cid FROM oceanAccess)
	order by cname ASC
);

-- Query 3 statements

	-- [5 marks] Find the landlocked countries which are surrounded by exactly one country. Report the id and name of the
	-- landlocked country, followed by the id and name of the country that surrounds it.

	-- Return the following Attributes:
		-- c1id			(landlocked country id)		[INTEGER]
		-- c1name		(landlocked country name)	[VARCHAR(20)]
		-- c2id			(surrounding country id)	[INTEGER]
		-- c2name		(surrounding country name)	[VARCHAR(20)]

		-- Order by:	c1name						ASC

CREATE VIEW llcountry AS
	SELECT cid
	FROM country
	WHERE cid NOT IN (SELECT cid FROM oceanAccess)
	order by cname;

CREATE VIEW llcountryandneighbour AS
	SELECT neighbour.country AS c1id, neighbour.neighbor AS c2id
	FROM neighbour join llcountry ON neighbour.country=llcountry.cid;

CREATE VIEW oneneighbour AS
	SELECT c1id
	FROM llcountryandneighbour	
	group by c1id
	having count(c2id)=1; 

INSERT INTO Query3 (
	SELECT llcountryandneighbour.c1id as c1id, c1.cname as c1name, llcountryandneighbour.c2id as c2id, c2.cname as c2name
	FROM llcountryandneighbour JOIN country AS c1 on llcountryandneighbour.c1id=c1.cid 
							   JOIN country AS c2 on llcountryandneighbour.c2id=c2.cid
	WHERE llcountryandneighbour.c1id IN (SELECT c1id FROM oneneighbour)
	order by c1.cname ASC
	);
DROP VIEW oneneighbour;
DROP VIEW llcountryandneighbour;
DROP VIEW llcountry;


-- Query 4 statements
	-- [5 marks] Find the accessible ocean(s) of each country. An ocean is accessible by a country if either the country itself has 
	-- a coastline on that ocean (direct access to the ocean) or the country is neighboring another country that has a coastline 
	-- on that ocean (indirect access). Report the name of the country and the name of the accessible ocean(s).

	-- Return the following Attributes:
		-- cname		(country name)		[VARCHAR(20)]
		-- oname		(ocean name)		[VARCHAR(20)]

		-- Order by:	cname 				ASC
		-- 				oname				DESC

CREATE VIEW hascoastalneighbour AS
	SELECT neighbour.country as cname, oceanAccess.oid as oname
	FROM neighbour JOIN oceanAccess ON neighbour.neighbor=oceanAccess.cid; -- find neighbours who have access to coast

CREATE VIEW directOrIndirect AS
	SELECT cname, oname FROM hascoastalneighbour
	UNION
	SELECT cid as cname, oid as oname FROM oceanAccess;

INSERT INTO Query4 ( SELECT * FROM directOrIndirect order by cname ASC, oname DESC);

DROP VIEW directOrIndirect;
DROP VIEW hascoastalneighbour;


-- Query 5 statements
	-- [5 marks] Find the top-10 countries with the highest average Human Development Index (HDI) over the	5-year period of 
	-- 2009-2013 (inclusive).

	-- Return the following Attributes:
		-- cid			(country id)				[INTEGER]
		-- cname		(country name)				[VARCHAR(20)]
		-- avghdi		(country\u2019s average HDI)		[REAL]

		-- Order by:	avghdi 						DESC

INSERT INTO Query5 (
	Select country.cid, country.cname, AVG(hdi_score) as avghdi 
	FROM hdi JOIN country ON hdi.cid=country.cid
	where year BETWEEN 2009 AND 2013
	group by country.cid
	order by avghdi DESC
	Limit 10);

-- Query 6 statements
	-- [5 marks] Find the countries for which their Human Development Index (HDI) is constantly increasing	over the 5-year period of 
	-- 2009-2013 (inclusive). Constantly increasing means that from year to year there is a positive change (increase) in 
	-- the country\u2019s HDI.

	-- Return the following Attributes:
		-- cid			(country id)		[INTEGER]
		-- cname		(country name)		[VARCHAR(20)]
		
		-- Order by:	cname				ASC

CREATE VIEW hdiInRange AS
	-- find the correct ranges (years)
	Select *
	FROM hdi 
	where year BETWEEN 2009 AND 2013;


CREATE VIEW badCountry AS
	Select c1.cid as cid
	-- find the years that the hdi did not increase and take the names of these countries
	FROM hdiInRange as c1, hdiInRange as c2
	WHERE c1.year + 1=c2.year AND c1.hdi_score >= c2.hdi_score AND c1.cid =c2.cid;

INSERT INTO Query6 (
	-- take the countries that don't *ALWAYS* have an increasing HDI and remove them from the correct years.
	SELECT country.cid as cid, country.cname as cname
	FROM hdiInRange JOIN country ON hdiInRange.cid=country.cid
	WHERE hdiInRange.cid NOT IN (SELECT cid FROM badCountry)
	group by country.cid
	order by country.cname ASC);

DROP VIEW badCountry;
DROP VIEW hdiInRange;


-- Query 7 statements
	-- [5 marks] Find the total number of people in the world that follow each religion. Report the id of the religion,
	-- the name of the religion and the respective number of people that follow it.

	-- Return the following Attributes:
		-- rid				(religionid)			[INTEGER]
		-- rname			(religionname)			[VARCHAR(20)]
		-- followers		(number of followers)	[INTEGER]
		
		-- Order by:		followers				DESC

INSERT INTO Query7 (
	SELECT religion.rid as rid, religion.rname as rname, SUM(country.population * religion.rpercentage / 100) AS followers
	FROM religion JOIN country ON religion.cid=country.cid
	group by religion.rid, religion.rname
	order by followers DESC);


-- Query 8 statements
	-- [5 marks] Find all the pairs of neighboring countries that have the same most popular language. For	example,
	-- <Canada, USA, English>	is one example	tuple because in both countries, English is the most popular language; 
	-- <Chile, Argentina, Spanish> can be another tuple, and so on. Report the names of the countries and the name 
	-- of the language.

	-- Return the following Attributes:
		-- c1name  			(country name)					[VARCHAR(20)]
		-- c2name			(neighboring country name)		[VARCHAR(20)]
		-- lname			(language name)					[VARCHAR(20)]

		-- Order by:		lname							ASC
		-- 					c1name							DESC

CREATE VIEW mostPopular AS
	SELECT l.cid, l.lid, l.lname, c.max
	From (
		SELECT cid, MAX(lpercentage) as max
		FROM language 
		group by cid) as c
	JOIN language as l on c.cid=l.cid
	WHERE c.max=l.lpercentage and c.cid=l.cid;


INSERT INTO Query8 (
	SELECT c1.cid as c1name, c2.cid as c2name, c1.lname as lname
	FROM neighbour Join mostPopular AS c1 on c1.cid=neighbour.country JOIN mostPopular AS c2 on c2.cid=neighbour.neighbor
	WHERE c1.lid=c2.lid
	order by lname ASC, c1name DESC
	);

DROP VIEW mostPopular;

-- Query 9 statements
	-- [5 marks] Find the country with the larger difference between the country's highest elevation point	and the depth of 
	-- its deepest ocean, among those oceans it has direct access to. If a country has no direct access to an ocean, you should
	-- consider is the depth of its deepest ocean to be 0. Report the name of the country and the total difference.

	-- Return the following Attributes:
		-- cname		(country name)		[VARCHAR(20)]
		-- totalspan	(total span)		[INTEGER]

CREATE VIEW landlocked AS
	SELECT cid, cname, height as totalspan
	FROM country
	WHERE cid NOT IN (SELECT cid FROM oceanAccess);

CREATE VIEW otherCountries AS
	SELECT country.cid as cid, country.cname as cname, country.height + MAX(ocean.depth) as totalspan
	FROM country JOIN oceanAccess ON country.cid=oceanAccess.cid JOIN ocean on oceanAccess.oid = ocean.oid
	group by country.cid;

INSERT INTO Query9 (
	SELECT cname, totalspan FROM landlocked
	UNION
	SELECT cname, totalspan FROM otherCountries
	order by totalspan DESC
	limit 1
	);
DROP VIEW landlocked;
DROP VIEW otherCountries;

-- Query 10 statements

	-- [5 marks] Find the country with the longest total border length (with all its neighboring countries). Report the country 
	-- and the total length of its borders.

	-- Return the following Attributes:
		-- cname				(country name)			[VARCHAR(20)]
		-- borderslength		(length of borders)		[INTEGER]

INSERT INTO Query10 (
	SELECT country as cname, SUM(length) as borderslength
	FROM neighbour
	group by country
	order by borderslength DESC
	limit 1);