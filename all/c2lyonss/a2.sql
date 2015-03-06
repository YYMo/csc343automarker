-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

--Sample Database used for Testing

 

-- Query 1 statements
-- A view of all neighbouring country pairs
CREATE	VIEW bordering AS
SELECT c1.cname cname, c2.cname nname, c1.cid cid, c2.cid nid, c2.height
FROM	country c1, country c2, (SELECT country, neighbor FROM neighbour) n	
WHERE	c1.cid = n.country AND c2.cid = n.neighbor;

--All countries that are shorter then their neighbour
CREATE VIEW low AS
SELECT	 b1.cid, b1.cname, b1.nid, b1.nname 
FROM	bordering b1, bordering b2
WHERE b1.cid = b2.cid AND b1.height < b2.height; 

-- A more refined bordering	
CREATE VIEW borders AS
SELECT	cid, cname, nid, nname
FROM	bordering;


-- All countries that are higher then all their neighbours
CREATE VIEW highest AS
SELECT	*
FROM	borders
EXCEPT	(SELECT * FROM low)
ORDER BY cname;
	
INSERT	INTO Query1(SELECT * FROM highest);
DROP VIEW bordering CASCADE;

-- Query 2 statements
--All landlocked countries
CREATE VIEW landlocked AS
SELECT	cid, cname
FROM	country c1
Where c1.cid NOT IN (SELECT cid FROM oceanAccess)
ORDER BY cname;

INSERT	INTO Query2(SELECT * FROM landlocked);



-- Query 3 statements
--Countries with multiple borders
CREATE VIEW MultipleBorder AS
SELECT Distinct(ll.cid), ll.cname
FROM	landlocked ll, country c1, country c2, neighbour n1, neighbour n2
WHERE 	ll.cid = n1.country AND ll.cid = n2.country AND c1.cid = n1.neighbor AND c2.cid = n2.neighbor AND c1.cid<>c2.cid;

--Countries with no borders
Create View NoBorder As
SELECT Distinct(ll.cid), ll.cname
FROM landlocked ll, neighbour 
Where ll.cid NOT IN (SELECT country FROM neighbour);

--Countries with one border
Create View OneBorder AS
SELECT Distinct(ll.cid), ll.cname, country.cid cid2, country.cname cname2
FROM MultipleBorder, NoBorder, landlocked ll, neighbour, country
WHERE ll.cid NOT IN (SELECT cid FROM NoBorder) AND ll.cid NOT IN (SELECT cid FROM MultipleBorder) AND ll.cid = neighbour.country AND neighbour.neighbor = country.cid
ORDER BY ll.cname; 


INSERT	INTO Query3(SELECT * FROM OneBorder);
DROP VIEW landlocked CASCADE;

-- Query 4 statements
--All countries with a coastline
CREATE VIEW Coast AS
SELECT	cname, oname
FROM	country, ocean, oceanAccess
WHERE	country.cid = oceanAccess.cid AND ocean.oid = oceanAccess.oid;

--All countries bordering a country with a coast
Create View beside AS 
SELECT Distinct(oname), c1.cname
FROM Coast, neighbour, country c1, country c2
WHERE c1.cid = neighbour.country AND neighbour.neighbor= c2.cid AND c2.cname IN (SELECT cname FROM Coast);

--The union of beside and Coast
Create View together AS
SELECT cname, oname FROM Coast
UNION ALL
SELECT cname, oname FROM beside;

Create View Answer AS
SELECT Distinct country.cname, ocean.oname
FROM Coast, beside, country, ocean
WHERE country.cname IN (SELECT cname FROM together) AND ocean.oname IN (SELECT oname FROM together)
Order By country.cname, ocean.oname DESC;




INSERT INTO Query4(SELECT * FROM Answer);
DROP VIEW Coast Cascade;

-- Query 5 statements
--All HDI from between 2013 and 2009
Create View timePeriod AS
SELECT * 
From hdi
where hdi.year<2013 AND hdi.year>2009;

--The top ten countries with increasing HDI scores
Create View top10 AS
SELECT  country.cid, cname, AVG(timePeriod.hdi_score) avgscore
FROM timePeriod, country
WHERE country.cid = timeperiod.cid
Group BY country.cid
Order By AVG(timePeriod.hdi_score) DESC
LIMIT 10; 

INSERT INTO Query5(SELECT cid, cname, avgscore FROM top10);


-- Query 6 statements
--countries that haven't increased
Create View noIncrease AS
SELECT DISTINCT country.cid, cname
FROM timePeriod h1, timePeriod h2, country
WHERE h1.year < h2.year AND h1.hdi_score>h2.hdi_score AND country.cid = h1.cid;

Create View Answer6 AS
Select DISTINCT country.cid, country.cname
FROM country, noIncrease, timePeriod
Where country.cid NOT IN (SELECT cid FROM noIncrease) and country.cid IN (SELECT cid FROM timePeriod)
Order By country.cname;


INSERT INTO Query6 (SELECT * FROM Answer6);
DROP VIEW timePeriod CASCADE;

-- Query 7 statements
--Total number of followers per releigion
Create View followers As
SELECT religion.rid, religion.rname, SUM(religion.rpercentage*country.population)
FROM religion, country
GROUP BY religion.rid, religion.rname;

INSERT INTO Query7 (SELECT * FROM followers);

DROP VIEW followers;

-- Query 8 statements
--View of the not largest language in a country
Create View low AS
SELECT DISTINCT l1.cid, l1.lid, l1.lname, l1.lpercentage
FROM language l1, language l2
where l1.lpercentage<l2.lpercentage AND l1.cid = l2.cid;

--Most common language per country
Create View HighestL AS
SELECT *
FROM language
EXCEPT
SELECT * 
FROM low;

Create View Answer8 AS
SELECT DISTINCT c1.cname c1name, c2.cname c2name, h1.lname
FROM neighbour, HighestL h1, HighestL h2, country c1, country c2
WHERE c1.cid =neighbour.country AND c2.cid = neighbour.neighbor AND h1.cid = c1.cid AND h2.cid =c2.cid AND h1.lid = h2.lid
ORDER BY h1.lname, c1name DESC; 

INSERT INTO Query8 (SELECT * FROM Answer8);
DROP VIEW low CASCADE;

-- Query 9 statements
--Countries with ocean access
Create View Access AS
SELECT country.cid, depth
FROM country, ocean, oceanaccess
WHERE country.cid = oceanaccess.cid AND ocean.oid = oceanaccess.oid;

--Lowest point in a neihbouring ocean to a country
Create View deepest AS
SELECT country.cid did, country.cname, MIN(depth) lowest
FROM Access, country
where country.cid = Access.cid
Group BY country.cid;

--Highest point to a country+ the lowest
Create View elevation AS
SELECT deepest.cname, deepest.lowest + country.height elevate
FROM deepest, country 
WHERE deepest.did = country.cid;

--Span of all countries, including landlocked
Create View Heights AS
SELECT cname, height
FROM country
where country.cid NOT IN (SELECT cid FROM Access) 
UNION ALL
SELECT cname, elevate
FROM elevation;

--Countries that dont have the maximum height difference
Create View lowspan AS
SELECT h1.cname, h1.height
FROM Heights h1, Heights h2
WHERE h1.height < h2.height;

Create View Answer9 AS
SELECT * 
FROM Heights
EXCEPT
SELECT *
FROM lowspan;

INSERT INTO Query9 (SELECT * FROM Answer9);
DROP VIEW Access Cascade;




-- Query 10 statements
--Length of all borders
Create View borderlength AS
SELECT cname, SUM(length) blength, country.cid
FROM neighbour, country
where country.cid = neighbour.country
GROUP BY country.cname, country.cid;

--Small borders
Create View smallBorder AS
SELECT bl1.cid
FROM borderlength bl1, borderlength bl2
where bl1.blength<bl2.blength;

Create View Answer10 AS
SELECT cname, blength
FROM borderlength
where borderlength.cid NOT IN (SELECT cid FROM smallBorder);

INSERT INTO Query10 (SELECT * FROM Answer10);
DROP VIEW borderlength CASCADE;
