-- The following tables will be used to store the results of your queries. 
-- Each of them should be populated by your last SQL statement that looks like:
-- "INSERT INTO QueryX (SELECT ...<complete your SQL query here> ... )"

-- -- -- -- -- -- -- -- -- -- -- QUERY 1
CREATE VIEW countriesAndNeighboursHeights as 
(SELECT country as c1id, height as c2height
FROM neighbour natural join 
	(SELECT cid as neighbor, height 
	 FROM country) as neighbourHeight);

CREATE VIEW maxNeighbourHeight as      
SELECT c1id, MAX(c2height) as c2maxheight			
FROM countriesAndNeighboursHeights			
GROUP BY c1id;

CREATE VIEW neighbours as 
SELECT country as c1id, neighbor as c2id 								          
FROM neighbour;

CREATE VIEW countries as
SELECT cid as c2id, cname as c2name,height as c2maxheight 
FROM country;

CREATE VIEW neighbourIDNAME as			
SELECT c1id , c2id, c2name			
FROM maxNeighbourHeight natural join 
neighbours natural join countries;

INSERT INTO Query1 (
SELECT c1id, c1name, c2id, c2name			
FROM  neighbourIDNAME natural join 
	(SELECT cid as c1id, cname as c1name
	 FROM country)	as countries		
ORDER BY c1name ASC);

DROP VIEW countriesAndNeighboursHeights CASCADE;


-- -- -- -- -- -- -- -- -- -- -- QUERY 2
CREATE VIEW noAccess AS
(SELECT cid FROM country) EXCEPT
(SELECT cid FROM oceanAccess);
 
INSERT INTO Query2 (
    SELECT cid, cname  
    FROM noAccess natural join country
    ORDER BY cname ASC); 
    
DROP VIEW noAccess CASCADE;

-- -- -- -- -- -- -- -- -- -- -- QUERY 3
CREATE TABLE Query3(
	c1id	INTEGER,
    c1name	VARCHAR(20),
	c2id	INTEGER,
    c2name	VARCHAR(20)
);

CREATE VIEW landlocked AS 
(SELECT cid FROM country) EXCEPT
(SELECT cid FROM oceanAccess);
  
CREATE VIEW landlockedNeighbours AS 
SELECT * FROM neighbour
WHERE country IN 
(SELECT * FROM landlocked);

CREATE VIEW numNeighbours AS
SELECT country as cid, COUNT(neighbor) as num
FROM landlockedNeighbours
GROUP BY cid;
 
CREATE VIEW oneNeighbour AS
SELECT cid, cname as c1name
FROM numNeighbours natural join country
WHERE num = 1;
 
CREATE VIEW getNeighbour AS 
SELECT cid as c1id, c1name, neighbor as c2id
FROM oneNeighbour natural join 
(SELECT country as cid, neighbor 
FROM neighbour) as neighbours ;  
 
INSERT INTO Query3(
SELECT c1id, c1name, c2id, cname as c2name
FROM getNeighbour natural join 
(SELECT cid as c2id, cname
FROM country) countries
ORDER BY c1name ASC);

DROP VIEW landlocked CASCADE;

-- -- -- -- -- -- -- -- -- -- -- QUERY 4
CREATE VIEW directAccesible AS  
    SELECT cid, oid
    FROM oceanAccess;
 
CREATE VIEW landlocked AS
    SELECT cid
    FROM (SELECT cid FROM country) as withAccess EXCEPT (SELECT cid FROM oceanAccess);
 
CREATE VIEW getLandlockedNeighbour AS
    SELECT cid, neighbor
    FROM landlocked, neighbour
    WHERE cid = country;
    
CREATE VIEW isNeighbourOceanAccesible AS  
    SELECT getLandlockedNeighbour.cid as hasNeighbor, oid
    FROM getLandlockedNeighbour, directAccesible
    WHERE directAccesible.cid = getLandlockedNeighbour.neighbor;
 
CREATE VIEW getAll AS  
    SELECT cid, oid
    FROM (SELECT * FROM directAccesible) as D UNION (SELECT * FROM isNeighbourOceanAccesible);
 
INSERT INTO Query4 (
    SELECT cname, oname
    FROM getAll natural join country natural join ocean
    ORDER BY cname ASC, oname DESC
);

DROP VIEW directAccesible CASCADE;


-- -- -- -- -- -- -- -- -- -- -- QUERY 5
CREATE VIEW hdi2009To2013 as 
SELECT cid, AVG(hdi_score)	as avghdi		
FROM hdi	
WHERE year <= 2013 AND year >= 2009			
GROUP BY cid					
ORDER BY avghdi DESC			
LIMIT 10;

INSERT INTO Query5 (
SELECT cid, cname, avghdi
FROM hdi2009To2013 natural join country
ORDER BY avghdi DESC); 

DROP VIEW hdi2009To2013 CASCADE;

-- -- -- -- -- -- -- -- -- -- -- QUERY 6
CREATE VIEW FIRST_YEAR AS 
SELECT cid, hdi_score as hdi_2009 FROM hdi WHERE year = 2009;

CREATE VIEW SECOND_YEAR AS 
SELECT cid, hdi_score as hdi_2010 FROM hdi WHERE year = 2010;

CREATE VIEW THIRD_YEAR AS 
SELECT cid, hdi_score as hdi_2011 FROM hdi WHERE year = 2011;

CREATE VIEW FOURTH_YEAR AS 
SELECT cid, hdi_score as hdi_2012 FROM hdi WHERE year = 2012;

CREATE VIEW FIFTH_YEAR AS 
SELECT cid, hdi_score as hdi_2013 FROM hdi WHERE year = 2013;

CREATE VIEW allHDI AS
SELECT * FROM FIRST_YEAR natural join 
SECOND_YEAR natural join 
THIRD_YEAR natural join
FOURTH_YEAR natural join 
FIFTH_YEAR;

CREATE VIEW increasingHDI AS 
SELECT cid 
FROM allHDI 
WHERE hdi_2013 > hdi_2012 AND 
hdi_2012 > hdi_2011 AND 
hdi_2011 > hdi_2010 AND 
hdi_2010 > hdi_2009;

INSERT INTO Query6(
SELECT cid, cname 
FROM increasingHDI natural join country 
ORDER BY cname ASC);

DROP VIEW FIRST_YEAR CASCADE; 

-- -- -- -- -- -- -- -- -- -- -- QUERY 7
CREATE VIEW rFollowers as	 
SELECT rid, rpercentage * 
	(SELECT population 						          
	 FROM country  						          
	 WHERE country.cid = religion.cid) as followers			
FROM religion;

CREATE VIEW totalFollowers as 		
SELECT rid, SUM(followers) as followers			
From rFollowers			
GROUP BY rid; 

INSERT INTO Query7(
SELECT rid, rname, followers
FROM totalFollowers natural join religion 
GROUP BY rid, rname, followers
ORDER BY followers DESC);

DROP VIEW rFollowers CASCADE;

-- -- -- -- -- -- -- -- -- -- -- QUERY 8
CREATE VIEW getMaxPercent AS 
	SELECT cid, max(lpercentage) as percent
	FROM language
	GROUP BY cid;

CREATE VIEW getLangName AS 
	SELECT getMaxPercent.cid, lname
	FROM getMaxPercent, language
	WHERE getMaxPercent.cid = language.cid AND getMaxPercent.percent = language.lpercentage;

CREATE VIEW getPair AS 
	SELECT g1.cid as c1id, g2.cid as c2id, g1.lname as l1name, g2.lname as l2name
	FROM getLangName g1, getLangName g2
	WHERE g1.cid <> g2.cid AND g1.lname = g2.lname;
	
CREATE VIEW getFirstName AS
	SELECT getPair.c1id, cname as c1name, c2id, l1name
	FROM getPair natural join country
	WHERE getPair.c1id = cid;

CREATE VIEW getSecondName AS
	SELECT getFirstName.c1name as c1name, country.cname as c2name, l1name
	FROM getFirstName natural join country
	WHERE getFirstName.c2id = country.cid;

INSERT INTO Query8 (
	SELECT c1name, c2name, l1name as lname 
	FROM getSecondName
	ORDER BY lname ASC, c1name DESC
);

DROP VIEW getMaxPercent CASCADE;

-- -- -- -- -- -- -- -- -- -- -- QUERY 9
CREATE VIEW countriesHeightDepth as 	 	
SELECT cname, height, depth			
FROM country NATURAL JOIN oceanAccess
NATURAL JOIN ocean;

CREATE VIEW heightDeepestDepth as 
SELECT cname, height, MAX(depth) as deepest_depth
FROM countriesHeightDepth
GROUP BY cname, height;

CREATE VIEW totalSpanDifference as 
SELECT cname, MAX(totalspan) as totalspan
FROM (SELECT cname, ABS(height - deepest_depth) as totalspan
FROM heightDeepestDepth) as heightDepthDifference
GROUP BY cname;

INSERT INTO Query9(
SELECT * FROM totalSpanDifference
WHERE totalspan >= ALL 
(SELECT totalspan FROM totalSpanDifference as totalspans));

DROP VIEW countriesHeightDepth CASCADE;

-- -- -- -- -- -- -- -- -- -- -- QUERY 10
CREATE VIEW countriesBordersLength as 
SELECT country as cid, SUM(length) as borderslength
FROM neighbour
GROUP BY country; 

CREATE VIEW maxBorderLength as 
SELECT cid, borderslength
FROM countriesBordersLength 
WHERE borderslength >= ALL 
(SELECT borderslength FROM countriesBordersLength);

INSERT INTO Query10 (
SELECT cname, borderslength
FROM maxBorderLength NATURAL JOIN country);

DROP VIEW countriesBordersLength CASCADE;
