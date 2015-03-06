-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
   CREATE VIEW maxheight as
   SELECT max(height) as height, neighbour.country as c1id
   FROM country, neighbour
   WHERE cid = neighbor
   GROUP BY neighbour.country;

   CREATE VIEW allexceptcountryname as
   SELECT c1id, cid as c2id, cname as c2name
   FROM maxheight, (neighbour join country on neighbour.neighbor = country.cid) n1
   WHERE maxheight.height = n1.height and maxheight.c1id = n1.country;

   INSERT INTO Query1(SELECT c1id, cname as c1name, c2id, c2name
   FROM (allexceptcountryname join country ON allexceptcountryname.c1id = country.cid)
   ORDER BY c1name);


   DROP VIEW allexceptcountryname;
   DROP VIEW maxheight;


-- Query 2 statements

   INSERT INTO Query2(SELECT cid, cname
   FROM country
   WHERE cid NOT IN (
   	SELECT cid
   	FROM oceanAccess
   	)
   ORDER BY cname);

-- Query 3 statements
   
   CREATE VIEW landlock as
   SELECT cid as landcid, cname as landcname
   FROM country
   WHERE cid NOT IN (
   	SELECT cid 
   	FROM oceanAccess
   	)
   ORDER BY cname;

   CREATE VIEW oneNeighbour as
   (
   	SELECT country, neighbor
   	FROM neighbour
   ) 
   EXCEPT 
   (
   SELECT N1.country as country, N1.neighbor as neighbor
   FROM neighbour N1, neighbour N2
   WHERE N1.country = N2.country and N1.neighbor != N2.neighbor
   );

   INSERT INTO Query3(SELECT country as c1id, landcname as c1name, neighbor as c2id, cname as c2name
   FROM oneNeighbour, landlock, country
   WHERE country = landcid and neighbor = cid
   ORDER BY c1name);

   DROP VIEW landlock;
   DROP VIEW oneNeighbour;



-- Query 4 statements

   CREATE VIEW indirectAccess as
   SELECT country as cid, oid
   FROM oceanAccess, neighbour
   where cid = neighbor;
   
   CREATE VIEW allAccess as
   (SELECT cid, oid 
   	FROM oceanAccess
   	   UNION 
   	SELECT cid, oid 
   	FROM indirectAccess);

   INSERT INTO Query4(SELECT cname, oname
   FROM ocean, country, allAccess
   WHERE allAccess.cid = country.cid and ocean.oid = allAccess.oid
   ORDER BY cname, oname DESC);

   DROP VIEW allAccess;
   DROP VIEW indirectAccess;
   

   

-- Query 5 statements

   CREATE VIEW averageHDI as
   SELECT cid as avgCid, AVG(hdi_score) as avghdi
   FROM hdi
   WHERE year <= 2013 and year >= 2009
   GROUP BY cid;

   INSERT INTO Query5(SELECT cid, cname, avghdi
   FROM averageHDI, country
   WHERE cid = avgCid
   ORDER BY avghdi DESC LIMIT 10);

   DROP VIEW averageHDI;


-- Query 6 statements

   CREATE VIEW hdi2009 as
   SELECT cid, hdi_score
   FROM hdi
   WHERE year = 2009;
   
   CREATE VIEW hdi2010 as
   SELECT cid, hdi_score
   FROM hdi
   WHERE year = 2010;
   
   CREATE VIEW hdi2011 as
   SELECT cid, hdi_score
   FROM hdi
   WHERE year = 2011;

   CREATE VIEW hdi2012 as
   SELECT cid, hdi_score
   FROM hdi
   WHERE year = 2012;

   CREATE VIEW hdi2013 as
   SELECT cid, hdi_score
   FROM hdi
   WHERE year = 2013;
   
   INSERT INTO Query6(SELECT cid, cname
   FROM country, (SELECT hdi2009.cid as increasingCid
   	FROM hdi2009, hdi2010, hdi2011, hdi2012, hdi2013
   	WHERE hdi2009.hdi_score < hdi2010.hdi_score and hdi2009.cid = hdi2010.cid
   	and hdi2010.hdi_score < hdi2011.hdi_score and hdi2010.cid = hdi2011.cid
   	and hdi2011.hdi_score < hdi2012.hdi_score and hdi2011.cid = hdi2012.cid
   	and hdi2012.hdi_score < hdi2013.hdi_score and hdi2012.cid = hdi2013.cid
   	GROUP BY hdi2009.cid) as HDIgrowth
   WHERE country.cid = increasingCid
   ORDER BY cname);

   DROP VIEW hdi2009;
   DROP VIEW hdi2010;
   DROP VIEW hdi2011;
   DROP VIEW hdi2012;
   DROP VIEW hdi2013;
   


-- Query 7 statements

   INSERT INTO Query7(SELECT rid, rname, SUM(rpercentage * population) as followers
   FROM country, religion
   WHERE country.cid = religion.cid
   GROUP BY rid, rname
   ORDER BY followers DESC);


-- Query 8 statements

   CREATE VIEW mostSpokenLanguage as
   SELECT country.cid as cid, country.cname as cname, lid, lname, MAX(lpercentage * population) as numofpeople
   FROM country, language 
   WHERE country.cid = language.cid
   GROUP BY country.cid, cname, lid, lname;

   
   INSERT INTO Query8(SELECT s1.cname as c1name, s2.cname as c2name, s1.lname
   FROM neighbour, mostSpokenLanguage s1, mostSpokenLanguage s2
   WHERE neighbor = s1.cid and country = s2.cid and s1.lname = s2.lname
   ORDER BY s1.lname, c1name DESC);
   
   DROP VIEW mostSpokenLanguage;


-- Query 9 statements

	CREATE VIEW oceandepth as
	SELECT cid, depth
	FROM ocean, oceanAccess
	WHERE ocean.oid = oceanAccess.oid;

	CREATE VIEW allcountries as
	SELECT cid
	FROM country;

	CREATE VIEW nooceans as
	SELECT cid, 0 as depth
	FROM (SELECT cid FROM allcountries
	EXCEPT SELECT cid FROM oceandepth) as oceahdepthcountries;

	INSERT INTO Query9(SELECT cname, MAX(height + depth) as totalspan
	FROM ((SELECT * FROM nooceans) UNION (SELECT * FROM oceandepth)) alldepth, country
	WHERE country.cid =  alldepth.cid
	GROUP BY cname
	ORDER BY totalspan DESC LIMIT 1);

	DROP VIEW nooceans;
	DROP VIEW allcountries;
	DROP VIEW oceandepth;
	
-- Query 10 statements

	CREATE VIEW lengths as
	SELECT country, SUM(length) as borderslength
	FROM neighbour
	GROUP BY country;

	INSERT INTO Query10(
		SELECT cname, borderslength
		FROM lengths, country
		WHERE country = cid
		ORDER BY borderslength DESC LIMIT 1);
		
	DROP VIEW lengths;

