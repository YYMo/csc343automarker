-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.


--	LAWRENCE TEMPLE 
--	ASSIGNMENT 2 
--	CSC343H - PROF DIANE HORTON
--	NOVEMBER 10 2014
--	THIS IS SOME RUDIMENTARY SQL

  SET search_path TO A2;
  
  
-- Query 1 statements
  --Create a vtable with the largest neighbor country
  CREATE VIEW maxHeightCountries AS
    --Find ALL neighbor combinations
    (SELECT n1.country as c, n1.neighbor as n
    FROM neighbour n1)
    EXCEPT
    -- EXCEPT those where the neighbor height is shorter than some other neighbors height
    (SELECT n1.country as c, n1.neighbor as n
    FROM neighbour n1 	JOIN neighbour n2 ON (n1.country=n2.country) 
			JOIN country c1 ON (c1.cid = n1.country)
			JOIN country c2 ON (c2.cid = n2.country)
    WHERE c1.height<c2.height);
    
  
  --Parse Skeleton Data for Output
  INSERT INTO Query1(
  SELECT co.cid as c1id, co.cname as c1name, nc.cid as c2id, nc.cname as c2name
  FROM maxHeightCountries mh JOIN country co on (mh.c = co.cid) join country nc on (mh.n = nc.cid)
  ORDER BY c1name ASC
  );
  
  DROP VIEW maxHeightCountries;
  
  
-- Query 2 statements
  --Create a table of cids without oceanaccess
  CREATE VIEW countriesThatIWant AS
    (SELECT cid 
    FROM country)
    EXCEPT
    (SELECT cid
    FROM oceanAccess);

  --Parse Skeleton Data for Output
  INSERT INTO Query2(
    SELECT c.cid as cid, c.cname as cname
    FROM countriesThatIWant ctiw join country c on (ctiw.cid = c.cid)
    ORDER BY cname ASC
  );
  
  DROP VIEW countriesThatIWant;

  
-- Query 3 statements
  --Creat a vtable of all landlocked countries
  CREATE VIEW landLocked AS
    (SELECT cid 
    FROM country)
    EXCEPT
    (SELECT cid
    FROM oceanAccess);

  --Create a vtable of all landlocked countries with only 1 neighbor
  CREATE VIEW countryWithOneNeighbor AS
    --all landlocked countries
    (SELECT cid
    FROM landLocked)
    EXCEPT
    --Minus the countries w/ 2 or more (diff) neighbours
    (SELECT nb1.country as cid
    FROM neighbour nb1 JOIN neighbour nb2 on (nb1.country = nb2.country)
    WHERE (nb1.neighbor != nb2.neighbor)
    );
  
  --Parse Skeleton Data for Output
  INSERT INTO Query3(
    SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name
    FROM countryWithOneNeighbor c1n JOIN neighbour nc ON (c1n.cid = nc.country)
				     JOIN country c1 ON (c1.cid = nc.country)
				     JOIN country c2 ON (c2.cid = nc.neighbor)
    ORDER BY c1name ASC
  );
  
  DROP VIEW countryWithOneNeighbor;
  DROP VIEW landLocked;
  
  
-- Query 4 statements
  --Create vtable of all countries with some sort fo ocean access
  CREATE VIEW OAccess AS
    -- Direct Access
    (SELECT c.cid as cid, o.oid as oid
    FROM country c JOIN oceanAccess o ON (c.cid = o.cid))
    UNION
    -- Indirect Access
    (SELECT n.country as cid, o.oid as oid
    FROM neighbour n JOIN oceanAccess o ON (n.neighbor = o.cid));
    
  --Parse Skeleton Data for Output
  INSERT INTO Query4(
    SELECT c.cname as cname, o.oname as oname
    FROM OAccess oa JOIN country c ON (oa.cid = c.cid)
		    JOIN ocean o on (oa.oid = o.oid)
    ORDER BY cname ASC, oname DESC
  );
  
  DROP VIEW OAccess;


-- Query 5 statements
  --Find an avarage of all HDI scores between 2009 and 2013
  CREATE VIEW averageHDIs AS
    SELECT AVG(hdi_score) as avgHDI, cid
    FROM hdi
    WHERE (year >= 2009 AND year <= 2013)
    GROUP BY (cid);
    
  --Parse skeleton data for output, also set limit
  INSERT INTO Query5(
    SELECT c.cid as cid, c.cname as cname, a.avgHDI as avghdi
    FROM averageHDIs a JOIN country c ON (a.cid = c.cid)
    ORDER BY avgHDI DESC
    LIMIT 10
  );
   
  DROP VIEW averageHDIs;


-- Query 6 statements
  --create a vtable with all HDI scores between 2009 and 2013
  CREATE VIEW fiveYrHDI AS
    SELECT * 
    FROM hdi
    WHERE (hdi.year>= 2009 AND hdi.year<=2013);

  --create a vtable of all countries with an increasing HDI
  CREATE VIEW incrHDI AS
    --all countries with 2 vals in the 2009-2013 range
    (SELECT h1.cid as cid
    FROM fiveYrHDI h1 JOIN fiveYrHDI h2 ON (h1.cid = h2.cid AND h1.year != h2.year))
    EXCEPT
    --minus the ones with some sort of decresaing hdi_score
    (SELECT h1.cid as cid
    FROM fiveYrHDI h1 JOIN fiveYrHDI h2 ON (h1.cid = h2.cid AND h1.year != h2.year)
    WHERE (h1.year < h2.year AND h1.hdi_score >= h2.hdi_score));
  
  --Parse skeleton for correct answers
  INSERT INTO Query6(
    SELECT c.cid as cid, c.cname as cname
    FROM incrHDI h JOIN country c ON (h.cid = c.cid)
    ORDER BY cname ASC
  );
  
  DROP VIEW incrHDI;
  DROP VIEW fiveYrHDI;
    
    
-- Query 7 statements
  INSERT INTO Query7( 
    SELECT r.rid as rid, r.rname as rname, SUM(r.rpercentage*c.population) as followers
    FROM religion r JOIN country c ON (r.cid = c.cid)
    GROUP BY rid, rname
    ORDER BY followers DESC
   );

   
-- Query 8 statements
  --Create a vtable of top languages in each country
  CREATE VIEW topLang AS
    -- Find all country/language combos
    (SELECT cid, lid, lname
    FROM language)
    EXCEPT
    -- Remove those with less than the top percentage
    (SELECT c.cid as cid, l1.lid as lid , l1.lname
    FROM country c JOIN language l1 ON (c.cid = l1.cid)
		    JOIN language l2 ON (c.cid = l2.cid)
    WHERE (l1.lpercentage < l2.lpercentage));
    
  --Create a vtable of those neighboring countries that share a top language  
  CREATE VIEW neighPop AS
    SELECT n.country as cid, n.neighbor as nid, tl1.lid as lid, tl1.lname as lname
    FROM neighbour n JOIN topLang tl1 ON (n.country = tl1.cid)
		      JOIN topLang tl2 ON (n.neighbor = tl2.cid)
    WHERE (tl1.lid = tl2.lid);
  
  --Parse Skeleton Data for Output
  INSERT INTO Query8(
    SELECT c1.cname as c1name, c2.cname as c2name, np.lname as lname 
    FROM neighPop np JOIN country c1 ON (np.cid = c1.cid)
		      JOIN country c2 ON (np.nid = c2.cid)
    ORDER BY lname ASC, c1name DESC
  );
  
  DROP VIEW neighPop;
  DROP VIEW topLang;
  
  
-- Query 9 statements  
  --Create a vtable with landlocked countries 
  CREATE VIEW landLocked AS
    (SELECT cid
    FROM country)
    EXCEPT
    (SELECT cid
    FROM oceanAccess);
  
  --Create a vtable with the deepest ocean (all oceans - shallower oceans)
  CREATE VIEW deepestOcean AS
    --Select all ocean/country combos
    (SELECT cid, oid
    FROM oceanAccess)
    EXCEPT
    --Select the oceans that are NOT AS DEEP AS SOME OTHER OCEAN
    (SELECT oa1.cid as cid, o1.oid as oid
    FROM oceanAccess oa1 JOIN oceanAccess oa2 ON (oa1.cid=oa2.cid)
			 JOIN ocean o1 ON (o1.oid=oa1.oid)
			 JOIN ocean o2 ON (o2.oid=oa2.oid)
    WHERE (o1.depth < o2.depth));
    
  --Insert into Q9 the sum of the deepest oceans and heigh of countries with an ocean
  --Unioned with the height of those poor countries without an ocean 
  INSERT INTO Query9(
    (SELECT c.cname as cname, (o.depth+c.height) as totalspan
    FROM country c JOIN deepestOcean dpo on (c.cid=dpo.cid)
		    JOIN ocean o on (o.oid = dpo.oid))
    UNION
    (SELECT c.cname as cname, c.height as totalspan
    FROM landLocked ll JOIN country c ON (ll.cid = c.cid))
  );
  
  DROP VIEW deepestOcean;
  DROP VIEW landLocked;

    
-- Query 10 statements

  --Insert into Query10 the sum of the border lengths for each country.  
  INSERT INTO Query10(
    SELECT c.cname as cname, SUM(length) as borderslength
    FROM neighbour n JOIN country c ON (n.country = c.cid)
    GROUP BY (cname)
  );