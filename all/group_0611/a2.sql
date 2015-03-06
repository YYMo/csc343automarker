-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW find_neighbors AS
    SELECT cid AS c1id, cname AS c1name,  neighbor AS c2id
    FROM country, neighbour
    WHERE cid = country;

CREATE VIEW find_neighbors_info AS
    SELECT c1id, c1name, c2id, cname AS c2name, height AS neighbor_height
    FROM find_neighbors, country
    WHERE c2id = cid;

CREATE VIEW find_tallest_neighbor AS
    SELECT c1id, MAX(neighbor_height) AS maximum
    FROM find_neighbors_info
    GROUP BY c1id;

CREATE VIEW q1_answer AS
    SELECT find_neighbors_info.c1id AS c1id, c1name, c2id, c2name
	FROM find_neighbors_info, find_tallest_neighbor
	WHERE find_neighbors_info.c1id = find_tallest_neighbor.c1id AND neighbor_height = maximum
	ORDER BY c1name ASC;

INSERT INTO Query1 (SELECT * FROM q1_answer);

DROP VIEW IF EXISTS q1_answer CASCADE;
DROP VIEW IF EXISTS find_tallest_neighbor CASCADE;
DROP VIEW IF EXISTS find_neighbors_info CASCADE;
DROP VIEW IF EXISTS find_neighbors CASCADE;



-- Query 2 statements
CREATE VIEW landlockeds AS
 	(SELECT cid 
	 FROM country)
EXCEPT
	(SELECT cid
	 FROM oceanAccess);

CREATE VIEW q2 AS
	SELECT country.cid, cname 
	FROM landlockeds, country
	WHERE landlockeds.cid = country.cid;

INSERT INTO Query2 
	SELECT *
	FROM q2
	ORDER BY cname ASC ;

DROP VIEW IF EXISTS landlockeds CASCADE;
DROP VIEW IF EXISTS q2 CASCADE;  

-- Query 3 statements
CREATE VIEW landlocked_countries AS
    (SELECT cid
    FROM country)
   	 EXCEPT
    (SELECT cid
    FROM oceanAccess);

CREATE VIEW single_neighbour AS
    SELECT cid
    	FROM country, neighbour
    	WHERE cid = country
    GROUP BY cid
    HAVING COUNT(neighbor) = 1;

CREATE VIEW landlocked_single_neighbour AS
    (SELECT cid
    FROM landlocked_countries)
   	 INTERSECT
    (SELECT  cid
    FROM single_neighbour);

CREATE VIEW find_landlocked_single_neighbour_name AS
    SELECT lsn.cid AS c1id ,cname AS c1name
    FROM landlocked_single_neighbour lsn, country
    WHERE lsn.cid = country.cid;


CREATE VIEW landlocked_single_neighbour_with_neighbour AS
    SELECT c1id, c1name, neighbor AS c2id
    FROM find_landlocked_single_neighbour_name, neighbour
    WHERE c1id = country;

CREATE VIEW q2_answer AS
    SELECT c1id, c1name, c2id, cname AS c2name
    	FROM landlocked_single_neighbour_with_neighbour, country
    WHERE c2id = cid
    ORDER BY c1name ASC;

INSERT INTO Query3 (SELECT * FROM q2_answer);

DROP VIEW IF EXISTS landlocked_countries CASCADE;
DROP VIEW IF EXISTS single_neighbour CASCADE;
DROP VIEW IF EXISTS landlocked_single_neighbour CASCADE;
DROP VIEW IF EXISTS find_landlocked_single_neighbour_name CASCADE;
DROP VIEW IF EXISTS landlocked_single_neighbour_with_neighbour CASCADE;
DROP VIEW IF EXISTS q2_answer CASCADE;




-- Query 4 statements
CREATE VIEW indirect AS
	SELECT country, oid
	FROM neighbour, oceanAccess
	WHERE neighbor = cid;

CREATE VIEW coastline AS
	(SELECT cid, oid
	 FROM oceanAccess)
			UNION
	(SELECT *
	 FROM indirect);

CREATE VIEW q4 AS
	(SELECT cname, oname
         FROM coastline, country, ocean
	 WHERE coastline.cid = country.cid and coastline.oid = ocean.oid); 

INSERT INTO Query4 
	SELECT * 
	FROM q4
	ORDER BY cname ASC, oname DESC; 
	

DROP VIEW IF EXISTS indirect CASCADE;
DROP VIEW IF EXISTS coastline CASCADE;
DROP VIEW IF EXISTS q4 CASCADE;
	 	     


-- Query 5 statements
CREATE VIEW find_correct_years AS
    SELECT *
    FROM hdi
    WHERE year >= 2009 AND year <= 2013;

CREATE VIEW query5_answer AS
    SELECT find_correct_years.cid AS cid, cname, SUM(hdi_score) / 5 AS avghdi
    FROM find_correct_years, country
    WHERE find_correct_years.cid = country.cid
    GROUP BY find_correct_years.cid, cname
    HAVING SUM(year) = 10055
    ORDER BY cname ASC
    LIMIT 10;

INSERT INTO Query5 (SELECT * FROM query5_answer);

DROP VIEW IF EXISTS find_correct_years CASCADE;
DROP VIEW IF EXISTS query5_answer CASCADE;



-- Query 6 statements
--NOTE: This query is behaving very strangely. When run automatically from the postgres shell it
--      claims that column h1.hdi_score does not exist. Even though it does not complain
--		if you define the view manually in the shell. What's more the column name exists, if you do it that way.
-- 

CREATE VIEW fiveyears AS
        SELECT h1.cid,  (h2.hdi_score - h1.hdi_score) AS diff
        FROM hdi h1, hdi h2
        WHERE h1.year = (h2.year - 5)  and h1.hdi_score < h2.hdi_score;

 CREATE VIEW fouryears AS
        SELECT h1.cid, (h2.hdi_score - h1.hdi_score) AS diff2
        FROM fiveyears h1, hdi h2
        WHERE h1.year = (h2.year - 4) and h1.hdi_score < h2.hdi_score and diff2 < h1.diff;

CREATE VIEW threeyears AS
        SELECT h1.cid, (h2.hdi_score - h1.hdi_score) AS diff3
        FROM fouryears h1, hdi h2
        WHERE h1.year = (h2.year -3) and h1.hdi_score < h2.hdi_score and diff3 < h1.diff2;

CREATE VIEW twoyears AS
        SELECT h1.cid, (h2.hdi_score - h1.hdi_score) AS diff4
        FROM threeyears h1, hdi h2
        WHERE h1.year = (h2.year -2) and h1.hdi_score < h2.hdi_score and diff4 < h1.diff3;

INSERT INTO Query6
        SELECT cid, cname
        FROM twoyears, country
        WHERE twoyears.cid = country.cid
        ORDER BY cname ASC;

DROP VIEW IF EXISTS twoyears CASCADE;
DROP VIEW IF EXISTS threeyears CASCADE;
DROP VIEW IF EXISTS fouryears CASCADE;
DROP VIEW IF EXISTS fiveyears CASCADE;
  	 	
-- Query 7 statements

CREATE VIEW find_populations AS
    SELECT rid, rname,( rpercentage / 100)  * population AS country_followers
    FROM religion, country
    WHERE religion.cid = country.cid;

CREATE VIEW query7_answer AS
    SELECT rid, rname, SUM(country_followers) AS followers
    FROM find_populations
    GROUP BY rid, rname
    ORDER BY followers DESC;

INSERT INTO Query7 (SELECT * FROM query7_answer);

DROP VIEW IF EXISTS find_populations CASCADE;
DROP VIEW IF EXISTS query7_answer CASCADE;


-- Query 8 statements
CREATE VIEW NotMost AS
    SELECT l1.cid, l1.lid
    FROM language l1, language l2
    WHERE l1.cid = l2.cid and l1.lid <> l2.lid and l1.lpercentage < l2.lpercentage;

CREATE VIEW Most AS
    (SELECT cid, lid
    FROM language)
            EXCEPT
    (SELECT *
    FROM NotMost);

CREATE VIEW langNeighbor AS
    SELECT m1.cid, m1.lid, m2.cid, m2.lid
    FROM neighbour, Most m1, Most m2
    WHERE neighbour.country = m1.cid and neighbour.neighbor = m2.cid;

CREATE VIEW same AS
    SELECT m1.cid, m2.cid, m1.lid, m2.lid
    FROM langNeighbor
    WHERE m1.cid <> m2.cid and m1.lid = m2.lid;

INSERT INTO Query8 
    SELECT c1.cname as c1name, c2.cname as c2name, lname as lname
    FROM same, country c1, country c2, language
    WHERE same.m1.cid = c1.cid and same.m2.cid = c2.cid and same.m1.lid = lid
    ORDER BY lname ASC, c1name DESC;

DROP VIEW IF EXISTS same CASCADE;
DROP VIEW IF EXISTS langNeighbor CASCADE;
DROP VIEW IF EXISTS Most CASCADE;
DROP VIEW IF EXISTS NotMost CASCADE;

-- Query 9 statements
CREATE VIEW no_ocean_access AS
    (SELECT cid
    FROM country)
   	 EXCEPT
    (SELECT cid
    FROM oceanAccess);

CREATE VIEW no_ocean_access_info AS
    SELECT country.cid, cname, height AS totalspan
    FROM country, no_ocean_access
    WHERE country.cid = no_ocean_access.cid;

CREATE VIEW ocean_access_info AS
    SELECT country.cid, cname, MAX(depth) + height AS totalspan
    FROM country, oceanAccess, ocean
    WHERE country.cid = oceanAccess.cid AND oceanAccess.oid = ocean.oid
    GROUP BY country.cid, cname, height;

CREATE VIEW largest_span_overall AS
    (SELECT cname, totalspan
    FROM no_ocean_access_info)
   		 UNION
    (SELECT cname, totalspan
    FROM ocean_access_info);

CREATE VIEW query9_answer AS
    SELECT *
    FROM largest_span_overall
    WHERE totalspan =
   	 (SELECT MAX(totalspan)
   	 FROM largest_span_overall);

INSERT INTO Query9 (SELECT * FROM query9_answer);

 DROP VIEW IF EXISTS no_ocean_access CASCADE;
 DROP VIEW IF EXISTS no_ocean_access_info CASCADE;
 DROP VIEW IF EXISTS ocean_access_info CASCADE;
 DROP VIEW IF EXISTS largest_span_overall CASCADE;
 DROP VIEW IF EXISTS query9_answer CASCADE;





-- Query 10 statements

CREATE VIEW total AS
    SELECT country, SUM(length) AS len
    FROM neighbour 
    GROUP BY country;
      
CREATE VIEW notMAX AS
    SELECT t1.country, t1.len
    FROM total t1, total t2
    WHERE t1.country <> t2.country and t1.len < t2.len;
     
CREATE VIEW maximum AS
    (SELECT *
    FROM total) EXCEPT (
    SELECT *
    FROM  notMAX);
    
INSERT INTO Query10
    SELECT country AS cname, len AS borderslength 
    FROM maximum, country
    WHERE country=cid;

DROP VIEW IF EXISTS maximum CASCADE;
DROP VIEW IF EXISTS notMAX CASCADE;
DROP VIEW IF EXISTS total CASCADE;


