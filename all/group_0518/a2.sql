-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

-- half_data contains a country's neighbours and its own data
CREATE VIEW half_data (cid, cname, height, c2id) AS 
						(SELECT cid, cname, height, neighbor
						FROM neighbour JOIN country ON country = cid);

-- all_data contains a country's and its neighbour's id and name, as well as the neighbour's elevation
CREATE VIEW all_data (c1id, c1name, c2id, c2name, elevation) AS
						(SELECT a1.cid, a1.cname, a2.cid, a2.cname, a2.height
						FROM half_data a1 JOIN half_data a2 ON a1.c2id = a2.cid);

-- highest_elevation contains a country and the highest elevation one of its neighbours has
CREATE VIEW highest_elevation(c1id, elevation) AS 
						(SELECT c1id, max(elevation) FROM all_data GROUP BY c1id);

-- result contains the id and names of a country and its neighbour with the highest elevation (there might be duplicates?)
CREATE VIEW result (c1id, c1name, c2id, c2name) AS 
						(SELECT c1id, c1name, c2id, c2name
						FROM all_data NATURAL JOIN highest_elevation);

-- remove the duplicates and order by name before inserting into the query
INSERT INTO Query1 (SELECT DISTINCT * FROM result ORDER BY c1name);

-- drop all views (via cascade)
DROP VIEW half_data CASCADE;

-- Query 2 statements

-- no_access contains the cids of all countries with no ocean access (landlocked)
CREATE VIEW no_access (cid) AS (SELECT cid FROM country
								EXCEPT
								SELECT cid FROM oceanAccess);
INSERT INTO Query2 (SELECT cid, cname FROM country NATURAL JOIN no_access
					ORDER BY cname);

DROP VIEW no_access CASCADE;


-- Query 3 statements

-- no_access contains the cids of all countries with no ocean access (landlocked)
CREATE VIEW no_access (c1id) AS (SELECT cid FROM country
								EXCEPT
								SELECT cid FROM oceanAccess);

-- one_neighbour contains the cids of all countries with exactly one neighbour
CREATE VIEW one_neighbour (c1id) AS (SELECT country FROM
									(SELECT country, count(neighbor) as numneighbors
									FROM neighbour
									GROUP BY country) AS o WHERE o.numneighbors = 1);

-- half_data contains a country's neighbours and its own data
CREATE VIEW half_data (cid, cname, c2id) AS 
						(SELECT cid, cname, neighbor
						FROM neighbour JOIN country ON country = cid);

-- all_data contains a country's and its neighbour's id and name
CREATE VIEW all_data (c1id, c1name, c2id, c2name) AS
						(SELECT a1.cid, a1.cname, a2.cid, a2.cname
						FROM half_data a1 JOIN half_data a2 ON a1.c2id = a2.cid);

-- natural join no_access (c1id), one_neighbour (c1id) and all_data (c1id, c1name, c2id, c2name) for countries with
-- no ocean occess and only one neighbour from all of the data
INSERT INTO Query3 (SELECT DISTINCT * FROM no_access NATURAL JOIN one_neighbour NATURAL JOIN all_data ORDER BY c1name);

DROP VIEW no_access CASCADE;
DROP VIEW one_neighbour CASCADE;
DROP VIEW half_data CASCADE;

-- Query 4 statements

-- direct contains all the countries with direct ocean access
CREATE VIEW direct (cid, cname, oid, oname) AS
									(SELECT cid, cname, oid, oname FROM
									(SELECT cid, cname FROM country) AS c
									NATURAL JOIN oceanAccess NATURAL JOIN
									(SELECT oid, oname FROM ocean) AS o);

-- indirect contains all the countries with indirect access
CREATE VIEW indirect_without_name (cid, oid, oname) AS
									(SELECT c1id, oid, oname FROM
									(SELECT country AS c1id, neighbor AS cid FROM neighbour) AS n
									NATURAL JOIN direct);

CREATE VIEW indirect (cid, cname, oid, oname) AS
									(SELECT cid, cname, oid, oname FROM
									indirect_without_name NATURAL JOIN
									(SELECT cid, cname FROM country) AS c);

-- result is the union of direct and indirect
CREATE VIEW result (cid, cname, oid, oname) AS
									(SELECT * FROM indirect UNION SELECT * FROM direct);

INSERT INTO Query4 (SELECT cname, oname FROM result ORDER BY cname, oname DESC);

DROP VIEW direct CASCADE;

-- Query 5 statements
CREATE VIEW average_hdi (cid, avghdi) AS (SELECT cid, avg(hdi_score) FROM hdi WHERE year >= 2009 OR year <= 2013 GROUP BY cid);

INSERT INTO Query5 (SELECT cid, cname, avghdi FROM average_hdi
												NATURAL JOIN
												(SELECT cid, cname FROM country) AS c
												ORDER BY avghdi DESC
												LIMIT 10);
DROP VIEW average_hdi;

-- Query 6 statements
--Five year HDI
CREATE VIEW five_year_hdi AS (SELECT cid, hdi_score,year FROM hdi
								WHERE year > 2008 AND year < 2014);

--Find the cids that have at least one decrease and take them out of all the cids
CREATE VIEW increasingCid AS (SELECT cid FROM five_year_hdi 
								WHERE cid <> ALL( SELECT h1.cid 
							    FROM five_year_hdi h1 JOIN five_year_hdi h2 ON h1.cid = h2.cid 
							    WHERE h1.year > h2.year AND (h1.hdi_score - h2.hdi_score) <= 0));

					
--get the country id and country name where the year is 2013 which means that 					
INSERT INTO Query6 (cid, cname) (SELECT DISTINCT cid, cname
								  FROM increasingCid NATURAL JOIN country
								  ORDER BY cname ASC);

DROP VIEW five_year_hdi CASCADE;


-- Query 7 statements

-- num_followers contains the number (not percentage) of followers for each religion
-- in each country (without cid contained, so you can just find the sum)
CREATE VIEW num_followers (rid, rname, num) AS
					(SELECT rid, rname, (rpercentage)*population AS followers FROM
					(SELECT cid, population FROM country) AS c NATURAL JOIN religion);

INSERT INTO Query7 (SELECT rid, rname, sum(num) FROM num_followers
					GROUP BY rid, rname
					ORDER BY sum(num) DESC);

DROP VIEW num_followers;


-- Query 8 statements
--Find the most popular language in each country 
CREATE VIEW CLanguage AS (SELECT DISTINCT lname, cname AS c1name
							FROM (neighbour n1 JOIN language l1 ON country = cid) NATURAL JOIN country  
							WHERE lpercentage >= ALL (SELECT lpercentage
														FROM neighbour n2 JOIN language l2 ON country = cid
														WHERE n1.country = n2.country));

--Find the most popular language in each neighbor
CREATE VIEW NLanguage AS (SELECT DISTINCT lname, cname AS c2name 
							FROM (neighbour n1 JOIN language l1 ON neighbor = cid) NATURAL JOIN country
							WHERE lpercentage >= ALL (SELECT lpercentage
													   FROM neighbour n2 JOIN language l2 ON neighbor = cid
													   WHERE n1.neighbor = n2.neighbor));

--Find the countries with the same most popular language
INSERT INTO Query8 (c1name, c2name, lname) (SELECT c1name, c2name, CLanguage.lname AS lname
											 FROM CLanguage NATURAL JOIN NLanguage
											 WHERE c1name <> c2name
											 ORDER BY lname ASC, c1name DESC);
DROP VIEW CLanguage CASCADE;
DROP VIEW NLanguage CASCADE;



-- Query 9 statements

-- direct contains all the countries with direct ocean access and their totalspan (sum of its height and depth)
CREATE VIEW direct (cname, totalspan) AS
									(SELECT cname, height + depth FROM
									(SELECT cid, height, cname FROM country) AS c
									NATURAL JOIN oceanAccess NATURAL JOIN
									(SELECT oid, depth, oname FROM ocean) AS o);

-- no_ocean contains all the countries with no access to the ocean -- the totalspan is just the height
CREATE VIEW no_ocean (cname, totalspan) AS (SELECT cname, height FROM country WHERE cname NOT IN (SELECT cname FROM direct));

-- full_set is the union of direct and no_ocean
CREATE VIEW full_set (cname, totalspan) AS (SELECT * FROM direct UNION SELECT * FROM no_ocean);

-- max_span is the highest totalspan there is
CREATE VIEW max_span (totalspan) AS (SELECT max(totalspan) FROM full_set);

-- insert any country with a totalspan that matches max_span
INSERT INTO Query9 (SELECT cname, totalspan from full_set NATURAL JOIN max_span);

DROP VIEW direct CASCADE;

-- Query 10 statements
--Sum of the border lengths of each country
CREATE VIEW sum_borders AS (SELECT country, SUM(length) AS border FROM neighbour
							 GROUP BY country);

--Getting the max border length 
CREATE VIEW max_borders AS (SELECT country , border FROM sum_borders
							 WHERE border >= ALL(SELECT border from sum_borders));

--getting the cname 
INSERT INTO Query10 (cname, borderslength) (SELECT cname, border AS borderlength
											 FROM max_borders JOIN country ON country = cid);

DROP VIEW sum_borders CASCADE;

