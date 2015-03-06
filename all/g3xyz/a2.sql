-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW neighbour_height AS --table with neighbour's height
SELECT n.country AS cid, n.neighbor AS c2id, cname AS c2name,height AS nheight
FROM neighbour n, country c
WHERE n.neighbor=c.cid;


CREATE VIEW highest_neighbour AS --neighbour with highest elevation
(SELECT * FROM neighbour_height)
	EXCEPT
(SELECT t1.cid, t1.c2id, t1.c2name, t1.nheight
FROM neighbour_height t1, neighbour_height t2
WHERE t1.cid=t2.cid AND t1.c2id!=t2.cid AND t1.nheight<t2.nheight);
	
INSERT INTO Query1(SELECT cid, cname, c2id, c2name
			FROM highest_neighbour NATURAL JOIN country
			ORDER BY cname ASC );

DROP VIEW IF EXISTS neighbour_height CASCADE;
DROP VIEW IF EXISTS highest_neighbour CASCADE;

-- Query 2 statements

CREATE VIEW landlocked_countries AS --allcountries - oceanaccess countries
(SELECT cid FROM country)
	EXCEPT
(SELECT cid FROM oceanAccess);

INSERT INTO Query2(SELECT cid, cname 
			FROM landlocked_countries NATURAL JOIN country
			ORDER BY cname ASC);

DROP VIEW IF EXISTS landlocked_countries CASCADE;

-- Query 3 statements

CREATE VIEW neighbour_count AS --country with their respective neighbour count
SELECT country,COUNT(neighbor) AS ncount
FROM neighbour
GROUP BY country;

CREATE VIEW one_neighbour AS -- country with one neighbour
SELECT country AS cid, neighbor as c2id
FROM neighbour_count NATURAL JOIN neighbour
WHERE ncount=1;

CREATE VIEW landlocked_one_neighbour AS -- getting the landlocked country
SELECT cid, c2id
FROM one_neighbour
WHERE NOT EXISTS 
	(SELECT *
		FROM oceanAccess
		WHERE oceanAccess.cid=one_neighbour.cid);

CREATE VIEW only_country_name AS -- one neighbour with country name
SELECT cid AS c1id, cname AS c1name, c2id AS cid
FROM landlocked_one_neighbour t1 NATURAL JOIN country c;

CREATE VIEW both_names AS -- one neighbour with both country and neighbour name
SELECT c1id, c1name, cid AS c2id, cname AS c2name
FROM only_country_name t1 NATURAL JOIN country c;

INSERT INTO Query3(SELECT *
			FROM both_names
			ORDER BY c1name ASC);

DROP VIEW IF EXISTS neighbour_count CASCADE;

DROP VIEW IF EXISTS one_neighbour CASCADE;

DROP VIEW IF EXISTS landlocked_one_neighbour CASCADE;
			
DROP VIEW IF EXISTS only_country_name CASCADE;

DROP VIEW IF EXISTS both_names CASCADE;

-- Query 4 statements

CREATE VIEW indirect_access_id AS
SELECT t2.country AS cid, t1.oid
FROM oceanAccess t1, neighbour t2
WHERE t1.cid=t2.neighbor;


CREATE VIEW both_accesses AS --both direct and indirect ocean access
(SELECT *
FROM indirect_access_id)
	UNION
(SELECT *
FROM oceanAccess);

INSERT INTO Query4(SELECT cname,oname
			FROM both_accesses NATURAL JOIN ocean NATURAL JOIN country
			ORDER BY cname ASC, oname DESC);

DROP VIEW IF EXISTS indirect_access_id  CASCADE;

DROP VIEW IF EXISTS both_accesses CASCADE;

-- Query 5 statements

CREATE VIEW between_2009_2013_hdi AS 
(SELECT cid,hdi_score FROM hdi WHERE year>=2009)
	EXCEPT ALL
(SELECT cid,hdi_score FROM hdi WHERE year>2013);

CREATE VIEW highest_avg_hdi AS
SELECT cid, AVG(hdi_score) AS avghdi
FROM between_2009_2013_hdi
GROUP BY cid LIMIT 10;

INSERT INTO Query5(SELECT cid,cname,avghdi
			FROM highest_avg_hdi NATURAL JOIN country
			ORDER BY avghdi DESC);

DROP VIEW IF EXISTS between_2009_2013_hdi CASCADE;

DROP VIEW IF EXISTS highest_avg_hdi CASCADE;

-- Query 6 statements

CREATE VIEW increasing_2009 AS
SELECT *
FROM hdi
WHERE year=2009;

CREATE VIEW increasing_2010 AS
SELECT t1.cid, t1.hdi_score
FROM hdi t1, increasing_2009 t2
WHERE t1.cid=t2.cid AND t1.year=2010 AND t1.hdi_score>t2.hdi_score;

CREATE VIEW increasing_2011 AS
SELECT t1.cid, t1.hdi_score
FROM hdi t1, increasing_2010 t2
WHERE t1.cid=t2.cid AND t1.year=2011 AND t1.hdi_score>t2.hdi_score;

CREATE VIEW increasing_2012 AS
SELECT t1.cid, t1.hdi_score
FROM hdi t1, increasing_2011 t2
WHERE t1.cid=t2.cid AND t1.year=2012 AND t1.hdi_score>t2.hdi_score;

CREATE VIEW increasing_2013 AS
SELECT t1.cid, t1.hdi_score
FROM hdi t1, increasing_2012 t2
WHERE t1.cid=t2.cid AND t1.year=2013 AND t1.hdi_score>t2.hdi_score;

INSERT INTO Query6(SELECT cid, cname 
			FROM increasing_2013 NATURAL JOIN country
			ORDER BY cname ASC);

DROP VIEW IF EXISTS increasing_2009 CASCADE;
DROP VIEW IF EXISTS increasing_2010 CASCADE;
DROP VIEW IF EXISTS increasing_2011 CASCADE;
DROP VIEW IF EXISTS increasing_2012 CASCADE;
DROP VIEW IF EXISTS increasing_2013 CASCADE;

-- Query 7 statements

CREATE VIEW country_rpopulation AS
SELECT cid, rid, rname, population*rpercentage AS followers
FROM religion NATURAL JOIN country;

CREATE VIEW total_rfollowers AS
SELECT DISTINCT rid, rname, SUM(followers) AS followers
FROM country_rpopulation
GROUP BY rid,rname;

INSERT INTO Query7(SELECT * 
			FROM total_rfollowers
			ORDER BY followers DESC);

DROP VIEW IF EXISTS country_rpopulation CASCADE;

DROP VIEW IF EXISTS total_rfollowers CASCADE;

-- Query 8 statements

CREATE VIEW country_most_lp AS -- % of most popular language of each country 
SELECT cid, cname, MAX(lpercentage) AS lpercentage
FROM language NATURAL JOIN country
GROUP BY cid, cname;

CREATE VIEW country_most_popularl AS --most popular language of each country
SELECT l.cid AS country,cname, lname
FROM country_most_lp t1, language l
WHERE t1.cid=l.cid AND t1.lpercentage=l.lpercentage;

CREATE VIEW most_popular_neighbourl AS --most popularl of country with neighbour
SELECT country, neighbor, cname, lname
FROM neighbour NATURAL JOIN country_most_popularl;

CREATE VIEW pairs_with_samePL AS
SELECT t1.cname AS c1name, t1.neighbor AS cid, t1.lname
FROM most_popular_neighbourl t1, country_most_popularl t2
WHERE t1.neighbor=t2.country AND t1.lname=t2.lname;

CREATE VIEW pairs_name_language AS
SELECT c1name, cname AS c2name, lname
FROM pairs_with_samePL NATURAL JOIN country ;

INSERT INTO Query8(SELECT * 
			FROM pairs_name_language
			ORDER BY lname ASC, c1name DESC);

DROP VIEW IF EXISTS country_most_lp CASCADE;

DROP VIEW IF EXISTS country_most_popularl CASCADE;

DROP VIEW IF EXISTS most_popular_neighbourl CASCADE;
DROP VIEW IF EXISTS pairs_with_samePL CASCADE;
DROP VIEW IF EXISTS pairs_name_language CASCADE;

-- Query 9 statements

CREATE VIEW  country_oid_height AS
SELECT cname, oid, height
FROM oceanAccess NATURAL JOIN country;


CREATE VIEW elev_depth_diff AS 
SELECT cname, MAX(height+depth) AS totalspan
FROM country_oid_height NATURAL JOIN ocean
GROUP BY cname;

CREATE VIEW no_direct_access_cid AS
(SELECT cid
	FROM country)
		EXCEPT
(SELECT cid
	FROM oceanAccess);

CREATE VIEW no_direct_access_span AS -- depth diff span of cid with no ocean
SELECT cname, height AS totalspan
FROM no_direct_access_cid NATURAL JOIN country;

CREATE VIEW elev_depth_diff_all AS
SELECT * 
FROM elev_depth_diff
	UNION
SELECT *
FROM no_direct_access_span;

CREATE VIEW not_largest_elev_depth AS
SELECT t1.cname, t2.totalspan
FROM elev_depth_diff_all t1, elev_depth_diff_all t2
WHERE t1.cname != t2.cname and t1.totalspan < t2.totalspan;

CREATE VIEW largest_elev_depth_cname AS
SELECT cname
FROM elev_depth_diff_all
	EXCEPT
SELECT cname
FROM not_largest_elev_depth;



INSERT INTO Query9(SELECT * 
			FROM largest_elev_depth_cname NATURAL JOIN
						elev_depth_diff_all);

DROP VIEW IF EXISTS country_oid_height CASCADE;
DROP VIEW IF EXISTS elev_depth_diff CASCADE;
DROP VIEW IF EXISTS no_direct_access_cid CASCADE;
DROP VIEW IF EXISTS no_direct_access_span CASCADE;
DROP VIEW IF EXISTS elev_depth_diff_all CASCADE;
DROP VIEW IF EXISTS not_largest_elev_depth CASCADE;
DROP VIEW IF EXISTS largest_elev_depth CASCADE;
DROP VIEW IF EXISTS largest_elev_depth_cname CASCADE;

-- Query 10 statements

CREATE VIEW countries_total_border AS -- total border length of each country
SELECT	country AS cid, SUM(length) AS borderslength
FROM neighbour
GROUP BY country;

CREATE VIEW country_with_nlb AS --countries with not longest borders
SELECT t1.cid, t1.borderslength
FROM countries_total_border t1, countries_total_border t2
WHERE t1.cid!=t2.cid AND t1.borderslength<t2.borderslength;

CREATE VIEW country_with_lb AS
(SELECT *
	FROM countries_total_border)
		EXCEPT
(SELECT *
	FROM country_with_nlb);

INSERT INTO Query10(SELECT cname, borderslength
			FROM country_with_lb NATURAL JOIN country);

DROP VIEW IF EXISTS countries_total_border CASCADE;
DROP VIEW IF EXISTS country_with_nlb CASCADE;
DROP VIEW IF EXISTS country_with_lb CASCADE;

