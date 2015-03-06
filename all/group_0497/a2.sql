

-- Add below your SQL statements. 
-- You can CREATE intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE OR REPLACE VIEW countryandneighbor AS
SELECT *
FROM country, neighbour
WHERE country.cid = neighbour.neighbor;

CREATE OR REPLACE VIEW high AS
SELECT c1.cid, c1.cname, c1.country
FROM countryAndNeighbor c1
        WHERE not exists (
        SELECT *
        FROM countryAndNeighbor c2
        WHERE c1.country = c2.country and c1.neighbor <> c2.neighbor and c2.height > c1.height
        );

CREATE OR REPLACE VIEW neighborinfo AS
SELECT high.cid AS c2id, high.cname AS c2name, high.country
FROM high;

CREATE OR REPLACE VIEW q1answer AS
SELECT cid AS c1id, cname AS c1name, c2id, c2name
FROM country, neighborinfo
WHERE neighborinfo.country = country.cid
ORDER BY c1name ASC;

INSERT INTO Query1(SELECT * FROM q1answer);

-- Drop Query 1 views
DROP VIEW countryandneighbor CASCADE;

-- Query 2 statements
CREATE OR REPLACE VIEW cIDocean AS
SELECT cid
FROM oceanAccess;

CREATE OR REPLACE VIEW oceanCountries AS
SELECT cid, cname
FROM country natural JOIN cIDOcean;

CREATE OR REPLACE VIEW landlocked AS
(SELECT cid, cname
FROM country)
EXCEPT
(SELECT *
FROM oceanCountries);

CREATE OR REPLACE VIEW q2answer AS
SELECT *
FROM landlocked
ORDER BY cname ASC;

INSERT INTO Query2(SELECT * FROM q2answer);

-- Drop Query 2 views
DROP VIEW cIDocean CASCADE;


-- Query 3 statements
CREATE OR REPLACE VIEW cidoceanaccess AS 
SELECT cid
FROM oceanAccess; 

CREATE OR REPLACE VIEW allnearocean AS 
SELECT cid, cname
FROM country natural JOIN cidoceanaccess;

CREATE OR REPLACE VIEW landlocked AS
(SELECT cid, cname
FROM country)
EXCEPT
(SELECT *
FROM allnearocean)
ORDER BY cname ASC; 

CREATE OR REPLACE VIEW cidneighbors AS 
SELECT country AS cid, neighbor,length 
FROM neighbour;

CREATE OR REPLACE VIEW landlockedneighbors AS 
SELECT cidneighbors.cid AS c1id, landlocked.cname AS c1name, neighbor
FROM cidneighbors JOIN landlocked ON cidneighbors.cid = landlocked.cid;

CREATE OR REPLACE VIEW oneneighbor AS 
SELECT c1id, count(neighbor)
FROM landlockedneighbors 
GROUP BY landlockedneighbors.c1id
HAVING count(neighbor) = 1;

CREATE OR REPLACE VIEW foundneighbor AS
SELECT c1id, c1name, neighbor AS cid 
FROM oneneighbor natural JOIN landlockedneighbors; 

CREATE OR REPLACE VIEW q3answer AS 
SELECT c1id, c1name, cid AS c2id, cname AS c2name 
FROM country natural JOIN foundneighbor
ORDER BY c1name ASC; 

INSERT INTO Query3 (SELECT * FROM q3answer);

-- Drop Query 3 views
DROP VIEW cidoceanaccess CASCADE;
DROP VIEW cidneighbors CASCADE;

-- Query 4 statements
CREATE OR REPLACE VIEW countrycoastlines AS
SELECT country.cid, country.cname, oceanAccess.oid
FROM oceanAccess JOIN country ON oceanAccess.cid = country.cid
GROUP BY country.cid, oceanAccess.oid
ORDER BY country.cid ASC;

CREATE OR REPLACE VIEW coastlinesneighbor AS
SELECT cname, neighbor, oid
FROM countrycoastlines JOIN neighbour ON countrycoastlines.cid = neighbour.country
GROUP BY countrycoastlines.oid, cname, neighbor;

CREATE OR REPLACE VIEW findneighbour AS
SELECT country.cname, oid
FROM country JOIN coastlinesneighbor ON coastlinesneighbor.neighbor = country.cid;

CREATE OR REPLACE VIEW alloceanaccess AS
(SELECT cname, oid
FROM findneighbour)
UNION 
(SELECT cname, oid
FROM countrycoastlines); 

CREATE OR REPLACE VIEW q4answer AS
SELECT cname, oname
FROM alloceanaccess natural JOIN ocean
ORDER BY cname ASC, oname DESC;

INSERT INTO Query4 (SELECT * FROM q4answer);

-- Drop Query 4 views
DROP VIEW countrycoastlines CASCADE;

-- Query 5 statements
CREATE OR REPLACE VIEW hdi2009to2014 AS
SELECT *
FROM hdi
WHERE year >= 2009 and year <= 2013;

CREATE OR REPLACE VIEW avg2009to2014 AS
SELECT cid, avg(hdi_score) AS avghdi
FROM hdi2009to2014
GROUP BY cid
ORDER BY avghdi DESC
LIMIT 10;

CREATE OR REPLACE VIEW q5answer AS
SELECT avg2009to2014.cid, cname, avghdi
FROM avg2009to2014 JOIN country ON avg2009to2014.cid = country.cid
ORDER BY avghdi DESC;

INSERT INTO Query5 (SELECT * FROM q5answer);

-- Drop Query 5 views
DROP VIEW hdi2009to2014 CASCADE;


-- Query 6 statements
CREATE OR REPLACE VIEW increase2009to2010 AS
SELECT h2.cid, h2.hdi_score
FROM hdi h1 JOIN hdi h2 on
	h1.cid = h2.cid and h1.year = 2009 and h2.year = 2010 and h1.hdi_score
	< h2.hdi_score;

CREATE OR REPLACE VIEW increase2009to2011 AS
SELECT h3.cid, h3.hdi_score
FROM increase2009to2010 i1 JOIN hdi h3 on
	i1.cid = h3.cid and h3.year = 2011 and i1.hdi_score < h3.hdi_score;

CREATE OR REPLACE VIEW increase2009to2012 AS
SELECT h4.cid, h4.hdi_score
FROM increase2009to2011 i2 JOIN hdi h4 on
	i2.cid = h4.cid and h4.year = 2012 and i2.hdi_score < h4.hdi_score;

CREATE OR REPLACE VIEW increase2009to2013 AS
SELECT h5.cid, h5.hdi_score
FROM increase2009to2012 i3 JOIN hdi h5 on
	i3.cid = h5.cid and h5.year = 2013 and i3.hdi_score < h5.hdi_score;

CREATE OR REPLACE VIEW q6answer AS
SELECT i.cid, c.cname
FROM increase2009to2013 i JOIN country c ON i.cid = c.cid
ORDER BY c.cname ASC;

INSERT INTO Query6(SELECT * FROM q6answer);

-- Drop Query 6 views
DROP VIEW increase2009to2010 CASCADE;

-- Query 7 statements
CREATE OR REPLACE VIEW followerspercountry AS
SELECT r.cid, rid, rname, (c.population * r.rpercentage) AS fpcountry
FROM country c JOIN religion r ON c.cid = r.cid;

CREATE OR REPLACE VIEW q7answer AS
SELECT rid, rname, sum(fpcountry) AS followers
FROM followerspercountry
GROUP BY rid, rname
ORDER BY followers DESC;
 
INSERT INTO Query7(SELECT * FROM q7answer);
	
-- Drop Query 7 views
DROP VIEW followerspercountry CASCADE;

-- Query 8 statements
CREATE OR REPLACE VIEW countrylanguage AS
	SELECT country, lid, lpercentage
	FROM neighbour JOIN language ON neighbour.country = language.cid;


CREATE OR REPLACE VIEW countrymaxlang AS
	SELECT country, max(lpercentage) AS mostpopular 
	FROM countrylanguage 
	GROUP BY country;

CREATE OR REPLACE VIEW countrymaxlangid AS
	SELECT DISTINCT cml.country, lid, mostpopular
	FROM countrymaxlang cml JOIN countrylanguage cl ON cml.country = cl.country
		and mostpopular = cl.lpercentage;

CREATE OR REPLACE VIEW neighborsmostpopularlang AS
	SELECT neighbour.country, neighbor, c1.country AS country1, c1.lid AS lid1,
		c1.mostpopular AS mostpopular1, c2.country AS country2, c2.lid AS lid2,
		c2.mostpopular AS mostpopular2
	FROM neighbour, countrymaxlangid c1, countrymaxlangid c2
	WHERE neighbour.country = c1.country and neighbour.neighbor = c2.country
		and c1.lid = c2.lid;

CREATE OR REPLACE VIEW q8answer AS
	SELECT DISTINCT country1.cname AS c1name, country2.cname AS c2name, lname
	FROM neighborsmostpopularlang n, country country1, country country2, language
	WHERE n.country = country1.cid and n.neighbor = country2.cid
		and n.lid1 = lid
	ORDER BY lname ASC, c1name DESC;

INSERT INTO Query8(SELECT * FROM q8answer);

DROP VIEW countrylanguage CASCADE;

--Query 9 statements
CREATE OR REPLACE VIEW countriesanddepth AS
SELECT cid, oa.oid, depth
FROM oceanAccess oa join ocean o on oa.oid = o.oid;

CREATE OR REPLACE VIEW countrydeepestocean AS
SELECT c.cid, max(depth) as deepestdepth
FROM country c join countriesanddepth cad on c.cid = cad.cid
GROUP BY c.cid;

CREATE OR REPLACE VIEW countrieswithnoocean AS
SELECT cid, 0 as deepestdepth
FROM ((SELECT cid
	FROM country)
	EXCEPT
	(SELECT cid
	FROM oceanAccess)) noocean;

CREATE OR REPLACE VIEW oceanandnoocean AS
(SELECT * FROM countrydeepestocean)
UNION
(SELECT * FROM countrieswithnoocean);

CREATE OR REPLACE VIEW largestdifference AS
SELECT ono.cid, deepestdepth, height, height + deepestdepth AS totalspan
FROM oceanandnoocean ono join country c on ono.cid = c.cid
ORDER BY totalspan DESC
LIMIT 1;

CREATE OR REPLACE VIEW q9answer AS
SELECT cname, totalspan
FROM largestdifference l join country c on l.cid = c.cid;

INSERT INTO Query9 (SELECT * FROM q9answer); 

DROP VIEW countriesanddepth CASCADE;

-- Query 10 statements
CREATE OR REPLACE VIEW sumlength AS 
SELECT sum(length), country
FROM neighbour 
GROUP BY country; 

CREATE OR REPLACE VIEW longcountry AS 
SELECT sum AS borderslength, country
FROM sumlength 
ORDER BY sum desc
limit 1; 
 
CREATE OR REPLACE VIEW q10answer AS 
SELECT cname, borderslength
FROM longcountry JOIN country ON longcountry.country = country.cid; 

INSERT INTO Query10(SELECT * FROM q10answer); 

DROP VIEW sumlength CASCADE;

