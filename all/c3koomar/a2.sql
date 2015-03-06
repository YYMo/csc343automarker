-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW dcountry AS
SELECT cid, cname, height, population 
FROM country;

CREATE VIEW dneighbour AS
SELECT country, neighbor, length 
FROM neighbour;
	
CREATE VIEW neighbourheight AS
SELECT c1.cid AS c1id, c1.height AS c1height, c2.cid AS c2id, c2.height AS c2height 
FROM (dcountry c1 
	JOIN dneighbour
		ON dneighbour.country = c1.cid 
	JOIN dcountry c2 
		ON dneighbour.neighbor = c2.cid);

CREATE VIEW highestneighbour AS
select c1id, c2id 
from neighbourheight n 
where not exists(
	select c1id, c2id 
	from neighbourheight n2 
	where n2.c2height > n.c2height 
		and n.c1id = n2.c1id);

INSERT INTO query1 (
SELECT c1id, c1.cname AS c1name, c2id, c2.cname AS c2name 
FROM (highestneighbour h 
	JOIN dcountry c1 ON h.c1id = c1.cid 
	JOIN dcountry c2 ON h.c2id = c2.cid) 
	ORDER BY c1name);

DROP VIEW dcountry CASCADE;
DROP VIEW dneighbour CASCADE;

-- Query 2 statements

CREATE VIEW countryCopy AS
SELECT * FROM country;

CREATE VIEW oceanCopy AS 
SELECT * FROM oceanaccess;
	
INSERT INTO query2 (
SELECT cid, cname
FROM countryCopy 
WHERE NOT EXISTS (
	SELECT * 
	FROM oceanCopy 
	WHERE oceanCopy.cid = countryCopy.cid));

DROP VIEW countryCopy CASCADE;
DROP VIEW oceanCopy CASCADE;
	
-- Query 3 statements
CREATE VIEW landlocked AS
SELECT * FROM query2;

CREATE VIEW noneighbour AS
SELECT * 
FROM landlocked 
WHERE NOT EXISTS (
	SELECT * 
	FROM neighbour 
	WHERE neighbour.country = landlocked.cid
);

CREATE VIEW morethanone AS
SELECT * 
FROM landlocked 
WHERE EXISTS (
	SELECT * 
	FROM neighbour n1
		JOIN neighbour n2
			ON n1.country = n2.country 
			AND NOT n1.neighbor = n2.neighbor
	WHERE n1.country = landlocked.cid)
;

CREATE VIEW exactlyone AS
SELECT * 
FROM landlocked 
	EXCEPT 
(SELECT * FROM noneighbour) 	
	EXCEPT 
(SELECT * FROM morethanone);
	
INSERT INTO query3 (
SELECT exactlyone.cid AS c1id, exactlyone.cname AS c1name, neighbour.neighbor as c2id, country.cname as c2name
FROM exactlyone 
	JOIN neighbour 
		ON exactlyone.cid = neighbour.country
	JOIN country
		ON neighbour.neighbor = country.cid
);

DROP VIEW landlocked CASCADE; 
-- Query 4 statements
CREATE VIEW directaccess AS
SELECT DISTINCT cname, oname
FROM country c 
	JOIN oceanaccess oa 
		ON c.cid = oa.cid
	JOIN ocean o
		ON oa.oid = o.oid;

CREATE VIEW indirectaccess AS
SELECT DISTINCT c2id AS cname, oname
FROM (
	SELECT neighbor, oname, c2.cname AS c2id
	FROM directaccess da
		JOIN country c
			ON da.cname = c.cname
		JOIN neighbour n
			ON c.cid = n.country
		JOIN country c2
			ON n.neighbor = c2.cid)
AS directneighbours;

INSERT INTO query4 (
	SELECT * FROM directaccess	
	UNION 
	SELECT * FROM indirectaccess
	ORDER BY cname ASC, oname DESC
);

DROP VIEW directaccess CASCADE;

-- Query 5 statements
CREATE VIEW avgHDI AS
SELECT AVG(hdi_score) AS avghdi, cid
FROM hdi
WHERE year > 2008 AND year < 2014
GROUP BY cid
ORDER BY AVG(hdi_score) DESC;

CREATE VIEW top10 AS 
SELECT * 
FROM avgHDI
LIMIT 10;

INSERT INTO query5 (
SELECT top10.cid, cname, avghdi 
FROM top10 
	JOIN country
		ON top10.cid = country.cid
ORDER BY avghdi DESC
);

DROP VIEW avghdi CASCADE;
-- Query 6 statements
CREATE VIEW fiveyearhdi AS
SELECT * 
FROM hdi
WHERE year > 2008 AND year < 2014
ORDER BY year ASC;

CREATE VIEW constantincrease AS
SELECT DISTINCT cid 
FROM fiveyearhdi
WHERE cid NOT IN(
	SELECT h1.cid 
	FROM fiveyearhdi h1, fiveyearhdi h2
	WHERE h1.year < h2.year AND h1.hdi_score > h2.hdi_score AND h1.cid = h2.cid);
	
INSERT INTO query6 (
SELECT cid, cname 
FROM constantincrease 
	NATURAL JOIN country);

DROP VIEW fiveyearhdi CASCADE;

-- Query 7 statements
CREATE VIEW percountry AS
SELECT cid, rid, rname, rpercentage * population AS followers
FROM religion NATURAL JOIN country;

CREATE VIEW perreligion AS
SELECT rid, rname, SUM(followers) AS followers
FROM percountry
GROUP BY rid, rname
ORDER BY followers DESC;

INSERT INTO query7 (
SELECT * from perreligion
);

DROP VIEW percountry CASCADE;
-- Query 8 statements
DROP VIEW IF EXISTS samelanguage CASCADE;
DROP VIEW IF EXISTS mostpopular CASCADE;

CREATE VIEW mostpopular AS
SELECT maxpercent.cid, lid, lname, maxpercent.lpercentage
FROM (  SELECT cid, max(lpercentage) AS lpercentage
		FROM language
		GROUP BY cid) as maxpercent
	JOIN language
	ON language.lpercentage = maxpercent.lpercentage AND maxpercent.cid = language.cid;
		

CREATE VIEW samelanguage AS
SELECT l1.cid AS c1id, l2.cid AS c2id, l1.lname 
FROM mostpopular l1, mostpopular l2 
WHERE l1.lid = l2.lid AND l1.cid <> l2.cid;

CREATE VIEW sharedneighbour AS
SELECT c1id, c2id 
FROM samelanguage
INTERSECT
SELECT country AS c1id, neighbor AS c2id
FROM neighbour;

CREATE VIEW countrynames AS
SELECT c1id, c1.cname AS c1name, c2id, c2.cname AS c2name
FROM sharedneighbour 
	JOIN country c1
		ON sharedneighbour.c1id = c1.cid
	JOIN country c2
		ON sharedneighbour.c2id = c2.cid;

CREATE VIEW q8 AS
SELECT c1name, c2name, lname 
FROM countrynames 
	JOIN samelanguage	
		ON countrynames.c1id = samelanguage.c1id AND countrynames.c2id = samelanguage.c2id
ORDER BY lname ASC, c1name DESC;

INSERT INTO query8 (SELECT * FROM q8);
-- Query 9 statements
CREATE VIEW deepestocean AS
SELECT cid, max(depth) AS depth
FROM oceanaccess NATURAL JOIN ocean
GROUP BY cid;

CREATE VIEW deepestall AS
SELECT country.cid, height, CASE WHEN depth IS NULL THEN 0 
								ELSE depth END as depth
FROM country LEFT OUTER JOIN deepestocean
	ON country.cid = deepestocean.cid;

CREATE VIEW differences AS
SELECT cid, (height + depth) as totalspan
FROM deepestall;
	
CREATE VIEW highest AS
SELECT cname, totalspan 
FROM differences NATURAL JOIN country
WHERE totalspan = (SELECT MAX(totalspan) 
					FROM differences);

INSERT INTO query9 (SELECT * FROM highest);

DROP VIEW IF EXISTS deepestocean CASCADE;
-- Query 10 statements

CREATE VIEW borderlength AS
SELECT country, SUM(length) AS border
FROM neighbour
GROUP BY country;

CREATE VIEW maxlength AS
SELECT cname, border
FROM borderlength JOIN country c
	ON borderlength.country = c.cid
WHERE border = (SELECT MAX(border) FROM borderlength);

INSERT INTO query10 (SELECT * FROM maxlength);

DROP VIEW IF EXISTS borderlength CASCADE;