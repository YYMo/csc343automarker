

-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

INSERT INTO Query1 (SELECT a.country as c1id, b.cname as c1name, a.neighbor as c2id, c.cname as c2name 
					FROM country b, country c, neighbour a 
					WHERE c.cid = a.neighbor AND b.cid = a.country AND
					c.height IN( SELECT MAX(foo.height) FROM (
												SELECT height 
												FROM country, neighbour 
												WHERE a.country = country AND neighbor = cid) AS foo)
					ORDER BY c1name ASC);
												

-- Query 2 statements
INSERT INTO Query2 (SELECT cid, cname FROM country WHERE cid IN(SELECT cid FROM country EXCEPT (SELECT cid FROM oceanAccess)) ORDER BY cname ASC);


-- Query 3 statements

INSERT INTO Query3 (SELECT a.cid as c1id, a.cname as c1name, b.c2id, b.c2name FROM Query2 a, Query1 b WHERE b.c1id = a.cid AND a.cid IN (SELECT country FROM neighbour GROUP BY country HAVING COUNT (country) = 1) ORDER BY c1name ASC);

-- Query 4 statements

INSERT INTO Query4 (SELECT a.cname, b.oname FROM country a , ocean b,((SELECT * FROM OceanAccess) UNION(SELECT a.country AS cid, b.oid AS oid FROM neighbour a, oceanAccess b WHERE a.neighbor = b.cid)) c WHERE a.cid = c.cid AND b.oid = c.oid ORDER BY cname ASC, oname DESC);

-- Query 5 statements

INSERT INTO Query5 (SELECT a.cid, b.cname, SUM(a.hdi_score)/5 AS avghdi FROM hdi a,country b WHERE a.cid = b.cid AND a.year >2008 AND a.year < 2014 GROUP BY a.cid, cname ORDER BY avghdi DESC LIMIT 10);

-- Query 6 statements

INSERT INTO Query6 (SELECT a.cid, b.cname FROM hdi a,country b WHERE b.cid = a.cid AND(SELECT c.hdi_score FROM hdi c WHERE year = 2009 AND c.cid = a.cid) < (SELECT c.hdi_score FROM hdi c WHERE year = 2010 AND c.cid = a.cid) AND (SELECT c.hdi_score FROM hdi c WHERE year = 2010 AND c.cid = a.cid) < (SELECT c.hdi_score FROM hdi c WHERE year = 2011 AND c.cid = a.cid) AND (SELECT c.hdi_score FROM hdi c WHERE year = 2011 AND c.cid = a.cid) < (SELECT c.hdi_score FROM hdi c WHERE year = 2012 AND c.cid = a.cid) AND (SELECT c.hdi_score FROM hdi c WHERE year = 2012 AND c.cid = a.cid) < (SELECT c.hdi_score FROM hdi c WHERE year = 2013 AND c.cid = a.cid ORDER BY b.cname ASC));

-- Query 7 statements

INSERT INTO Query7 (SELECT a.rid, a.rname, SUM(a.rpercentage * b.population) AS followers FROM religion a, country b WHERE b.cid = a.cid GROUP BY a.rid, a.rname ORDER BY followers DESC); 

-- Query 8 statements

INSERT INTO Query8 (SELECT a.cname as c1name, b.cname as c2name, c.lname
FROM language c INNER JOIN country a ON c.cid = a.cid 
				INNER JOIN neighbour d ON a.cid = d.country 
				INNER JOIN country b ON b.cid = d.neighbor 
				INNER JOIN language e ON e.cid = b.cid 
				WHERE c.lid = e.lid
				GROUP BY c1name,c2name,c.lname,c.lpercentage,e.lpercentage,a.cid,b.cid
				HAVING c.lpercentage = (SELECT MAX(f.lpercentage) FROM language f WHERE f.cid = a.cid) 
				AND (e.lpercentage = (SELECT MAX(lpercentage) FROM language f WHERE f.cid = b.cid)) 
				ORDER BY c.lname ASC, c1name DESC); 
-- Query 9 statements
INSERT INTO Query9 (SELECT cname, height + COALESCE(depth,0) as totalspan 
FROM country FULL OUTER JOIN oceanAccess ON country.cid = oceanAccess.cid 
FULL OUTER JOIN ocean ON oceanAccess.oid = ocean.oid  GROUP BY 
cname,height,depth HAVING height + COALESCE(depth,0) IN ((SELECT Max(height + COALESCE(depth,0)) as totalspan 
FROM country FULL OUTER JOIN oceanAccess ON country.cid = oceanAccess.cid 
FULL OUTER JOIN ocean ON oceanAccess.oid = ocean.oid)) );


-- Query 10 statements

CREATE VIEW border AS
SELECT country, SUM(length) FROM neighbour GROUP BY country ORDER BY 
sum DESC LIMIT 1;
--Get name
CREATE VIEW named AS
SELECT country.cname, border.sum FROM border LEFT JOIN country ON
border.country=country.cid;
--Insert and clean up
INSERT INTO Query10
SELECT * FROM named;
DROP VIEW named CASCADE;
DROP VIEW border CASCADE;

