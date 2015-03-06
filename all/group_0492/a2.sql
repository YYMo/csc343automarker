-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.


-- CREATE VIEW statements
DROP VIEW IF EXISTS neighbours CASCADE;
CREATE VIEW neighbours AS SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name, c2.height AS nHeight FROM country c1, neighbour, country c2 WHERE c1.cid=neighbour.country AND neighbour.neighbor=c2.cid;

-- Query 1 statements
INSERT INTO Query1(SELECT c1id, c1name, c2id, c2name FROM neighbours n1 WHERE nheight >= ALL (SELECT max(nheight) FROM neighbours n2 WHERE n1.c1id=n2.c1id GROUP BY c1id) ORDER BY c1name ASC);


-- Query 2 statements
INSERT INTO Query2(SELECT cid, cname FROM country c1 WHERE NOT EXISTS (SELECT * FROM country c2, oceanaccess WHERE c1.cid=c2.cid AND c2.cid=oceanaccess.cid) ORDER BY cname ASC);


-- Query 3 statements
DROP VIEW IF EXISTS oneneighbour CASCADE;
CREATE VIEW oneneighbour AS SELECT country, count(*) FROM neighbour GROUP BY country HAVING count(*)=1;
INSERT INTO Query3(SELECT c1id, c1name, c2id, c2name FROM neighbours WHERE neighbours.c1id IN (SELECT cid FROM Query2) AND neighbours.c1id IN (SELECT country FROM oneneighbour) ORDER BY c1name ASC);


-- Query 4 statements
DROP VIEW IF EXISTS indirectocean CASCADE;
CREATE VIEW indirectocean AS SELECT DISTINCT c1id AS cid, c1name AS cname, ocean.oid, ocean.oname FROM neighbours, oceanaccess, ocean WHERE c2id=cid AND oceanaccess.oid=ocean.oid; 
DROP VIEW IF EXISTS directocean CASCADE;
CREATE VIEW directocean AS SELECT DISTINCT country.cid, cname, ocean.oid, ocean.oname FROM country, oceanaccess, ocean WHERE country.cid=oceanaccess.cid AND oceanaccess.oid=ocean.oid; 
INSERT INTO Query4(SELECT cname, oname FROM ((SELECT * FROM indirectocean) UNION (SELECT * FROM directocean)) q4 ORDER BY cname ASC, oname DESC);


-- Query 5 statements
DROP VIEW IF EXISTS cidhdi CASCADE;
CREATE VIEW cidhdi AS SELECT cid, avg(hdi_score) AS avghdi FROM hdi WHERE year>=2009 AND year <= 2013 GROUP BY cid ORDER BY avg(hdi_score) DESC LIMIT 10;
INSERT INTO Query5(SELECT country.cid, cname, avghdi FROM country, cidhdi WHERE country.cid=cidhdi.cid ORDER BY avghdi DESC);


-- Query 6 statements
DROP VIEW IF EXISTS increasedyears CASCADE;
CREATE VIEW increasedyears AS SELECT cid, year FROM hdi h1 WHERE year >=2009 AND year <=2013 AND NOT EXISTS (SELECT cid FROM hdi h2 WHERE h2.year >=2009 AND h2.year <=2013 AND h1.cid=h2.cid AND h1.year > h2.year AND (h1.hdi_score < h2.hdi_score)) ORDER BY cid;
DROP VIEW IF EXISTS alwaysincreasing CASCADE;
CREATE VIEW alwaysincreasing AS SELECT cid FROM increasedyears GROUP BY cid HAVING count(*) = 5;
INSERT INTO Query6(SELECT cid, cname FROM country WHERE cid IN (SELECT cid FROM alwaysincreasing) ORDER BY cname ASC);


-- Query 7 statements
INSERT INTO Query7(SELECT rid, rname, sum(round(rpercentage*population)) AS followers FROM country, religion WHERE country.cid=religion.cid GROUP BY rid, rname ORDER BY followers DESC);


-- Query 8 statements
DROP VIEW IF EXISTS countrylanguage CASCADE;
CREATE VIEW countrylanguage AS SELECT cid, lname FROM language l1 WHERE lpercentage = (SELECT max(lpercentage) FROM language l2 WHERE l1.cid=l2.cid GROUP BY cid);
DROP VIEW IF EXISTS neighbourcountrylanguage CASCADE;
CREATE VIEW neighbourcountrylanguage AS SELECT c1name, cl1.lname AS l1name, c2name, cl2.lname AS l2name FROM neighbours, countrylanguage AS cl1, countrylanguage AS cl2 WHERE c1id=cl1.cid AND c2id=cl2.cid;
INSERT INTO Query8(SELECT c1name, c2name, l1name AS lname FROM neighbourcountrylanguage WHERE l1name=l2name ORDER BY lname ASC, c1name DESC);


-- Query 9 statements
DROP VIEW IF EXISTS spans CASCADE;
CREATE VIEW spans as SELECT country.cname, height + coalesce(maxdepth, 0) as totalspan FROM country LEFT JOIN (SELECT cid, max(ocean.depth) as maxdepth FROM oceanaccess, ocean WHERE oceanaccess.oid=ocean.oid GROUP BY cid) oceandepth ON country.cid=oceandepth.cid;
INSERT INTO Query9(SELECT cname, totalspan FROM spans WHERE totalspan = (SELECT max(totalspan) FROM spans));


-- Query 10 statements
DROP VIEW IF EXISTS borders CASCADE;
CREATE VIEW borders AS SELECT cname, sum(length) as borderslength FROM neighbour, country WHERE neighbour.country = country.cid GROUP BY country.cname;
INSERT INTO Query10(SELECT cname, borderslength FROM borders WHERE borderslength=(SELECT max(borderslength) FROM borders));
