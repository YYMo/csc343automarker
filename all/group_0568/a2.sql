-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW cntrynghbr AS
SELECT country AS c1id, cname AS c2name, neighbor as c2id
FROM neighbour JOIN country ON neighbour.neighbor=country.cid
WHERE (country, height) IN
(SELECT neighbour.country, max(country.height)    
FROM neighbour JOIN country ON neighbour.neighbor=country.cid GROUP BY country);

INSERT INTO Query1 (
SELECT c1id, cname AS c1name, c2id, c2name
FROM (SELECT * FROM cntrynghbr) c2 JOIN country ON c2.c1id=country.cid
ORDER BY c1name ASC
);

DROP VIEW cntrynghbr;

-- Query 2 statements

INSERT INTO Query2 (
SELECT DISTINCT cid, cname
FROM country
WHERE cid NOT IN (SELECT cid FROM oceanaccess)
ORDER BY cname ASC
);


-- Query 3 statements

CREATE VIEW onenghbr AS
SELECT country AS cid
FROM neighbour
GROUP BY country
HAVING count(neighbor)=1;

INSERT INTO Query3 (
SELECT neighbour.country AS c1id, c1.cname AS c1name, neighbour.neighbor AS c2id, c2.cname AS c2name
FROM (SELECT cid FROM Query2 INTERSECT SELECT cid FROM onenghbr) landlocked, country c1, country c2, neighbour 
WHERE neighbour.country=c1.cid AND neighbour.country=landlocked.cid AND neighbour.neighbor=c2.cid
ORDER BY c1name ASC
);

DROP VIEW onenghbr;

-- Query 4 statements

CREATE VIEW neighbourocean AS
SELECT neighbour.country AS cid, oid
FROM neighbour JOIN oceanAccess ON neighbour.neighbor=oceanAccess.cid;


INSERT INTO Query4 (
SELECT cname, oname
FROM ((SELECT * FROM oceanAccess) UNION (SELECT * FROM neighbourocean)) 
accessible JOIN country USING (cid) JOIN ocean USING (oid)
ORDER BY cname ASC, oname DESC
);

DROP VIEW neighbourocean;

-- Query 5 statements


INSERT INTO Query5 (
SELECT country.cid, country.cname, avg(hdi_score) AS avghdi 
FROM country JOIN hdi USING (cid)
WHERE hdi.year BETWEEN 2009 AND 2013 
GROUP BY country.cid, country.cname 
ORDER BY avghdi DESC 
LIMIT 10
);


-- Query 6 statements

CREATE VIEW hdiyears AS
SELECT cid, year, hdi_score
FROM hdi
GROUP BY cid, year, hdi_score
HAVING year BETWEEN 2009 AND 2013;

INSERT INTO Query6 (
SELECT cid, cname
FROM (
SELECT t1.cid, t2.hdi_score-t1.hdi_score AS difference
FROM hdiyears t1 JOIN hdiyears t2 USING (cid)
WHERE t1.cid=t2.cid AND t2.year=t1.year+1) AS diff JOIN country USING (cid)
GROUP BY cid, cname
HAVING min(difference) > 0
ORDER BY cname ASC
);

DROP VIEW hdiyears;

-- Query 7 statements

INSERT INTO Query7 (
SELECT rid, rname, sum(religion.rpercentage/100*country.population) AS followers 
FROM religion JOIN country USING (cid)
GROUP BY rname, rid
ORDER BY followers
);


-- Query 8 statements

CREATE VIEW poplancountry AS
SELECT cid, cname, lpercentage, lname
FROM language JOIN country USING (cid) WHERE (cid, lpercentage) IN
(SELECT cid, MAX(lpercentage)
FROM language
GROUP BY cid);

INSERT INTO Query8 (
SELECT c1.cname AS c1name, c2.cname AS c2name, c1.lname
FROM (SELECT * FROM poplancountry) c1, (SELECT * FROM poplancountry) c2, neighbour nghbr
WHERE c1.cid=nghbr.country AND c2.cid=nghbr.neighbor 
AND c1.lname=c2.lname
ORDER BY lname ASC, c1name DESC
);

DROP VIEW poplancountry;

-- Query 9 statements

INSERT INTO Query9 (
SELECT country.cname, (country.height-ocean.depth) as totalspan
FROM country JOIN oceanAccess USING(cid) JOIN ocean USING(oid)
GROUP BY country.cname, totalspan 
ORDER BY totalspan DESC
LIMIT 1
);

-- Query 10 statements

INSERT INTO Query10 (
SELECT country.cname, SUM(neighbour.length) AS borderslength
FROM country JOIN neighbour ON country.cid = neighbour.country
GROUP BY cname
ORDER BY borderslength DESC
LIMIT 1
);