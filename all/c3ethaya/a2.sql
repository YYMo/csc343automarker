-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW nheight AS 
SELECT country, neighbor, cname AS nname 
FROM neighbour n1 JOIN country c1 ON n1.neighbor=c1.cid
WHERE height >= ALL(SELECT height
		    FROM neighbour JOIN country ON neighbour.neighbor=country.cid 
		    WHERE country=n1.country);

INSERT INTO QUERY1(
SELECT country c1id, cname AS c1name, neighbor AS c2id, nname AS c2name
FROM nheight JOIN country ON nheight.country=country.cid                                             
ORDER BY c1name ASC);

DROP VIEW nheight;    


-- Query 2 statements
CREATE VIEW landlocked AS
(SELECT cid FROM country)
	EXCEPT
(SELECT cid FROM oceanAccess);

INSERT INTO QUERY2(
SELECT cid, cname
FROM landlocked JOIN country USING(cid)
ORDER BY cname);


-- Query 3 statements
CREATE VIEW single AS
SELECT country, MAX(neighbor) as neighbor
FROM neighbour
WHERE country IN (SELECT cid FROM landlocked)
GROUP BY country
HAVING COUNT(neighbor)=1;

INSERT INTO QUERY3(
SELECT country as c1id, c1.cname as c1name, neighbor as c2id, c2.cname as c2name
FROM (single JOIN country c1 ON single.country=c1.cid) JOIN country c2 ON single.neighbor=c2.cid
ORDER BY c1name ASC);

DROP VIEW single;
DROP VIEW landlocked; 


-- Query 4 statements
CREATE VIEW someAccess AS 
(SELECT * FROM oceanAccess)
	UNION
(SELECT country as cid, oid
 FROM oceanAccess JOIN neighbour ON neighbour.neighbor=oceanAccess.cid);

INSERT INTO QUERY4( 
SELECT cname, oname
FROM (someAccess JOIN country USING(cid)) JOIN ocean USING(oid)
ORDER BY cname ASC, oname DESC);

DROP VIEW someAccess;


-- Query 5 statements
CREATE VIEW averageHDI AS 
SELECT cid, AVG(hdi_score) as avghdi   
FROM hdi
WHERE year >= 2009 AND year <= 2013
GROUP BY cid;

INSERT INTO QUERY5(
SELECT cid, cname, avghdi
FROM averageHDI JOIN country USING(cid)
ORDER BY avghdi DESC
LIMIT 10);

DROP VIEW averageHDI; 


-- Query 6 statements
CREATE VIEW increasing AS
(SELECT cid FROM hdi)
	EXCEPT
(SELECT cid
 FROM (SELECT * FROM hdi WHERE year >= 2009 AND year <= 2013) hdi09to13
 WHERE hdi_score <= ANY(SELECT hdi_score FROM hdi WHERE year>=2009 AND year<hdi09to13.year AND cid=hdi09to13.cid));

INSERT INTO QUERY6(
SELECT cid, cname
FROM increasing JOIN country USING(cid)
ORDER BY cname ASC);

DROP VIEW increasing;

-- Query 7 statements
INSERT INTO QUERY7(
SELECT rid, MAX(rname), SUM(rpercentage*population) AS followers
FROM religion JOIN country USING(cid)
GROUP BY rid
ORDER BY followers DESC);


-- Query 8 statements
CREATE VIEW poplang AS
SELECT l1.cid AS cid, l1.lid AS poplid, l1.lname AS poplname
FROM language l1
WHERE lpercentage >= ALL(SELECT lpercentage FROM language WHERE cid=l1.cid);

CREATE VIEW lneighbour AS
SELECT p1.cid AS country, p2.cid AS neighbor, p1.poplname as lname
FROM poplang p1 JOIN poplang p2 USING(poplid) 
WHERE p2.cid IN (SELECT neighbor FROM neighbour WHERE country=p1.cid);

INSERT INTO QUERY8(
SELECT c1.cname as c1name, c2.cname as c2name, lname
FROM (lneighbour JOIN country c1 ON c1.cid=lneighbour.country) JOIN country c2 ON c2.cid=lneighbour.neighbor
ORDER BY lname ASC, c1name DESC);

DROP VIEW poplang CASCADE;

-- Query 9 statements
CREATE VIEW span AS   
SELECT cid, cname, height+MAX(COALESCE(depth,0)) as totalspan
FROM (oceanAccess JOIN ocean USING(oid)) RIGHT JOIN country USING(cid)
GROUP BY cid;

INSERT INTO QUERY9(
SELECT cname, totalspan 
FROM span s1
WHERE NOT EXISTS(SELECT totalspan FROM span WHERE totalspan > s1.totalspan)); 
 
DROP VIEW span;
-- Query 10 statements
CREATE VIEW borders AS 
SELECT country, SUM(length) AS borderslength
FROM neighbour JOIN country ON neighbour.neighbor=country.cid
GROUP BY country;
  
INSERT INTO QUERY10(
SELECT cname, borderslength 
FROM borders b1 JOIN country ON b1.country=country.cid
WHERE NOT EXISTS(SELECT borderslength FROM borders WHERE borderslength > b1.borderslength)); 

DROP VIEW borders;
