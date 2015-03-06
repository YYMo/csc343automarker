-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statement

CREATE VIEW tempview AS
SELECT * FROM
((SELECT cid AS c2id, height, cname AS c2name FROM country) AS temp1
NATURAL JOIN
(SELECT country AS c1id, neighbor AS c2id FROM neighbour) AS temp2);

CREATE VIEW tempview2 AS
SELECT * FROM
(SELECT c1id, c2id, c2name, height FROM tempview) AS temp1
NATURAL JOIN
(SELECT cid as c1id, cname as c1name FROM country) AS temp2; 

CREATE VIEW tempview3 AS
SELECT * FROM
(SELECT c1id, max(height) AS height FROM tempview2 GROUP BY c1id) AS temp1
NATURAL JOIN
(SELECT * FROM tempview2) AS temp2;

CREATE VIEW tempview4 AS
SELECT * FROM
(SELECT c1id, c1name, c2id, c2name FROM tempview3 ORDER BY c1name ASC) AS temp1;

INSERT INTO Query1 SELECT * FROM tempview4 ORDER BY c1name ASC;

DROP VIEW tempview4;
DROP VIEW tempview3;
DROP VIEW tempview2;
DROP VIEW tempview;

-- Query 2 statements

CREATE VIEW tempview AS
SELECT * FROM
(SELECT cid FROM country) AS temp1
EXCEPT
(SELECT cid FROM oceanAccess);

CREATE VIEW tempview2 AS
SELECT * FROM
((SELECT cid, cname FROM country) AS temp1
NATURAL JOIN
(SELECT * FROM tempview) AS temp2) ORDER BY cid;

INSERT INTO Query2 (SELECT cid, cname FROM tempview2 ORDER BY cname);

DROP VIEW tempview2;
DROP VIEW tempview;

-- Query 3 statements

CREATE VIEW tempview AS
SELECT * FROM
(SELECT cid FROM country) AS temp1
EXCEPT
(SELECT cid FROM oceanAccess);

CREATE VIEW tempview2 AS
SELECT * FROM
(SELECT cid AS c2id, cname AS c2name FROM country) AS temp6
NATURAL JOIN 
(SELECT * FROM
(SELECT cid AS c1id, cname as c1name FROM country) AS temp4
NATURAL JOIN
(SELECT * FROM
(SELECT country AS c1id FROM neighbour GROUP BY country HAVING count(neighbor) = 1) AS temp1
NATURAL JOIN
(SELECT country AS c1id, neighbor AS c2id FROM neighbour) AS temp2) AS temp3) AS temp5;

CREATE VIEW tempview3 AS
SELECT c1id, c1name, c2id, c2name FROM tempview2 ORDER BY c1name ASC;

INSERT INTO Query3 SELECT * FROM tempview3 ORDER BY c1name ASC;

DROP VIEW tempview3;
DROP VIEW tempview2;
DROP VIEW tempview;

-- Query 4 statements

CREATE VIEW tempview AS
SELECT country AS cid, oid FROM
(SELECT country, neighbor FROM neighbour) AS temp1
NATURAL JOIN
(SELECT cid AS neighbor, oid FROM oceanAccess) AS temp2;

CREATE VIEW tempview2 AS
SELECT * FROM tempview 
UNION
SELECT * FROM  oceanAccess;

CREATE VIEW tempview3 AS
SELECT cname, oname FROM
(SELECT oid, oname FROM ocean) AS temp4
NATURAL JOIN
(SELECT * FROM
(SELECT * FROM tempview2) AS temp1
NATURAL JOIN
(SELECT cid, cname FROM country) AS temp2) AS temp3 ORDER BY cname ASC, oname DESC;

INSERT INTO Query4 SELECT * FROM tempview3;

DROP VIEW tempview3;
DROP VIEW tempview2;
DROP VIEW tempview;

-- Query 5 statements

CREATE VIEW tempview AS
SELECT cid, AVG(hdi_score) AS avghdi 
FROM hdi 
WHERE (2008<year AND year<2014) 
GROUP BY cid 
ORDER BY avghdi ASC 
LIMIT 10;

CREATE VIEW tempviewNamed AS
SELECT * FROM
(SELECT cid, cname FROM country) AS temp1
NATURAL JOIN
(SELECT * FROM tempview) AS temp2;

INSERT INTO Query5 (SELECT * FROM tempviewNamed ORDER BY avghdi DESC);

DROP VIEW tempviewNamed;
DROP VIEW tempview;

-- Query 6 statements

CREATE VIEW v1 AS
SELECT cid, hdi_score AS h1 FROM hdi WHERE year = 2009;

CREATE VIEW v2 AS
SELECT cid, hdi_score AS h2 FROM hdi WHERE year = 2010;

CREATE VIEW v3 AS
SELECT cid, hdi_score AS h3 FROM hdi WHERE year = 2011;

CREATE VIEW v4 AS
SELECT cid, hdi_score AS h4 FROM hdi WHERE year = 2012;

CREATE VIEW v5 AS
SELECT cid, hdi_score AS h5 FROM hdi WHERE year = 2013;

CREATE VIEW combineV AS
SELECT * FROM
((((SELECT * FROM v1) AS temp1
NATURAL JOIN
(SELECT * FROM v2) AS temp2) AS temp3
NATURAL JOIN
(SELECT * FROM v3) AS temp4) AS temp5
NATURAL JOIN
(SELECT * FROM v4) AS temp6) AS temp7
NATURAL JOIN
(SELECT * FROM v5) AS temp8;

CREATE VIEW filter AS
SELECT * FROM combineV 
WHERE (h5>h4 AND h4>h3 AND h3>h2 AND h2>h1);

SELECT * FROM combineV;
SELECT * FROM filter;

CREATE VIEW addcname AS
SELECT cid, cname FROM
(SELECT * FROM filter) AS temp1
NATURAL JOIN
(SELECT * FROM country) AS temp2
ORDER BY cname ASC; 

INSERT INTO Query6 SELECT * FROM addcname;

DROP VIEW addcname;
DROP VIEW filter;
DROP VIEW combineV;
DROP VIEW v5;
DROP VIEW v4;
DROP VIEW v3;
DROP VIEW v2;
DROP VIEW v1;

-- Query 7 statements

CREATE VIEW tempview AS
SELECT * FROM
(SELECT * FROM religion) AS temp1
NATURAL JOIN
(SELECT * FROM country) AS temp2;

CREATE VIEW tempview2 AS
SELECT rid, rname, rpercentage*population AS follower FROM tempview;
-- Is that right?

CREATE VIEW tempview3 AS
SELECT rid, rname, sum(follower) AS followers FROM tempview2 GROUP BY rid, rname;

INSERT INTO Query7 SELECT * FROM tempview3 ORDER BY followers DESC;

DROP VIEW tempview3;
DROP VIEW tempview2;
DROP VIEW tempview;

-- Query 8 statements

CREATE VIEW mostPopular AS
SELECT cid, lid, lname FROM
(SELECT cid, max(lpercentage) AS lpercentage FROM language GROUP BY cid) AS temp1
NATURAL JOIN 
(SELECT * FROM language) AS temp2; 

CREATE VIEW giveCountry AS
SELECT * FROM
(SELECT cid AS cid1, cname AS c1name FROM country) AS temp1
NATURAL JOIN
(SELECT cid AS cid1, lid AS lid1, lname FROM mostPopular) AS temp2;

CREATE VIEW giveNeighbour AS
SELECT * FROM
(SELECT country AS cid1, neighbor AS cid2 FROM neighbour) AS temp1
NATURAL JOIN
(SELECT cid AS cid2, lid AS lid2, lname FROM mostPopular) AS temp2;

CREATE VIEW giveNeighbourN AS
SELECT * FROM
(SELECT * FROM giveNeighbour) AS temp1
NATURAL JOIN
(SELECT cid AS cid2, cname AS c2name FROM country) AS temp2;

CREATE VIEW combine AS
SELECT * FROM
(SELECT * FROM giveCountry) AS temp1
NATURAL JOIN
(SELECT * FROM giveNeighbourN) AS temp2
ORDER BY lname ASC, c1name DESC;

INSERT INTO Query8 SELECT c1name, c2name, lname FROM combine;

DROP VIEW combine;
DROP VIEW giveNeighbourN;
DROP VIEW giveNeighbour;
DROP VIEW giveCountry;
DROP VIEW mostPopular;

-- Query 9 statements

CREATE VIEW hasAccess AS
SELECT * FROM
(SELECT cid, oid FROM oceanaccess) AS temp1
NATURAL JOIN
(SELECT cid, cname, height FROM country) AS temp2;

CREATE VIEW noAccessData AS
SELECT cid, cname, height AS totalspan FROM country WHERE cid NOT IN (
    SELECT cid
    FROM oceanaccess
) 
ORDER BY totalspan DESC LIMIT 1;

CREATE VIEW hasAccessData AS
SELECT cid, cname, max(height+depth) AS totalspan FROM
(SELECT * FROM hasAccess) AS temp1
NATURAL JOIN
(SELECT oid, depth FROM ocean) AS temp2
GROUP BY cid, cname ORDER BY totalspan DESC LIMIT 1;

CREATE VIEW maxSpan AS
SELECT cname, totalspan FROM
((SELECT * FROM noAccessData) 
UNION
(SELECT * FROM hasAccessData)) AS temp1 
ORDER BY totalspan DESC LIMIT 1;

INSERT INTO Query9 SELECT cname, totalspan FROM maxSpan;

DROP VIEW maxSpan;
DROP VIEW noAccessData;
DROP VIEW hasAccessData;
DROP VIEW hasAccess; 

-- Query 10 statements

CREATE VIEW tempview AS
SELECT * FROM
(SELECT * FROM neighbour) AS temp1
NATURAL JOIN
(SELECT cid AS country, cname FROM country) AS temp2;

CREATE VIEW tempview2 AS
SELECT country, cname, SUM(length) AS borderslength 
FROM tempview 
GROUP BY country, cname;

CREATE VIEW tempview3 AS
SELECT cname, borderslength FROM tempview2 ORDER BY borderslength ASC LIMIT 1;

INSERT INTO Query10 SELECT * FROM tempview3; 

DROP VIEW tempview3;
DROP VIEW tempview2;
DROP VIEW tempview;












