-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW NH AS (SELECT * FROM country C JOIN neighbour N ON C.cid = N.neighbor);
CREATE VIEW MH AS (SELECT country AS c1id, max(height) AS height FROM NH GROUP BY country);
CREATE VIEW STUFF AS(SELECT c1id, neighbor AS c2id, cname AS c2name FROM MH JOIN NH ON MH.c1id = NH.country AND neighbor = NH.neighbor AND MH.height = NH.height);

INSERT INTO Query1(SELECT c1id, cname AS c1name, c2id, c2name FROM STUFF JOIN country C ON C.cid = STUFF.c1id GROUP BY c1id, c1name, c2id, c2name ORDER BY c1name ASC);

DROP VIEW NH;
DROP VIEW MH;
DROP VIEW STUFF;

-- Query 2 statements

INSERT INTO Query2 (SELECT distinct cid, cname
FROM country
WHERE cid NOT IN (SELECT cid FROM oceanAccess) 
ORDER BY cname ASC);

-- Query 3 statements

CREATE VIEW L AS(SELECT distinct cid, cname FROM country WHERE cid NOT IN (SELECT cid FROM oceanAccess));
CREATE VIEW N1 AS(SELECT cid AS c1id
FROM neighbour N JOIN L ON L.cid = N.country GROUP BY cid HAVING count(neighbor) = 1)
CREATE VIEW LNAME AS(SELECT c1id, cname AS c1name FROM N1 JOIN country C ON c1id = C.cid);
CREATE VIEW NID AS(SELECT c1id, c1name, neighbor AS c2id FROM LNAME JOIN neighbour N ON c1id = N.country); 

INSERT INTO Query3 (SELECT c1id, c1name, c2id, cname AS c2name FROM NID JOIN country C ON c2id = C.cid ORDER BY c1name ASC);

DROP VIEW L;
DROP VIEW N1;
DROP VIEW LNAME;
DROP VIEW NID;

-- Query 4 statements

INSERT INTO Query4(SELECT cname, oname 
FROM (SELECT cname, oid 
FROM (SELECT cid, oid FROM oceanAccess
UNION
SELECT neighbor AS cid, oid 
FROM (SELECT cid, oid FROM oceanAccess) JOIN neighbour N ON N.country = cid) AS A JOIN country C ON A.cid = C.cid) AS B JOIN ocean O ON B.oid = O.oid
ORDER BY cname ASC, oname DESC);


-- Query 5 statements

INSERT INTO Query5((SELECT cid, cname, avghdi
FROM (SELECT cid, avg(hdi_score) AS avghdi 
FROM hdi 
WHERE year >= 2009 AND year <= 2013
GROUP BY cid
ORDER BY avghdi, DESC) AS A JOIN country C ON A.cid = C.cid) LIMIT 10);

-- Query 6 statements

CREATE VIEW Y1 AS (SELECT cid, hdi_score AS hdi1 FROM hdi WHERE year = 2009);
CREATE VIEW Y2 AS (SELECT cid, hdi_score AS hdi2 FROM hdi WHERE year = 2010);
CREATE VIEW Y3 AS (SELECT cid, hdi_score AS hdi3 FROM hdi WHERE year = 2011);
CREATE VIEW Y4 AS (SELECT cid, hdi_score AS hdi4 FROM hdi WHERE year = 2012);
CREATE VIEW Y5 AS (SELECT cid, hdi_score AS hdi5 FROM hdi WHERE year = 2013);

CREATE VIEW JY1 AS (SELECT * FROM country C JOIN Y1 ON C.cid = Y1.cid);
CREATE VIEW JY2 AS (SELECT * FROM JY1 JOIN Y2 ON JY1.cid = Y2.cid WHERE Y2.hdi2 > JY1.hd1 );
CREATE VIEW JY3 AS (SELECT * FROM JY2 JOIN Y3 ON JY2.cid = Y3.cid WHERE Y3.hdi3 > JY2.hdi2);
CREATE VIEW JY4 AS (SELECT * FROM JY3 JOIN Y4 ON JY3.cid = Y4.cid WHERE Y4.hdi4 > JY3.hdi3);

INSERT INTO Query6(SELECT cid, cname FROM JY4 JOIN Y5 ON JY4.cid = Y5.cid 
WHERE Y5.hdi5 > JY4.hdi4 ORDER BY cname ASC);

DROP VIEW Y1;
DROP VIEW Y2;
DROP VIEW Y3;
DROP VIEW Y4;
DROP VIEW Y5;

DROP VIEW JY1;
DROP VIEW JY2;
DROP VIEW JY3;
DROP VIEW JY4;

-- Query 7 statements

INSERT INTO Query7(SELECT rid, rname, sum(rpercentage*population) AS followers
FROM religion R JOIN country C ON R.cid = C.cid
GROUP BY rid, rname
ORDER BY followers DESC);

CREATE VIEW A AS(SELECT rid FROM religion R JOIN country C ON R.cid = C.cid GROUP BY rid);

-- Query 8 statements

CREATE VIEW POP AS (SELECT cid, lname, max(lpercentage) FROM language GROUP BY cid);
CREATE VIEW A AS (SELECT * FROM neighbour N JOIN POP ON N.country = POP.cid);
CREATE VIEW B AS (SELECT * FROM neighbour N JOIN POP ON N.neighbor = POP.cid);
CREATE VIEW RES AS (FROM A JOIN B ON A.country = B.country AND A.neighbor = B.neighbor AND A.lname = B.lname);
CREATE VIEW ALMOST AS (SELECT cname AS c1name, lname FROM country C JOIN RES ON C.cid = RES.country);

INSERT INTO Query8(SELECT c1name, cname AS c2name, lname FROM ALMOST JOIN country C ON ALMOST.neighbor = C.cid ORDER BY lname ASC, c1name DESC); 

DROP VIEW POP;
DROP VIEW A;
DROP VIEW B;
DROP VIEW RES;
DROP VIEW ALMOST;

-- Query 9 statements

CREATE VIEW depthpercountry AS (SELECT cid, max(depth) AS depth FROM oceanAccess O JOIN ocean OC ON O.oid = OC.oid GROUP BY (cid));

CREATE VIEW zerodepth AS (SELECT distinct cid, cname, 0 AS depth, height FROM country
WHERE cid NOT IN (SELECT cid FROM oceanAccess));

CREATE VIEW namewithdepth AS (SELECT C.cid, cname, depth FROM depthpercountry DC JOIN country C ON C.cid = DC.cid);

CREATE VIEW STUFF AS (SELECT namewithdepth.cid, namewithdepth.cname, depth, height FROM namewithdepth JOIN country C ON C.cid = namewithdepth.cid);

CREATE VIEW data AS (SELECT cname, (height + depth) AS totalspan FROM STUFF
UNION
SELECT cname, (height + depth) AS totalspan FROM zerodepth); 

INSERT INTO Query9(SELECT cname, totalspan FROM data WHERE totalspan >= ALL(SELECT totalspan FROM data));

DROP VIEW depthpercountry;
DROP VIEW zerodepth;
DROP VIEW namewithdepth;
DROP VIEW STUFF;
DROP VIEW data;

-- Query 10 statements

CREATE VIEW Longestborder AS (SELECT country, sum(length) AS borderslength FROM neighbour GROUP BY country HAVING sum(length) >= ALL(SELECT sum(length) FROM neighbour GROUP BY country));

INSERT INTO Query10(SELECT cname, borderslength FROM Longestborder L JOIN country C ON C.cid = L.country);

DROP VIEW Longestborder;
