-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.
set search_path to a2;

DELETE FROM Query1;
DELETE FROM Query2;
DELETE FROM Query3;
DELETE FROM Query4;
DELETE FROM Query5;
DELETE FROM Query6;
DELETE FROM Query7;
DELETE FROM Query8;
DELETE FROM Query9;
DELETE FROM Query10;

-- Query 1 statements
DELETE FROM Query1;
CREATE VIEW highest as
SELECT country,  max(height) as highest
FROM neighbour, country
WHERE neighbor = cid
GROUP BY country;

CREATE VIEW highestNeighbour as
SELECT country, cid, n.cname, height
FROM (neighbour JOIN country ON neighbour.neighbor=country.cid) n
WHERE height=(SELECT highest FROM highest h where n.country=h.country);

INSERT INTO Query1 (select country, c.cname, h.cid, h.cname from highestNeighbour h join country c on h.country = c.cid ORDER BY c.cname ASC);

--INSERT INTO Query1 ((SELECT h.country, country.cname, h.cid, h.cname from highestNeighbour h, country where h.country=country.cid ));

DROP VIEW highest CASCADE;


-- Query 2 statements
DELETE FROM Query2;
CREATE VIEW Locked as
(SELECT cid
FROM country)
EXCEPT
(SELECT cid
FROM oceanAccess);

INSERT INTO Query2 (SELECT Locked.cid, cname
FROM Locked join country on Locked.cid=country.cid ORDER BY cname ASC);

DROP VIEW Locked CASCADE;


-- Query 3 statements
CREATE VIEW Locked as
(SELECT cid
FROM country)
EXCEPT
(SELECT cid
FROM oceanAccess);

CREATE VIEW oneNeighbour as
SELECT l.cid, max(n.neighbor) as neighbor
FROM Locked l JOIN Neighbour n on l.cid=n.country
GROUP BY l.cid
HAVING count(n.neighbor) = 1;

---SELECT * from oneNeighbour;

INSERT INTO Query3 (SELECT o.cid, c.cname, o.neighbor, c2.cname FROM (oneNeighbour o JOIN country c ON o.cid = c.cid) JOIN country c2 ON o.neighbor=c2.cid ORDER BY c.cname ASC);

DROP VIEW Locked CASCADE; 


-- Query 4 statements
CREATE VIEW Direct AS
SELECT c.cname, o.oname
FROM (oceanAccess a JOIN ocean o ON a.oid=o.oid) JOIN country c ON a.cid=c.cid; 

CREATE VIEW Indirect AS
SELECT n.country, o.oname
FROM (neighbour n JOIN oceanAccess a ON n.neighbor=a.cid) JOIN ocean o ON a.oid=o.oid;

INSERT INTO Query4 (SELECT * FROM DIRECT ORDER BY cname ASC, oname DESC) UNION (SELECT c.cname, i.oname FROM Indirect i JOIN country c ON i.country=c.cid ORDER BY c.cname ASC, i.oname DESC);

DROP VIEW Direct CASCADE;
DROP VIEW INDIRECT CASCADE;  

-- Query 5 statements
CREATE VIEW Year AS
SELECT h.cid, avg(hdi_score) as avghdi
FROM Hdi h JOIN country c ON h.cid=c.cid
WHERE h.year >= 2009 and h.year <= 2013
GROUP BY h.cid;


INSERT INTO Query5 (SELECT year.cid, cname, avghdi 
FROM Year JOIN country ON year.cid=country.cid
ORDER BY avghdi DESC
LIMIT 10);

DROP VIEW Year CASCADE;


-- Query 6 statements
CREATE VIEW Year5 AS
SELECT h.cid, c.cname, h.year, h.hdi_score
FROM Hdi h JOIN country c ON h.cid=c.cid
WHERE h.year >=2009 and h.year <= 2013
ORDER BY h.cid, h.year;

CREATE VIEW HDIincrease AS
SELECT y1.cid
FROM Year5 as y1, Year5 as y2
WHERE y1.cid = y2.cid and y1.year = y2.year - 1 and y1.hdi_score < y2.hdi_score
GROUP BY y1.cid
HAVING count(y1.cid) = 4;

INSERT into Query6 (SELECT c.cid, cname FROM HDIincrease h JOIN country c ON h.cid = c.cid ORDER BY c.cname ASC);
----
--CREATE VIEW INCREASING AS
--SELECT y1.cid, y1.cname
--FROM Year5 y1, Year5 y2, Year5 y3, Year5 y4, Year5 y5
--WHERE y1.cid = y2.cid and y2.cid = y3.cid and y3.cid = y4.cid and y4.cid = y5.cid and
  --   y1.hdi_score < y2.hdi_score and y2.hdi_score < y3.hdi_score and y3.hdi_score < y4.hdi_score and y4.hdi_score < y5.hdi_score;

--INSERT INTO Query6 (SELECT * FROM Increasing ORDER BY cname);

DROP VIEW YEAR5 CASCADE;


-- Query 7 statements
CREATE VIEW Quantity AS
SELECT rid, sum((c.population * rpercentage)) as followers
FROM Religion r JOIN country c ON r.cid=c.cid
GROUP BY rid;
 

INSERT INTO Query7 (SELECT q.rid, rname, followers
FROM Quantity q JOIN religion r ON q.rid = r.rid
ORDER BY followers DESC);


DROP VIEW Quantity CASCADE;


-- Query 8 statements
CREATE VIEW Popular AS
SELECT cid, lname
FROM Language L1
WHERE lpercentage = (SELECT max(lpercentage) FROM Language L2 GROUP BY cid HAVING L1.cid=L2.cid);

---select * from Popular;

CREATE VIEW Pairs AS
SELECT n.country, n.neighbor, p.lname
FROM (Neighbour n JOIN Popular p ON n.country=p.cid) JOIN Popular p2 ON n.neighbor=p2.cid
WHERE p.lname = p2.lname;

INSERT INTO Query8 (SELECT c1.cname, c2.cname, p.lname
FROM (PAIRS p JOIN country c1 ON p.country = c1.cid) JOIN country c2 ON p.neighbor = c2.cid ORDER BY lname ASC, c1.cname DESC);

DROP VIEW Popular CASCADE;

-- Query 9 statements
CREATE VIEW dAccess AS
SELECT cid, max(depth) as max_depth
FROM oceanAccess a JOIN ocean o ON a.oid=o.oid
GROUP BY cid;

CREATE VIEW nAccess AS
(SELECT cid
FROM country) EXCEPT
(SELECT c.cid
FROM country c JOIN oceanAccess a ON c.cid=a.cid);

CREATE VIEW Zero AS
SELECT cid, 0 as max_depth
FROM nAccess;

CREATE VIEW Depth AS
(SELECT * FROM dAccess) UNION (SELECT * FROM Zero);

CREATE VIEW Difference AS
SELECT c.cid, c.cname, (height + max_depth) as totalspan
FROM country c JOIN Depth d on c.cid=d.cid;

INSERT INTO Query9 (SELECT cname, totalspan FROM Difference WHERE totalspan = (SELECT max(totalspan) FROM Difference));

DROP VIEW dAccess CASCADE;
DROP VIEW nAccess CASCADE;   

 


-- Query 10 statements
CREATE VIEW Lengths AS
SELECT country, sum(length) as borderslength
FROM Neighbour 
GROUP BY country;

INSERT INTO Query10 (SELECT c.cname, L.borderslength FROM Lengths L JOIN country c ON L.country=c.cid 
			WHERE L.borderslength = (SELECT max(borderslength) FROM Lengths));

DROP VIEW Lengths CASCADE;

