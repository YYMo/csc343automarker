-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW highest AS
SELECT country, max(height) AS height
FROM neighbour n JOIN country c ON n.neighbor = c.cid
GROUP BY country
ORDER BY country ASC;

INSERT INTO Query1
(SELECT n.country AS c1id, c1.cname AS c1name, n.neighbor AS c2id, c2.cname AS c2name
FROM country c1 JOIN neighbour n ON (c1.cid = n.country) JOIN country c2 ON (c2.cid = n.neighbor) JOIN highest ON (c1.cid = highest.country)
WHERE c2.height = highest.height
ORDER BY c1name ASC);

DROP VIEW highest;

-- Query 2 statements
INSERT INTO Query2
((SELECT cid, cname
FROM country)
EXCEPT 
(SELECT c.cid, c.cname
FROM oceanAccess o JOIN country c on (o.cid = c.cid))
ORDER BY cname ASC);

-- Query 3 statements
CREATE VIEW landlocked AS
((SELECT cid
FROM country)
EXCEPT 
(SELECT c.cid
FROM oceanAccess o JOIN country c on (o.cid = c.cid)));

CREATE VIEW single AS
SELECT l.cid 
FROM landlocked l JOIN neighbour n on (l.cid = n.country) JOIN country c on (n.neighbor = c.cid)
GROUP BY l.cid 
HAVING count(n.neighbor) = 1;

INSERT INTO Query3
(SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name 
FROM country c1 JOIN neighbour n ON (c1.cid = n.country) JOIN country c2 ON (c2.cid = n.neighbor)
WHERE c1.cid in (SELECT * FROM single)
ORDER BY c1name ASC);

DROP VIEW single;
DROP VIEW landlocked;


-- Query 4 statements
CREATE VIEW direct AS
SELECT c.cname AS cname, oname
FROM country c JOIN oceanAccess oA ON (c.cid = oA.cid) JOIN ocean o ON (oA.oid = o.oid);

CREATE VIEW indirect AS
SELECT c.cname AS cname, oname
FROM country c JOIN neighbour n ON (c.cid = n.country)
JOIN oceanAccess oA ON (n.neighbor = oA.cid) JOIN ocean o ON (oA.oid = o.oid);

INSERT INTO Query4
((SELECT * FROM direct UNION SELECT * FROM indirect)
ORDER BY cname ASC, oname DESC);

DROP VIEW indirect;
DROP VIEW direct;

-- Query 5 statements
INSERT INTO Query5
(SELECT c.cid AS cid, cname, avg(hdi_score) AS avghdi
FROM country c JOIN hdi h ON (c.cid = h.cid)
WHERE year <= 2013 and year >= 2009
GROUP BY c.cid
ORDER BY avg(hdi_score) DESC
LIMIT 10);


-- Query 6 statements
CREATE VIEW notMonotonic AS
SELECT h1.cid AS cid
FROM hdi h1 JOIN hdi h2 ON (h1.cid = h2.cid)
WHERE 2009 <= h1.year and 2009 <= h2.year and 2013 >= h1.year and 2013 >= h2.year and h1.year > h2.year and h1.hdi_score <= h2.hdi_score;

CREATE VIEW monotonic AS
((SELECT cid FROM country) EXCEPT (SELECT * FROM notMonotonic))

INSERT INTO Query6
(SELECT m.cid AS cid, c.cname AS cname 
FROM monotonic m JOIN country c ON (m.cid = c.cid)
ORDER BY c.cname ASC);

DROP VIEW monotonic;
DROP VIEW notMonotonic;


-- Query 7 statements
CREATE VIEW religionPop AS
SELECT c.cid AS cid, rid, rname, rpercentage*population AS rpop
FROM religion r JOIN country c ON (c.cid = r.cid);

INSERT INTO Query7 
(SELECT rid, rname, sum(rpop) AS followers
FROM religionPop
GROUP BY rid, rname
ORDER BY sum(rpop) DESC);

DROP VIEW religionPop;


-- Query 8 statements
CREATE view neighbouring AS 
SELECT c.cid AS cid, x.cid AS nid, c.cname AS c1name, x.cname AS c2name 
FROM country c JOIN neighbour n ON n.country=c.cid JOIN country x ON n.neighbor=x.cid;

CREATE VIEW mostPopularLanguage AS 
SELECT cid, lid, lname 
FROM language l 
WHERE l.lpercentage >= ALL(SELECT l2.lpercentage FROM language l2 WHERE l2.cid=l.cid);

INSERT INTO Query8
(SELECT c1name, c2name, lname 
FROM neighbouring n JOIN mostPopularLanguage m ON n.cid=m.cid 
WHERE EXISTS(SELECT lid FROM mostPopularLanguage mpl WHERE mpl.cid=n.nid AND m.lid=mpl.lid)
ORDER BY lname ASC, c1name DESC);

DROP VIEW mostPopularLanguage;
DROP VIEW neighbouring;


-- Query 9 statements
CREATE VIEW landheight AS
((SELECT cname, height AS totalspan
FROM country)
EXCEPT 
(SELECT c.cname, c.height AS totalspan
FROM oceanAccess o JOIN country c on (o.cid = c.cid)));

CREATE VIEW oceanheight AS
(SELECT c.cname AS cname, c.height+o.depth AS totalspan
FROM oceanAccess oA JOIN country c on (oA.cid = c.cid) JOIN ocean o ON (o.oid = oA.oid));

INSERT INTO Query9
(SELECT * FROM landheight UNION SELECT * FROM oceanheight
ORDER BY totalspan DESC
LIMIT 1);

DROP VIEW oceanheight;
DROP VIEW landheight;

-- Query 10 statements

INSERT INTO Query10
(SELECT c.cname AS cname, sum(n.length) AS borderslength
FROM country c JOIN neighbour n ON c.cid = n.country
GROUP BY c.cname
ORDER BY sum(n.length) DESC
LIMIT 1);

