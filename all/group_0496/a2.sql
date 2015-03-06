-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW countryNeighbour AS
SELECT c2.cid AS c1id, c2.cname AS c1name, c.cid AS c2id, c.cname AS c2name, c.height AS height
FROM neighbour n JOIN country c ON n.neighbor = c.cid 
JOIN country c2 ON n.country = c2.cid;

CREATE VIEW countryHeight AS
SELECT c1id,MAX(height) AS height
FROM countryNeighbour
GROUP BY c1id;

INSERT INTO Query1
SELECT cn.c1id, c1name, c2id, c2name
FROM countryNeighbour cn JOIN countryHeight ch
ON cn.height = ch.height AND cn.c1id = ch.c1id
ORDER BY c1name ASC;

DROP VIEW countryHeight;
DROP VIEW countryNeighbour;

-- Query 2 statements
INSERT INTO Query2 
(SELECT cid,cname 
FROM country 
WHERE cid NOT IN 
                  (SELECT cid 
                  FROM oceanAccess))
ORDER BY cname ASC;

-- Query 3 statements
CREATE VIEW landlock AS 
SELECT cid AS c1id, cname AS c1name
FROM country WHERE cid NOT IN 
                               (SELECT cid 
                               FROM oceanAccess);

CREATE VIEW landlockCN AS 
SELECT c1id, c1name, cid AS c2id, cname AS c2name
FROM ((landlock JOIN neighbour n ON landlock.c1id = n.country) 
JOIN country c ON c.cid = n.neighbor);

CREATE VIEW landlockMoreThan1 AS 
SELECT DISTINCT x1.c1id AS c1id 
FROM landlockCN x1 WHERE x1.c2id <> ANY
                                        (SELECT x2.c2id 
                                         FROM landlockCN x2 
                                         WHERE x1.c1id = x2.c1id); 

INSERT INTO Query3 (
SELECT c1id, c1name, c2id, c2name 
FROM landlockCN 
WHERE c1id NOT IN 
                  (SELECT l2.c1id 
                  FROM landlockMoreThan1 l2))
ORDER BY c1name ASC;

DROP VIEW landlockMoreThan1;
DROP VIEW landlockCN;
DROP VIEW landlock;

-- Query 4 statements
CREATE VIEW access AS 
(SELECT * 
FROM oceanAccess) 
UNION 
		(SELECT country AS cid, oid
		FROM (oceanAccess JOIN neighbour ON neighbour.neighbor = oceanAccess.cid));

INSERT INTO Query4
SELECT DISTINCT cname,oname 
FROM ((access JOIN country ON country.cid = access.cid) 
JOIN ocean ON access.oid = ocean.oid) 
ORDER BY cname ASC, oname DESC;

DROP VIEW access;

-- Query 5 statements
CREATE VIEW AVGHDI AS 
SELECT  AVG(hdi_score) AS avghdi, cid 
FROM hdi WHERE year >= 2009 AND year <= 2013 
GROUP BY cid;

INSERT INTO Query5
SELECT AVGHDI.cid AS cid, cname, avghdi 
FROM (AVGHDI JOIN country ON AVGHDI.cid = country.cid) 
ORDER BY avghdi DESC 
LIMIT 10;

DROP VIEW AVGHDI;

-- Query 6 statements
CREATE VIEW countries AS
SELECT h.cid AS cid , cname, h.year AS year
FROM (country c JOIN hdi h ON c.cid = h.cid)
WHERE h.year >= 2009 AND h.year <= 2013;

INSERT INTO Query6
(SELECT DISTINCT cid,cname 
FROM countries 
WHERE cid NOT IN 
(SELECT h1.cid FROM (hdi h1 JOIN hdi h2 
							    ON (h1.year >= 2009 AND h1.year <= 2013 AND
							    h2.year >= 2009 AND h2.year <= 2013 AND
							    h1.cid = h2.cid AND 
								h1.year < h2.year AND 
								h1.hdi_score >= h2.hdi_score))))
ORDER BY cname ASC;

DROP VIEW countries;

-- Query 7 statements
CREATE VIEW religionCountry AS 
SELECT rid,rname,(rpercentage * population) AS followers 
FROM (religion JOIN country ON religion.cid = country.cid);

INSERT INTO Query7
SELECT rid,rname,SUM(followers) AS followers 
FROM religionCountry 
GROUP BY rid,rname 
ORDER BY followers DESC;

DROP VIEW religionCountry;

-- Query 8 statements
CREATE VIEW mostPopPercents AS
SELECT cid , MAX(lpercentage) as lpercentage
FROM language 
GROUP BY cid;

CREATE VIEW mostPopLang AS
SELECT l.cid AS cid ,lname
FROM language l JOIN mostPopPercents p 
ON (p.lpercentage = l.lpercentage AND l.cid = p.cid);
 
CREATE VIEW countryLangTuple AS
SELECT p1.cid AS cid1, p2.cid AS cid2, p1.lname AS lname
FROM (mostPopLang p1 JOIN neighbour n1 ON p1.cid = n1.country) 
JOIN mostPopLang p2 ON p2.cid = n1.neighbor
WHERE p1.lname = p2.lname AND p1.cid <> p2.cid; 

INSERT INTO Query8
SELECT c1.cname AS c1name ,c2.cname AS c2name, lname
FROM countryLangTuple tuple JOIN country c1 ON c1.cid = tuple.cid1 
JOIN country c2 ON c2.cid = tuple.cid2
ORDER BY lname ASC, c1name DESC;

DROP VIEW countryLangTuple;
DROP VIEW mostPopLang;
DROP VIEW mostPopPercents;

-- Query 9 statements
CREATE VIEW countrySpan AS
SELECT DISTINCT c.cname AS cname,  (c.height + o.depth) AS totalspan
FROM ((country c JOIN oceanAccess oa ON c.cid = oa.cid)
JOIN ocean o ON oa.oid = o.oid)
UNION
	SELECT cname, height AS totalspan
	FROM country 
	WHERE cid NOT IN 
                     (SELECT cid 
                     FROM oceanAccess);

INSERT INTO Query9
SELECT * 
FROM countrySpan 
WHERE totalspan >= ALL
(SELECT totalspan 
FROM countrySpan);

DROP VIEW countrySpan;

-- Query 10 statements
CREATE VIEW countryLength AS
SELECT country AS cid, SUM(length) AS borderslength
FROM neighbour
GROUP BY country;

INSERT INTO Query10
SELECT c.cname AS cname, cl.borderslength AS borderslength
FROM countryLength cl JOIN country c ON cl.cid = c.cid
WHERE borderslength >= ALL
                           (SELECT borderslength 
                            FROM countryLength);

DROP VIEW countryLength;


