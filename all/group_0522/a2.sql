

-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1(
SELECT c1id, c1name, c2id, c2name
FROM
     (SELECT c1id, cname as c1name, highest
      FROM
           (SELECT country as c1id, max(height) as highest
            FROM country, neighbour
            WHERE neighbor = cid
            GROUP BY c1id) as a, country                
       WHERE c1id = cid) b
       INNER JOIN
       (SELECT country, neighbor as c2id, cname as c2name, height
        FROM neighbour, country
        WHERE neighbor = cid) c
ON highest = height and c1id = country
ORDER BY c1name ASC);
             
             
-- Query 2 statements
INSERT INTO Query2(
SELECT cid, cname 
FROM country 
WHERE cid <> ALL
    (SELECT cid
     FROM oceanAccess
     GROUP BY cid)
     ORDER BY cname ASC);


-- Query 3 statements

-- the country -> 1. no border with ocean
-- and 2. only has one neighbor --> it's currounded by the neighbor country
INSERT INTO Query3(
SELECT c1id, c1name, c2id, cname as c2name
FROM
    (SELECT c1id, cname as c1name, neighbor as c2id
    FROM
        ((SELECT cid as c1id FROM country 
          WHERE cid <> ALL (SELECT cid FROM oceanAccess GROUP BY cid))
          INTERSECT
          (SELECT country as c1id FROM neighbour 
           GROUP BY country HAVING count(neighbor) = 1)) a, country, neighbour
    WHERE c1id = cid and c1id = country) b, country
WHERE c2id = cid
ORDER BY c1name ASC);


-- Query 4 statements
INSERT INTO Query4(
SELECT cname, oname
FROM
(SELECT cname, oid
FROM
(SELECT cid, oid FROM oceanAccess
UNION
--countries whose neighbor has oceanaccess
(SELECT DISTINCT country as cid, oid
FROM neighbour, oceanAccess
WHERE neighbor = cid)) a
INNER JOIN country
ON a.cid = country.cid) b
INNER JOIN ocean
ON b.oid = ocean.oid
ORDER BY cname ASC, oname DESC);


-- Query 5 statements
INSERT INTO Query5(
SELECT b.cid, cname, avghdi
FROM
   (SELECT cid, avg(hdi_score) as avghdi
   FROM
    (SELECT * FROM hdi WHERE year >= 2009
     INTERSECT
     SELECT * FROM hdi WHERE year <= 2013) a
    GROUP BY cid) b 
    INNER JOIN country
    ON b.cid = country.cid
ORDER BY avghdi DESC LIMIT 10);

-- Query 6 statements
INSERT INTO Query6(
SELECT c.cid, cname FROM
(SELECT DISTINCT cid FROM
(SELECT * FROM hdi WHERE year >= 2009
INTERSECT
SELECT * FROM hdi WHERE year <= 2013) t
WHERE t.cid NOT IN
(SELECT a.cid FROM
    (SELECT * FROM hdi WHERE year >= 2009
     INTERSECT
     SELECT * FROM hdi WHERE year <= 2013) a
     INNER JOIN
     (SELECT * FROM hdi WHERE year >= 2009
     INTERSECT
     SELECT * FROM hdi WHERE year <= 2013) b
     ON a.cid = b.cid and a.year < b. year and a.hdi_score > b.hdi_score)) c, country
WHERE c.cid = country.cid
ORDER BY cname ASC);

-- Query 7 statements
INSERT INTO Query7(
SELECT rid, rname, sum(cfollor) followers FROM
(SELECT country.cid, rid, rname, (rpercentage* population) as cfollor
FROM country, religion
WHERE country.cid = religion.cid) a
GROUP BY rid, rname
ORDER BY followers DESC);


-- Query 8 statements
-- find pair of cid's with same popular language
-- find whether they are neighbours
INSERT INTO Query8(
SELECT cA.cname c1name, cB.cname c2name, lname FROM
(SELECT e.c1id id1, e.c2id id2, lname FROM
(SELECT c.country c1id, d.country c2id, c.lname FROM
(SELECT DISTINCT country, lname,lpercentage FROM
    (SELECT cid, lname, lpercentage
        FROM language
        WHERE lpercentage IN 
            (SELECT MAX(lpercentage) FROM language GROUP BY cid)) a, neighbour
    WHERE country = cid) c
INNER JOIN
(SELECT DISTINCT country, lname FROM
    (SELECT cid, lname
        FROM language
        WHERE lpercentage IN 
            (SELECT MAX(lpercentage) FROM language GROUP BY cid)) b, neighbour
    WHERE country = cid) d
ON c.lname = d.lname and c.country < d.country) e, neighbour
WHERE e.c1id = country and e.c2id = neighbor) f, country cA, country cB
WHERE f.id1 = cA.cid and f.id2 = cB.cid
ORDER BY lname ASC, c1name DESC);


-- Query 9 statements
INSERT INTO Query9(
SELECT cname, max(height + depth) totalspan
FROM country, oceanAccess, ocean
WHERE country.cid = oceanAccess.cid and ocean.oid = oceanAccess.oid
GROUP BY country.cname
UNION
SELECT cname, (height + 0) totalspan FROM
(SELECT cid, height, cname FROM country WHERE cid NOT IN (SELECT cid FROM oceanAccess)) a);


-- Query 10 statements
INSERT INTO Query10(
SELECT cname, borderslength FROM
(SELECT country, sum(length) borderslength
FROM neighbour
GROUP BY country
HAVING sum(length)  =
    (SELECT max(totallength) FROM 
        (SELECT country, sum(length) totallength FROM neighbour GROUP BY country) a))b, country
WHERE country = cid);

