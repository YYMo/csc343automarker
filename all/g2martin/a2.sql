-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Example of creating a view:
-- CREATE VIEW topkek AS
-- SELECT lel,pls
-- FROM gooby
-- WHERE keks > 100

-- Query 1 statements
INSERT INTO Query1(
SELECT c.cid c1id, c.cname c1name, c2.cid c2id, c2.cname c2name
FROM country c
    LEFT JOIN neighbour n ON n.country=c.cid
    LEFT JOIN country c2 ON c2.cid=n.neighbor
WHERE c2.cid =
    (SELECT ns.cid
     FROM neighbour ne LEFT JOIN country ns ON ns.cid=ne.neighbor
     WHERE ne.country=c.cid
     ORDER BY ns.height DESC
     LIMIT 1)
ORDER BY c1name ASC
);

-- DROP VIEW ...



-- Query 2 statements
INSERT INTO Query2(
SELECT c.cid, c.cname
FROM country c
WHERE c.cid NOT IN (SELECT cid FROM oceanAccess)
ORDER BY cname ASC
);

-- DROP VIEW ...



-- Query 3 statements
CREATE VIEW Landlocked AS
SELECT c.cid
FROM country c
WHERE c.cid NOT IN (SELECT cid FROM oceanAccess)
;

CREATE VIEW OneNeighbour AS
SELECT country cid
FROM neighbour n
WHERE n.country NOT IN
    (SELECT n1.country
     FROM neighbour n1 LEFT JOIN neighbour n2 ON n2.country=n1.country
                                            AND n2.neighbor<>n1.neighbor)
;

CREATE VIEW Surrounded AS
SELECT cid FROM Landlocked
INTERSECT
SELECT cid FROM OneNeighbour
;

INSERT INTO Query3(
SELECT c1.cid c1id, c1.cname c1name, c2.cid c2id, c2.cname c2name
FROM country c1
    LEFT JOIN neighbour n ON n.country=c1.cid
    LEFT JOIN country c2 ON c2.cid=n.neighbor
WHERE c1.cid IN (SELECT cid FROM Surrounded)
ORDER BY c1name ASC
);

DROP VIEW Landlocked, OneNeighbour, Surrounded
;



-- Query 4 statements
CREATE VIEW AccOceanCountries AS
SELECT DISTINCT n.country cid, oa.oid
FROM oceanAccess oa
    LEFT JOIN neighbour n ON n.neighbor=oa.cid
UNION
SELECT DISTINCT o.cid, o.oid
FROM oceanAccess o
;

INSERT INTO Query4(
SELECT c.cname,o.oname
FROM AccOceanCountries aoc
    LEFT JOIN country c ON c.cid=aoc.cid
    LEFT JOIN ocean o ON o.oid=aoc.oid
ORDER BY cname ASC, oname DESC
);

DROP VIEW AccOceanCountries;



-- Query 5 statements
INSERT INTO Query5(
SELECT c.cid, c.cname, AVG(hdi_score) avghdi
FROM country c
    LEFT JOIN hdi h ON h.cid=c.cid
WHERE h.year >= 2009 AND h.year <= 2013
GROUP BY c.cid, c.cname
ORDER BY avghdi DESC
LIMIT 10
);

-- DROP VIEW ...



-- Query 6 statements
CREATE VIEW OneNonIncreasing AS
SELECT DISTINCT h1.cid
FROM hdi h1
    LEFT JOIN hdi h2 ON h2.cid=h1.cid
                        AND h2.year > h1.year
                        AND h2.hdi_score <= h1.hdi_score
WHERE h1.year >= 2009 AND h1.year <= 2013
;

CREATE VIEW AlwaysIncreasing AS
SELECT h.cid
FROM hdi h
WHERE h.cid NOT IN (SELECT cid FROM OneNonIncreasing)
;

INSERT INTO Query6(
SELECT c.cid, c.cname
FROM country c
WHERE c.cid IN (SELECT cid FROM AlwaysIncreasing)
ORDER BY cname ASC
);

DROP VIEW OneNonIncreasing, AlwaysIncreasing
;



-- Query 7 statements
INSERT INTO Query7(
SELECT r.rid, r.rname, SUM(r.rpercentage * c.population) followers
FROM religion r
    LEFT JOIN country c ON c.cid=r.cid
GROUP BY r.rid,r.rname
ORDER BY followers DESC
);

-- DROP VIEW ...



-- Query 8 statements
CREATE VIEW MostPopularLangs AS
SELECT c.cid, c.cname, l.lname
FROM country c
    LEFT JOIN language l ON l.cid=c.cid
WHERE l.lid =
    ( SELECT lang.lid
      FROM language lang
      WHERE lang.cid=c.cid
      ORDER BY lang.lpercentage DESC
      LIMIT 1 )
;

INSERT INTO Query8(
SELECT mpl1.cname c1name, mpl2.cname c2name, mpl1.lname lname
FROM MostPopularLangs mpl1
    INNER JOIN MostPopularLangs mpl2 ON mpl2.lname=mpl1.lname
                                        AND mpl2.cid > mpl1.cid
ORDER BY lname ASC, c1name DESC
);

DROP VIEW MostPopularLangs
;



-- Query 9 statements
CREATE VIEW DeepestOcean AS
SELECT c.cid, c.cname, MAX(c.height) elevation, MAX(o.depth) deepest
FROM country c
    LEFT JOIN oceanAccess oa ON oa.cid=c.cid
    LEFT JOIN ocean o ON o.oid=oa.oid
GROUP BY c.cid, c.cname
;

INSERT INTO Query9(
SELECT doc.cname cname,
    (doc.elevation +
      (CASE WHEN doc.deepest IS NULL THEN 0
            ELSE doc.deepest
       END)
    ) totalspan
FROM DeepestOcean doc
ORDER BY totalspan DESC
LIMIT 1
);

DROP VIEW DeepestOcean
;



-- Query 10 statements
CREATE VIEW BorderLength AS
SELECT c.cid, c.cname, SUM(n.length) borderslength
FROM country c
    LEFT JOIN neighbour n ON n.country=c.cid
GROUP BY c.cid, c.cname
;

INSERT INTO Query10(
SELECT bl.cname cname, bl.borderslength borderslength
FROM BorderLength bl
ORDER BY bl.borderslength DESC
LIMIT 1
);

DROP VIEW BorderLength
;
