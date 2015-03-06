-- Add below your SQL statements.
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW heights AS
SELECT N.country AS c1id, MAX(C.height) AS elevation
FROM country C, neighbour N
WHERE N.neighbor = C.cid
GROUP BY N.country;

INSERT INTO Query1 (
SELECT DISTINCT C1.cid AS c1id, C1.cname AS c1name, C2.cid AS c2id, C2.cname AS c2name
FROM heights H, country C1, country C2, neighbour N
WHERE H.elevation = C2.height AND N.neighbor = C2.cid AND H.c1id = C1.cid
ORDER BY C1.cname ASC);

DROP VIEW heights;

-- Query 2 statements

INSERT INTO Query2 (
SELECT C.cid, C.cname
FROM country C
WHERE C.cid NOT IN (
  SELECT DISTINCT O.cid
  FROM oceanAccess O
)
ORDER BY C.cname);

-- Query 3 statements

CREATE VIEW oneneighbour AS
SELECT N.country AS country, COUNT(N.neighbor) AS neighbours
FROM neighbour N
WHERE N.country NOT IN (
    SELECT cid FROM oceanAccess
)
GROUP BY N.country
HAVING COUNT(N.neighbor) = 1;

CREATE VIEW neighbourcountries AS
SELECT O.country AS c1id, N.neighbor AS c2id
FROM oneneighbour O, neighbour N
WHERE O.country = N.country;

INSERT INTO Query3 (
SELECT NC.c1id AS c1id, C1.cname AS c1name, NC.c2id AS c2id, C2.cname AS c2name
FROM neighbourcountries NC, country C1, country C2
WHERE NC.c1id = C1.cid AND NC.c2id = C2.cid
ORDER BY C1.cname ASC);

DROP VIEW neighbourcountries;
DROP VIEW oneneighbour;

-- Query 4 statements

INSERT INTO Query4 (
SELECT C.cname AS cname, O.oname AS oname
FROM country C, ocean O, oceanAccess OA
WHERE C.cid = OA.cid AND O.oid = OA.oid
UNION
SELECT C.cname AS cname, O.oname AS oname
FROM country C, neighbour N, ocean O, oceanAccess OA
WHERE N.country = C.cid AND OA.cid = N.neighbor AND O.oid = OA.oid
ORDER BY cname ASC, oname DESC);

-- Query 5 statements

CREATE VIEW avghdi AS
SELECT cid, AVG(hdi_score) AS avghdi
FROM hdi
WHERE year >= 2009 AND year <= 2013
GROUP BY cid
ORDER BY AVG(hdi_score) DESC
LIMIT 10;

INSERT INTO Query5 (
SELECT A.cid AS cid, C.cname AS cname, A.avghdi AS avghdi
FROM avghdi A, country C
WHERE A.cid = C.cid
ORDER BY A.avghdi DESC
);

DROP VIEW avghdi;

-- Query 6 statements

CREATE VIEW hdicountries AS
SELECT h09.cid AS cid
FROM hdi h09, hdi h10, hdi h11, hdi h12, hdi h13
WHERE h09.year = 2009 AND h10.year = 2010 AND h11.year = 2011 AND h12.year = 2012
      AND h13.year = 2013 AND h09.hdi_score < h10.hdi_score AND h10.hdi_score <
      h11.hdi_score AND h11.hdi_score < h12.hdi_score AND h12.hdi_score <
      h13.hdi_score AND h09.cid = h10.cid AND h10.cid = h11.cid AND h11.cid =
      h12.cid AND h12.cid = h13.cid;

INSERT INTO Query6 (
SELECT H.cid AS cid, C.cname AS cname
FROM hdicountries H, country C
WHERE H.cid = C.cid
ORDER BY C.cname ASC);

DROP VIEW hdicountries;

-- Query 7 statements

CREATE VIEW relfollowers AS
SELECT R.rid AS rid, SUM(R.rpercentage*C.population) AS followers
FROM religion R, country C
WHERE R.cid = C.cid
GROUP BY R.rid;

INSERT INTO Query7 (
SELECT R.rid AS rid, R.rname AS rname, RF.followers AS followers
FROM relfollowers RF, (
  SELECT DISTINCT rid, rname
  FROM religion
) R
WHERE RF.rid = R.rid
ORDER BY RF.followers DESC
);

DROP VIEW relfollowers;

-- Query 8 statements

CREATE VIEW poplangper AS
SELECT cid, MAX(lpercentage) AS popper
FROM language
GROUP BY cid;

CREATE VIEW poplang AS
SELECT C.cname AS cname, L.lname AS lname
FROM country C, poplangper PL, language L
WHERE C.cid = PL.cid AND PL.popper = L.lpercentage AND PL.cid = L.cid;

INSERT INTO Query8 (
SELECT P1.cname AS c1name, P2.cname AS c2name, P1.lname AS lname
FROM poplang P1, poplang P2
WHERE P1.cname <> P2.cname AND P1.lname = P2.lname
ORDER BY P1.lname ASC, c1name DESC
);

DROP VIEW poplang;
DROP VIEW poplangper;

-- Query 9 statements

CREATE VIEW oceanDepths AS
SELECT OA.cid AS cid, MAX(O.depth) AS depth
FROM ocean O, oceanAccess OA
WHERE O.oid = OA.oid
GROUP BY OA.cid;

CREATE VIEW diffWithOcean AS
SELECT C.cname AS cname, O.depth + C.height AS totalspan
FROM oceanDepths O, country C
WHERE O.cid = C.cid;

CREATE VIEW diffWithoutOcean AS
SELECT C.cname AS cname, C.height AS totalspan
FROM (
    SELECT *
    FROM country
    WHERE cid NOT IN (
        SELECT DISTINCT O.cid
        FROM oceanAccess O
    )
) C;

INSERT INTO Query9 (
    SELECT X.cname, MAX(X.totalspan) AS totalspan
    FROM (
        SELECT * FROM diffWithOcean
        UNION
        SELECT * FROM diffWithoutOcean
    ) X
    WHERE X.totalspan = (
        SELECT MAX(Y.totalspan)
        FROM (
            SELECT * FROM diffWithOcean
            UNION
            SELECT * FROM diffWithoutOcean
        ) Y
    )
    GROUP BY X.cname
);

DROP VIEW diffWithoutOcean;
DROP VIEW diffWithOcean;
DROP VIEW oceanDepths;

-- Query 10 statements

CREATE VIEW borders AS
SELECT C.cname AS cname, SUM(N.length) AS length
FROM country C, neighbour N
WHERE C.cid = N.country
GROUP BY C.cname;

INSERT INTO Query10 (
    SELECT B.cname AS cname, MAX(B.length) AS borderslength
    FROM borders B
    WHERE B.length = (
        SELECT MAX(length)
        FROM borders
    )
    GROUP BY B.cname
);

DROP VIEW borders;
