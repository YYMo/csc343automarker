-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW nheights AS(
    SELECT N.country, N.neighbor, C.height
    FROM neighbour N, country C
    WHERE C.cid = N.neighbor);

CREATE VIEW comparison AS(
    SELECT A.*
    FROM nheights A LEFT OUTER JOIN nheights B
    ON A.country = B.country AND A.height < B.height
    WHERE B.height IS NULL ORDER BY A.country);

INSERT INTO QUERY1(
    SELECT A.country AS c1id, B.cname AS c1name,
           A.neighbor AS c2id, C.cname AS c2name
    FROM comparison A, country B, country C
    WHERE A.country = B.cid AND A.neighbor = C.cid
    ORDER BY B.cname
    );

DROP VIEW comparison;
DROP VIEW nheights;


-- Query 2 statements

CREATE VIEW LandlockedID AS(
    SELECT cid FROM country
    WHERE cid NOT IN(
    SELECT cid FROM oceanAccess));

INSERT INTO QUERY2(
    SELECT C.cid AS cid, C.cname AS cname
    FROM LandlockedID L, country C
    WHERE L.cid = C.cid
    ORDER BY cname ASC);

DROP VIEW LandlockedID;

-- Query 3 statements

CREATE VIEW LandlockedID AS(
    SELECT cid FROM country
    WHERE cid NOT IN(
    SELECT cid FROM oceanAccess));

CREATE VIEW ImperialistGerrymandering AS(
    SELECT L.cid
    FROM LandlockedID L, neighbour N
    WHERE L.cid = N.country
    GROUP BY L.cid
    HAVING count(DISTINCT N.neighbor) = 1
    );

CREATE VIEW Surrounded AS(
    SELECT N.country, N.neighbor
    FROM ImperialistGerrymandering G, Neighbour N
    WHERE G.cid = N.country
    );

INSERT INTO QUERY3(
    SELECT A.cid AS c1id, A.cname AS c1name,
           B.cid AS c2id, B.cname AS c2name
    FROM country A, Surrounded S, country B
    WHERE A.cid = S.country AND B.cid = S.neighbor
    ORDER BY A.cname
    );

DROP VIEW Surrounded;
DROP VIEW ImperialistGerrymandering;
DROP VIEW LandlockedID;

-- Query 4 statements


CREATE VIEW IndirectAccess AS(
    SELECT N.neighbor AS cid, A.oid AS oid 
    FROM oceanAccess A, neighbour N
    WHERE A.cid = N.country
    );

CREATE VIEW AllAccess AS(
    (SELECT cid, oid FROM IndirectAccess)
    UNION
    (SELECT cid, oid FROM oceanAccess)
    );

INSERT INTO QUERY4(
    SELECT C.cname AS cname, S.oname AS oname
    FROM AllAccess A, country C, ocean S
    WHERE A.cid = C.cid AND A.oid = S.oid
    ORDER BY cname ASC, oname DESC);


DROP VIEW AllAccess;
DROP VIEW IndirectAccess;

-- Query 5 statements

CREATE VIEW Averages AS(
    SELECT H.cid AS cid, avg(hdi_score) AS avg
    FROM hdi H
    WHERE (year>2008) AND (year<2014)
    GROUP BY H.cid
    ORDER BY avg DESC
    LIMIT 10
    );

INSERT INTO QUERY5(
    SELECT A.cid AS cid, C.cname AS cname, A.avg AS avghdi
    FROM Averages A, country C
    WHERE A.cid=C.cid
    ORDER BY avghdi DESC
    );

DROP VIEW Averages;

-- Query 6 statements

CREATE VIEW A2009 AS(
    SELECT * FROM hdi WHERE year=2009);

CREATE VIEW A2010 AS(
    SELECT * FROM hdi WHERE year=2010);

CREATE VIEW A2011 AS(
    SELECT * FROM hdi WHERE year=2011);

CREATE VIEW A2012 AS(
    SELECT * FROM hdi WHERE year=2012);

CREATE VIEW A2013 AS(
    SELECT * FROM hdi WHERE year=2013);

CREATE VIEW net AS(
    SELECT A2009.cid AS cid
    FROM A2009, A2010, A2011, A2012, A2013
    WHERE A2009.cid = A2010.cid AND
          A2010.cid = A2011.cid AND
          A2011.cid = A2012.cid AND
          A2012.cid = A2013.cid AND
          
          A2009.hdi_score < A2010.hdi_score AND
          A2010.hdi_score < A2011.hdi_score AND
          A2011.hdi_score < A2012.hdi_score AND
          A2012.hdi_score < A2013.hdi_score
    );

INSERT INTO QUERY6(
    SELECT N.cid AS cid, C.cname AS cname
    FROM net N, country C
    WHERE N.cid = C.cid
    ORDER BY cname ASC
    );


DROP VIEW net;
DROP VIEW A2013;
DROP VIEW A2012;
DROP VIEW A2011;
DROP VIEW A2010;
DROP VIEW A2009;


-- Query 7 statements

INSERT INTO QUERY7(
    SELECT R.rid AS rid,
           R.rname AS rname,
           sum((C.population * R.rpercentage)) AS followers
    FROM country C, religion R
    WHERE C.cid = R.cid
    GROUP BY rid, rname
    ORDER BY followers DESC
    );

-- Query 8 statements

CREATE VIEW MostCommon AS(
    SELECT A.cid, B.lname, A.topcent FROM
    (SELECT cid, max(lpercentage) AS topcent
    FROM language
    GROUP BY cid) AS A
    INNER JOIN
    (SELECT cid, lname, lpercentage
    FROM language) AS B
    ON B.lpercentage = A.topcent
    AND B.cid = A.cid
    );

INSERT INTO QUERY8(

    SELECT M.cname AS c1name, N.cname AS c2name, lname
    FROM Country M, Country N,
    (SELECT X.cid AS c1id, Y.cid AS c2id, X.lname AS lname
    FROM MostCommon X, MostCommon Y
    WHERE X.lname = Y.lname AND
    (X.cid, Y.cid) IN (SELECT country, neighbor FROM neighbour)) AS P 
    WHERE M.cid = P.c1id AND N.cid = P.c2id
    ORDER BY P.lname ASC, c1name DESC
    );

DROP VIEW MostCommon;

-- Query 9 statements

CREATE VIEW Nocean AS(
    (SELECT cid FROM country)
    EXCEPT
    (SELECT cid FROM oceanAccess)
    );

CREATE VIEW NoceanHeights AS(
    SELECT C.cname AS cname, C.height AS totalspan
    FROM Nocean N, country C
    WHERE N.cid = C.cid);

CREATE VIEW OceanHeights AS(
    SELECT C.cname AS cname, (C.height + S.depth) AS totalspan
    FROM oceanAccess A, ocean S, country C
    WHERE A.cid=C.cid AND A.oid=S.oid);

CREATE VIEW AllHeights AS(
    (SELECT * FROM NoceanHeights)
    UNION
    (SELECT * FROM OceanHeights)
    );

INSERT INTO QUERY9(
    SELECT cname, max(totalspan) AS totalspan
    FROM AllHeights
    GROUP BY cname
    );

DROP VIEW AllHeights;
DROP VIEW OceanHeights;
DROP VIEW NoceanHeights;
DROP VIEW Nocean;


-- Query 10 statements

CREATE VIEW BorderLengths AS(
    SELECT country, sum(length) AS border
    FROM neighbour 
    GROUP BY country);

CREATE VIEW MaxLength AS(
    SELECT country, border
    FROM BorderLengths
    WHERE border IN
    (SELECT max(border)
    FROM BorderLengths));

INSERT INTO QUERY10(
    SELECT C.cname AS cname, B.border AS borderslength
    FROM MaxLength B, country C
    WHERE B.country = C.cid
    GROUP BY C.cname, borderslength);

DROP VIEW MaxLength;   
DROP VIEW BorderLengths;
