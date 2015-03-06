-- Add below your SQL statements.
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW table1 AS
  SELECT C1.cid AS cid1, C1.cname AS name1, C1.height AS height1, C2.cid AS cid2, C2.cname AS name2, C2.height AS height2
  FROM country AS C1, neighbour AS N1, country AS C2
  WHERE C1.cid=N1.country AND C2.cid=N1.neighbor;

INSERT INTO Query1 (
  SELECT T1.cid1 AS c1id, T1.name1 AS c1name, T1.cid2 AS c2id, T1.name2 AS c2name
  FROM table1 AS T1, (SELECT cid1, MAX(height2) AS max FROM table1 GROUP BY cid1) AS A1
  WHERE T1.cid1=A1.cid1 AND T1.height2=A1.max
  ORDER BY T1.name1
);

-- Query 2 statements
INSERT INTO Query2 (
  SELECT country.cid, country.cname
  FROM country
  WHERE country.cid NOT IN(SELECT cid FROM oceanaccess)
);

-- Query 3 statements
CREATE VIEW table3 AS
(SELECT country.cid, country.cname
FROM country
WHERE country.cid NOT IN(SELECT cid FROM oceanaccess))
INTERSECT
(SELECT cid, cname
FROM country
WHERE (SELECT COUNT(neighbour.neighbor) FROM neighbour WHERE neighbour.country=country.cid)=1
);

INSERT INTO Query3 (
  SELECT T3.cid AS c1id, T3.cname AS c1name, C1.cid AS c2id, C1.cname AS c2name
  FROM table3 AS T3, country AS C1, neighbour AS N1
  WHERE T3.cid=N1.country AND C1.cid=N1.neighbor
);

-- Query 4 statements
CREATE VIEW table4 AS
(SELECT C1.cid, C1.cname, O.oname
FROM oceanAccess AS O1, country AS C1, ocean AS O
WHERE O1.cid=C1.cid AND O1.oid=O.oid);

CREATE VIEW table4b AS
((SELECT * FROM table4)
UNION
(SELECT C1.cid, C1.cname, O1.oname
FROM country AS C1, neighbour AS N1, country AS C2, ocean AS O1
WHERE C1.cid=N1.country AND N1.neighbor=C2.cid AND (N1.neighbor, C2.cname, O1.oname) IN (SELECT * FROM table4))
);

INSERT INTO Query4 (
  SELECT cname, oname
  FROM table4b
  ORDER BY cname ASC, oname DESC
);

-- Query 5 statements
CREATE VIEW table5 AS (
  SELECT cid, AVG(hdi_score) AS avghdi
  FROM hdi
  WHERE 2009<=year AND year<=2013
  GROUP BY cid
  ORDER BY AVG(hdi_score) DESC
  LIMIT 10
);

INSERT INTO Query5 (
  SELECT C1.cid, C1.cname, T.avghdi
  FROM country C1, table5 T
  WHERE C1.cid=T.cid
);

-- Query 6 statements
INSERT INTO Query6 (SELECT hdi5.cid, C1.cname
FROM hdi AS hdi5, country AS C1
WHERE hdi5.cid = C1.cid AND year=2013 AND hdi_score > (SELECT hdi4.hdi_score FROM hdi AS hdi4 WHERE year=2012 AND hdi4.cid=hdi5.cid) AND
                                                     (SELECT hdi4.hdi_score FROM hdi AS hdi4 WHERE year=2012 AND hdi4.cid=hdi5.cid) >
                                                     (SELECT hdi3.hdi_score FROM hdi AS hdi3 WHERE year=2011 AND hdi3.cid=hdi5.cid) AND
                                                     (SELECT hdi3.hdi_score FROM hdi AS hdi3 WHERE year=2011 AND hdi3.cid=hdi5.cid) >
                                                     (SELECT hdi2.hdi_score FROM hdi AS hdi2 WHERE year=2010 AND hdi2.cid=hdi5.cid) AND
                                                     (SELECT hdi2.hdi_score FROM hdi AS hdi2 WHERE year=2010 AND hdi2.cid=hdi5.cid) >
                                                     (SELECT hdi1.hdi_score FROM hdi AS hdi1 WHERE year=2009 AND hdi1.cid=hdi5.cid)
ORDER BY C1.cname);

-- Query 7 statements
CREATE VIEW table7 AS (
  SELECT R1.rid, SUM(C1.population*R1.rpercentage) AS followers
  FROM religion AS R1, country AS C1
  WHERE R1.cid=C1.cid
  GROUP BY R1.rid
);

INSERT INTO Query7 (
  SELECT T7.rid, R1.rname, T7.followers
  FROM table7 AS T7, religion AS R1
  WHERE T7.rid=R1.rid
  ORDER BY T7.followers DESC
);


-- Query 8 statements
CREATE VIEW table8 AS (SELECT L1.cid, L1.lname
FROM language AS L1
WHERE L1.lpercentage >= ALL(SELECT L2.lpercentage FROM language AS L2 WHERE L2.cid=L1.cid));

INSERT INTO Query8 (
  SELECT C1.cname AS c1name, C2.cname AS c2name, T1.lname
  FROM table8 AS T1, table8 AS T2, neighbour, country AS C1, country AS C2
  WHERE C1.cid=neighbour.country AND C2.cid=neighbour.neighbor AND T1.cid=C1.cid AND T2.cid=C2.cid AND T1.lname=T2.lname
  ORDER BY T1.lname ASC, C1.cname DESC
);

-- Query 9 statements
CREATE VIEW table9a AS (SELECT country.cid, MAX(ocean.depth) AS maxdepth
FROM country, oceanAccess, ocean
WHERE country.cid=oceanAccess.cid AND ocean.oid=oceanAccess.oid
GROUP BY country.cid);

INSERT INTO Query9 (
(SELECT country.cname, country.height+table9a.maxdepth AS totalspan
  FROM table9a, country
  WHERE table9a.cid=country.cid)
  UNION
  (SELECT country.cname, country.height AS totalspan
    FROM country
    WHERE country.cid NOT IN (SELECT table9a.cid FROM table9a)));

-- Query 10 statements
CREATE VIEW table10 AS (
  SELECT country AS cid, SUM(length) AS length
  FROM neighbour
  GROUP BY country
);

INSERT INTO Query10 (
  SELECT C1.cname, T10.length AS borderslength
  FROM country C1, table10 T10
  WHERE T10.length=(SELECT MAX(length) FROM table10) AND C1.cid=T10.cid
);