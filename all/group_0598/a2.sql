-- Add below your SQL statements. hehes
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW highest(c1id,c2highest) AS
SELECT c1.cid,
       max(c2.height)
FROM country c1,
     country c2,
     neighbour n
WHERE c1.cid=n.country
      AND c2.cid=n.neighbor
GROUP BY c1.cid;

INSERT INTO Query1(
SELECT c1id,
       c1.cname AS c1name,
       c2.cid AS c2id,
       c2.cname AS c2name
FROM highest h,
     country c1,
     country c2
WHERE c1.cid = h.c1id
  AND c2.height = h.c2highest
ORDER BY c1name ASC
);

DROP VIEW highest;

-- Query 2 statements
CREATE VIEW landlocked AS
SELECT cid
FROM country
WHERE cid NOT IN (SELECT cid
                  FROM oceanAccess
                 );

INSERT INTO Query2(
    SELECT c.cid,
           c.cname
    FROM landlocked l,
         country c
    WHERE c.cid=l.cid
    ORDER BY c.cname ASC
);

DROP VIEW landlocked;

-- Query 3 statements
CREATE VIEW landlocked AS
SELECT cid
FROM country
WHERE cid NOT IN (SELECT cid
                  FROM oceanAccess
                 );

CREATE VIEW onesurrounding(c1id) AS
SELECT l.cid
FROM landlocked l,
     neighbour n
WHERE l.cid = n.country
GROUP BY l.cid
HAVING count(*) = 1;

INSERT INTO Query3(
    SELECT c1.cid AS c1id,
           c1.cname AS c1name,
           c2.cid AS c2id,
           c2.cname AS c2name
    FROM onesurrounding o,
         country c1,
         country c2,
         neighbour n
    WHERE c1id = c1.cid
          AND c1id=country
          AND neighbor=c2.cid
    ORDER BY c1name ASC
);

DROP VIEW onesurrounding;
DROP VIEW landlocked;

-- Query 4 statements
CREATE VIEW indirectid(cid,oid) AS
SELECT n.neighbor,
       o.oid
FROM oceanAccess o,
     neighbour n
WHERE o.cid=n.country;

CREATE VIEW indirect(cname,oname) AS
SELECT c.cname,
       o.oname
FROM indirectid i,
     ocean o,
     country c
WHERE i.cid=c.cid
  AND i.oid=o.oid;

CREATE VIEW direct(cname,oname) AS
SELECT c.cname,
       o.oname
FROM oceanAccess oa,
     ocean o,
     country c
WHERE oa.cid=c.cid
  AND oa.oid=o.oid;

INSERT INTO Query4(
    SELECT * FROM indirect
    UNION
    SELECT * FROM direct
    ORDER BY cname ASC,
             oname DESC
);

DROP VIEW direct;
DROP VIEW indirect;
DROP VIEW indirectid;

-- Query 5 statements
CREATE VIEW fiveyear(cid,avghdi) AS
SELECT cid,
       avg(hdi_score)
FROM hdi
WHERE year>=2009
  AND year<=2013
GROUP BY cid;

INSERT INTO Query5(
    SELECT c.cid,
           c.cname,
           avghdi
    FROM fiveyear y,
         country c
    WHERE c.cid = y.cid
    ORDER BY avghdi DESC
    LIMIT 10
);

DROP VIEW fiveyear;

-- Query 6 statements
CREATE VIEW year09 AS
SELECT cid,
       hdi_score
FROM hdi
WHERE year=2009;

CREATE VIEW year10 AS
SELECT cid,
       hdi_score
FROM hdi
WHERE year=2010;

CREATE VIEW year11 AS
SELECT cid,
       hdi_score
FROM hdi
WHERE year=2011;

CREATE VIEW year12 AS
SELECT cid,
       hdi_score
FROM hdi
WHERE year=2012;

CREATE VIEW year13 AS
SELECT cid,
       hdi_score
FROM hdi
WHERE year=2013;

CREATE VIEW increase AS
SELECT y1.cid
FROM year09 y1,
     year10 y2
WHERE y1.cid=y2.cid
  AND y1.hdi_score<y2.hdi_score
INTERSECT
SELECT y2.cid
FROM year10 y2,
     year11 y3
WHERE y2.cid=y3.cid
  AND y2.hdi_score<y3.hdi_score
INTERSECT
SELECT y3.cid
FROM year11 y3,
     year12 y4
WHERE y3.cid=y4.cid
  AND y3.hdi_score<y4.hdi_score
INTERSECT
SELECT y4.cid
FROM year12 y4,
     year13 y5
WHERE y4.cid=y5.cid
  AND y4.hdi_score<y5.hdi_score;

INSERT INTO Query6(
    SELECT c.cid,
           c.cname
    FROM increase i,
         country c
    WHERE i.cid=c.cid
    ORDER BY c.cname ASC
);

DROP VIEW increase;
DROP VIEW year09;
DROP VIEW year10;
DROP VIEW year11;
DROP VIEW year12;
DROP VIEW year13;

-- Query 7 statements
CREATE VIEW followers(cid,rid,num) AS
SELECT r.cid,
       r.rid,
       rpercentage*population
FROM religion r,
     country c
WHERE r.cid=c.cid;

INSERT INTO Query7(
    SELECT f.rid,
           r.rname,
           sum(num) AS followers
    FROM followers f,
         religion r
    WHERE f.rid=r.rid
    GROUP BY f.rid,
             r.rname
    ORDER BY followers DESC
);

DROP VIEW followers;

-- Query 8 statements
CREATE VIEW mpl(cid,maxpercentage) AS
SELECT cid,
       max(lpercentage)
FROM language
GROUP BY cid;

CREATE VIEW maxl AS
SELECT m.cid,
       lid,
       lname
FROM mpl m,
     language l
WHERE m.cid=l.cid
      AND lpercentage=maxpercentage;

CREATE VIEW pair(c1id,c2id,c1name,c2name,lname) AS
SELECT c1.cid,
       c2.cid,
       c1.cname,
       c2.cname,
       m1.lname
FROM maxl m1,
     maxl m2,
     country c1,
     country c2
WHERE c1.cid=m1.cid
  AND c2.cid=m2.cid
  AND c1.cid!=c2.cid
  AND m1.lname=m2.lname;

INSERT INTO Query8(
SELECT c1name,
       c2name,
       lname
FROM pair p,
     neighbour n
WHERE country=c1id
  AND neighbor=c2id
ORDER BY lname ASC,
         c1name DESC
);

DROP VIEW pair;
DROP VIEW maxl;
DROP VIEW mpl;

-- Query 9 statements
CREATE VIEW deepest(cid,depth) AS
SELECT oa.cid,
       max(o.depth)
FROM ocean o,
     oceanAccess oa
WHERE oa.oid=o.oid
GROUP BY oa.cid;

CREATE VIEW alldepth AS
SELECT p.cid,
       coalesce(d.depth, 0) AS depth
FROM country p
     LEFT JOIN deepest d ON p.cid=d.cid;

CREATE VIEW span(cname,totalspan) AS
SELECT c.cname,
       c.height+a.depth
FROM alldepth a,
     country c
WHERE a.cid=c.cid;

INSERT INTO Query9(
    SELECT cname,
           totalspan
    FROM span
    WHERE totalspan = (SELECT max(totalspan)
                       FROM span
                      )
);

DROP VIEW span;
DROP VIEW alldepth;
DROP VIEW deepest;

-- Query 10 statements

CREATE VIEW border(cid, borderslength) AS
SELECT country,
       sum(length)
FROM neighbour
GROUP BY country;

INSERT INTO Query10(
    SELECT c.cname,
           borderslength
    FROM border b,
         country c
    WHERE b.cid=c.cid
          AND borderslength=(SELECT max(borderslength)
                             FROM border
                            )
);

DROP VIEW border;
