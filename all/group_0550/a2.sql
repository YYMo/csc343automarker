-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW candn 
AS 
  SELECT c.cid      AS c1id, 
         c.cname    AS c1name, 
         n.neighbor AS c2id 
  FROM   country c 
         join neighbour n 
           ON c.cid = n.country; 

CREATE VIEW withh 
AS 
  SELECT c.c1id, 
         c.c1name, 
         c.c2id, 
         co.cname AS c2name, 
         co.height 
  FROM   candn c 
         join country co 
           ON c.c2id = co.cid; 

CREATE VIEW maxheight 
AS 
  SELECT Max(height) AS maxH, 
         c1id 
  FROM   withh 
  GROUP  BY c1id; 

INSERT INTO query1 
(SELECT withh.c1id, 
        withh.c1name, 
        withh.c2id, 
        withh.c2name 
 FROM   maxheight 
        join withh 
          ON withh.c1id = maxheight.c1id 
             AND withh.height = maxheight.maxh 
 ORDER  BY c1name ASC); 

DROP VIEW maxheight; 

DROP VIEW withh; 

DROP VIEW candn; 

-- Query 2 statements
INSERT INTO query2 
(SELECT cid, 
        cname 
 FROM   country 
 WHERE  country.cid NOT IN (SELECT cid 
                            FROM   oceanaccess) 
 ORDER  BY cname ASC);

-- Query 3 statements
CREATE VIEW landlock 
AS 
  SELECT cid, 
         cname 
  FROM   country 
  WHERE  country.cid NOT IN (SELECT cid 
                             FROM   oceanaccess); 

CREATE VIEW temp 
AS 
  SELECT d5.c1id, 
         d5.c1name, 
         d5.c2id, 
         d5.cname AS c2name 
  FROM   ( country 
           join (SELECT c.cid      AS c1id, 
                        c.cname    AS c1name, 
                        n.neighbor AS c2id 
                 FROM   country c 
                        join neighbour n 
                          ON c.cid = n.country)d 
             ON country.cid = d.c2id)d5; 

CREATE VIEW noflandlock 
AS 
  SELECT c1id, 
         c1name, 
         c2id, 
         c2name 
  FROM   temp 
         join landlock 
           ON temp.c1id = landlock.cid; 

INSERT INTO query3 
(SELECT c1id, 
        c1name, 
        c2id, 
        c2name 
 FROM   noflandlock 
 WHERE  noflandlock.c1id IN (SELECT c1id 
                             FROM   (SELECT c1id, 
                                            Count(c2id) AS num 
                                     FROM   noflandlock 
                                     GROUP  BY c1id)d 
                             WHERE  d.num = 1) 
 ORDER  BY c1name ASC); 

DROP VIEW noflandlock; 

DROP VIEW temp; 

DROP VIEW landlock; 

-- Query 4 statements
CREATE VIEW directa 
AS 
  SELECT b.cname, 
         b.oname 
  FROM   ((oceanaccess o 
           join ocean 
             ON o.oid = ocean.oid)a 
          join country 
            ON a.cid = country.cid)b; 

CREATE VIEW indirecta 
AS 
  SELECT cname, 
         oname 
  FROM   ( ((SELECT DISTINCT b.neighbor, 
                            b.oid 
            FROM   ( (SELECT cid, 
                           oid 
                    FROM   oceanaccess)a 
                     join neighbour n 
                       ON a.cid = n.country)b)c 
            join country 
              ON c.neighbor = country.cid)d 
           join ocean 
             ON d.oid = ocean.oid); 

INSERT INTO query4 
((SELECT * 
  FROM   directa) 
 UNION 
 (SELECT * 
  FROM   indirecta) 
 ORDER  BY cname ASC, 
           oname DESC); 

DROP VIEW directa; 

DROP VIEW indirecta; 

-- Query 5 statements
CREATE VIEW ahdi AS 
SELECT   hdi.cid, 
         Avg(hdi_score) AS avghdi 
FROM     (country 
join     hdi 
ON       country.cid = hdi.cid) 
WHERE    2009 <= hdi.year 
AND      hdi.year <= 2013 
GROUP BY hdi.cid;
INSERT INTO query5 
            ( 
                     SELECT   ahdi.cid, 
                              country.cname, 
                              ahdi.avghdi 
                     FROM     (ahdi 
                     join     country 
                     ON       ahdi.cid = country.cid) 
                     ORDER BY ahdi.avghdi DESC limit 10 
            );
DROP VIEW ahdi; 

-- Query 6 statements
CREATE VIEW h 
AS 
  (SELECT hdi.cid       AS cid, 
          country.cname AS cname, 
          hdi.year      AS YEAR, 
          hdi_score     AS score 
   FROM   hdi 
          JOIN country 
            ON hdi.cid = country.cid); 

CREATE VIEW view1 
AS 
  (SELECT h1.cid, 
          h1.cname 
   FROM   h h1, 
          h h2 
   WHERE  h1.year = 2009 
          AND h2.year = 2010 
          AND h1.cid = h2.cid 
          AND h1.score < h2.score); 

CREATE VIEW view2 
AS 
  (SELECT h1.cid, 
          h1.cname 
   FROM   h h1, 
          h h2 
   WHERE  h1.year = 2010 
          AND h2.year = 2011 
          AND h1.cid = h2.cid 
          AND h1.score < h2.score); 

CREATE VIEW view3 
AS 
  (SELECT h1.cid, 
          h1.cname 
   FROM   h h1, 
          h h2 
   WHERE  h1.year = 2011 
          AND h2.year = 2012 
          AND h1.cid = h2.cid 
          AND h1.score < h2.score); 

CREATE VIEW view4 
AS 
  (SELECT h1.cid, 
          h1.cname 
   FROM   h h1, 
          h h2 
   WHERE  h1.year = 2012 
          AND h2.year = 2013 
          AND h1.cid = h2.cid 
          AND h1.score < h2.score); 

INSERT INTO query6 
(SELECT * 
 FROM   (view1 
         natural JOIN view2 
         natural JOIN view3 
         natural JOIN view4) order by cname ASC); 

DROP VIEW view4, view3, view2, view1, h; 

-- Query 7 statements
INSERT INTO query7
(SELECT rid,
        rname,
        SUM(population * rpercentage) AS followers
 FROM   ( country
          join religion
            ON country.cid = religion.cid)
 GROUP  BY rid,
           rname
 ORDER  BY followers DESC); 

-- Query 8 statements
CREATE VIEW popl
AS
  SELECT LANGUAGE.cid,
         LANGUAGE.lid,
         LANGUAGE.lpercentage,
         LANGUAGE.lname
  FROM   (LANGUAGE
          JOIN (SELECT cid,
                       Max(lpercentage) AS ml
                FROM   LANGUAGE
                GROUP  BY cid)a
            ON LANGUAGE.cid = a.cid
               AND LANGUAGE.lpercentage = a.ml);

CREATE VIEW poplwn
AS
  SELECT popl.cid   AS c1id,
         popl.lid,
         popl.lname,
         n.neighbor AS c2id
  FROM   popl
         JOIN neighbour n
           ON popl.cid = n.country;

CREATE VIEW popl2
AS
  SELECT poplwn.c1id,
         poplwn.c2id,
         poplwn.lname
  FROM   poplwn
         JOIN popl
           ON poplwn.c2id = popl.cid
  WHERE  poplwn.lid = popl.lid;

INSERT INTO query8
(SELECT a.c1name,
        country.cname AS c2name,
        a.lname
 FROM   (SELECT country.cname AS c1name,
                popl2.c2id,
                popl2.lname
         FROM   ( popl2
                  JOIN country
                    ON popl2.c1id = country.cid))a
        JOIN country
          ON a.c2id = country.cid
 ORDER  BY a.lname asc,
           a.c1name desc);

DROP VIEW popl2, poplwn, popl; 
-- Query 9 statements
CREATE VIEW t1
AS
  (SELECT country.cid     AS cid,
          country.cname   AS cname,
          country.height  AS height,
          oceanaccess.oid AS oid
   FROM   country
          JOIN oceanaccess
            ON country.cid = oceanaccess.cid);

CREATE VIEW t2
AS
  (SELECT t1.cid      AS cid,
          t1.cname    AS cname,
          t1.height   AS height,
          t1.oid      AS oid,
          ocean.oname AS oname,
          ocean.depth AS depth
   FROM   t1
          JOIN ocean
            ON t1.oid = ocean.oid);

CREATE VIEW maxt
AS
  SELECT cname,
         Max (height + depth) AS totalspan
  FROM   t2
  GROUP  BY cname
  UNION
  SELECT cname,
         height AS totalspan
  FROM   country
  WHERE  cid NOT IN (SELECT cid
                     FROM   oceanaccess)
  ORDER  BY totalspan desc;

CREATE VIEW maxtwr
AS
  SELECT cname,
         totalspan,
         RANK()
           OVER(
             ORDER BY totalspan desc) AS RANK
  FROM   maxt;

INSERT INTO query9
(SELECT cname,
        totalspan
 FROM   maxtwr
 WHERE  RANK = 1);

DROP VIEW maxtwr, maxt, t2, t1; 
-- Query 10 statements
CREATE VIEW temp AS
            (
                     SELECT   cname,
                              SUM(length) AS borderslength
                     FROM     country
                     join     neighbour
                     ON       country.cid = neighbour.country
                     GROUP BY country.cname
            );
INSERT INTO query10
            (
                   SELECT *
                   FROM   temp
                   WHERE  borderslength =
                          (
                                   SELECT   borderslength
                                   FROM     temp
                                   ORDER BY borderslength DESC limit 1)
            );
DROP VIEW temp;
