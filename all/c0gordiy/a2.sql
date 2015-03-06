-- Add below your SQL statements.
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.
 -- Query 1 statements

CREATE VIEW neighbour_heights AS
SELECT neighbour.country,
       neighbour.neighbor,
       country.height,
       country.cname nname
FROM neighbour
INNER JOIN country ON neighbour.neighbor=country.cid;


INSERT INTO query1
  ( SELECT d.c1id, c.cname c1name, d.c2id, d.c2name
   FROM country c
   INNER JOIN
     ( SELECT nh.country c1id, nh.neighbor c2id, nh.nname c2name
      FROM neighbour_heights nh
      INNER JOIN
        ( SELECT country, max(height) max_neighbour_height
         FROM neighbour_heights
         GROUP BY country ) mh ON nh.country=mh.country
      AND nh.height=mh.max_neighbour_height ) d ON c.cid=d.c1id
   ORDER BY d.c1id ASC);


DROP VIEW neighbour_heights;

 -- Query 2 statements

INSERT INTO query2
  ( SELECT country.cid, country.cname
   FROM country
   INNER JOIN
     ( SELECT distinct(cid)
      FROM country
      WHERE cid NOT IN
          ( SELECT distinct(cid)
           FROM oceanaccess ) ) dry ON country.cid=dry.cid
   ORDER BY country.cname ASC);

 -- Query 3 statements

INSERT INTO query3
  ( SELECT answer.c1id, answer.c1name, answer.c2id, d.cname c2name
   FROM country d
   INNER JOIN
     ( SELECT dry_neighbours.c1id, c.cname c1name, dry_neighbours.c2id
      FROM country c
      INNER JOIN
        ( SELECT n.country c1id, n.neighbor c2id
         FROM neighbour n
         INNER JOIN
           ( SELECT neighbour.country
            FROM neighbour, query2
            WHERE neighbour.country=query2.cid
            GROUP BY neighbour.country HAVING count(DISTINCT neighbour.neighbor)=1 ) ids ON n.country=ids.country ) dry_neighbours ON dry_neighbours.c1id=c.cid ) answer ON answer.c2id=d.cid
   ORDER BY answer.c1name ASC) ;

 -- Query 4 statements

INSERT INTO query4
  ( SELECT coasts.cname, ocean.oname
   FROM ocean
   INNER JOIN
     ( SELECT country.cname, waters.oid
      FROM country
      INNER JOIN
        ( SELECT *
         FROM oceanaccess
         UNION SELECT neighbour.country cid, water.oid
         FROM neighbour
         INNER JOIN
           (SELECT *
            FROM oceanaccess ) water ON neighbour.neighbor=water.cid ) waters ON waters.cid=country.cid ) coasts ON coasts.oid=ocean.oid
   ORDER BY ocean.oname DESC) ;

 -- Query 5 statements

INSERT INTO query5
  ( SELECT country.cid, country.cname, hdistats.avghdi
   FROM country
   INNER JOIN
     ( SELECT cid, avg(hdi_score) avghdi
      FROM hdi
      WHERE YEAR>=2009
        AND YEAR <=2013
      GROUP BY cid LIMIT 10 ) hdistats ON hdistats.cid=country.cid
   ORDER BY hdistats.avghdi DESC) ;

 -- Query 6 statements

CREATE VIEW target_years AS
SELECT cid,
       hdi_score,
       YEAR
FROM hdi
WHERE YEAR>=2009
  AND YEAR <=2013 ;


INSERT INTO query6
  ( SELECT country.cid, country.cname
   FROM country
   INNER JOIN
     ( SELECT a.cid, count(a.cid) positive_jumps
      FROM target_years a
      LEFT OUTER JOIN target_years b ON b.YEAR>a.YEAR
      LEFT OUTER JOIN target_years c ON c.YEAR<b.YEAR
      AND c.YEAR > a.YEAR
      WHERE c.YEAR IS NULL
        AND a.cid=b.cid
        AND b.hdi_score-a.hdi_score >0
      GROUP BY a.cid ) deltas ON deltas.cid=country.cid
   WHERE deltas.positive_jumps >=4
   ORDER BY country.cname ASC);


DROP VIEW target_years;

 -- Query 7 statements

INSERT INTO query7
  ( SELECT r.rid, r.rname, sum(r.followers) AS followers
   FROM
     ( SELECT religion.rid, religion.rname, (country.population * rpercentage) AS followers
      FROM country
      INNER JOIN
        ( SELECT *
         FROM religion ) religion ON religion.cid=country.cid
      ORDER BY followers DESC ) r
   GROUP BY r.rid, r.rname
   ORDER BY followers DESC);

 -- Query 8 statements

CREATE VIEW popular_languages AS
  ( SELECT l.cid,
           language.lid,
           l.most_common
   FROM LANGUAGE
   INNER JOIN
     ( SELECT cid,
              max(lpercentage) most_common
      FROM LANGUAGE
      GROUP BY cid ) l ON l.cid=LANGUAGE.cid
   AND LANGUAGE.lpercentage=l.most_common);


CREATE VIEW countries_with_shared_tongues AS
  ( SELECT popular_languages.cid c1,
                                 k.cid c2
   FROM popular_languages
   INNER JOIN
     ( SELECT *
      FROM popular_languages) k ON k.lid=popular_languages.lid
   WHERE k.cid!=popular_languages.cid);


INSERT INTO query8
  (SELECT *
   FROM countries_with_shared_tongues INTERSECT SELECT country c1, neighbor c2
   FROM neighbour);


DROP VIEW countries_with_shared_tongues;


DROP VIEW popular_languages;

 -- Query 9 statements
 -- Query 10 statements
