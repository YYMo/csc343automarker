--1

CREATE VIEW max_neighbor AS
SELECT c1.cname as c1name,max(c2.height) as height
FROM country as c1,country as c2,neighbour as n
WHERE c1.cid = n.country AND c2.cid = n.neighbor
GROUP BY c1name
ORDER BY c1name ASC;

INSERT INTO Query1(
  SELECT c1.cid as c1id,c1.cname as c1name,c2.cid as c2id,c2.cname as c2name
  FROM country as c1,country as c2,neighbour as n
  WHERE c1.cid = n.country AND c2.cid = n.neighbor AND (c1.cname,c2.height) IN (
    select *
    from max_neighbor
  )
  ORDER BY c1name ASC
);

DROP VIEW max_neighbor;

--2
INSERT INTO Query2(
  select c1.cid, c1.cname
  from country as c1
  where c1.cid not in (select cid from oceanAccess)
  order by cname ASC
);
--3
CREATE VIEW landlocked AS
SELECT a2.cid as cid, a2.cname as cname
FROM oceanAccess a1 FULL JOIN country a2
ON a1.cid = a2.cid
WHERE a1.oid IS NULL;

CREATE VIEW landlockedNeighbour AS
SELECT a1.cid as c1id, a1.cname as c1name, a2.neighbor as c2id
FROM landlocked a1, neighbour a2
WHERE a1.cid = a2.country;

INSERT INTO Query3(
  select c1.cid as c1id, c1.cname as c1name, c3.cid as c2id, c3.cname as c2name
  from
  (select a1.c1id as c1id, a1.c1name as c1name, a1.c2id as c2id
   from landlockedNeighbour as a1
   JOIN
   (select c1id
    from landlockedNeighbour
    group by c1id
    having count(c2id) = 1
   ) as a2 on a1.c1id = a2.c1id
  ) as n1
   JOIN
   country as c1 on n1.c1id = c1.cid JOIN country as c3 on n1.c2id = c3.cid
   order by c1name asc
);

DROP VIEW landlockedNeighbour;
DROP VIEW landlocked;
--4
CREATE VIEW nb as
  SELECT n.country,c1.cname as name1,n.neighbor,c2.cname as name2,o.oid,o.oname
  FROM neighbour as n,oceanAccess as oa,country as c1,country as c2,ocean as o
  WHERE n.country=oa.cid AND c1.cid=n.country AND c2.cid=n.neighbor AND o.oid=oa.oid;

INSERT INTO Query4(
  SELECT nb.name1 as cname,nb.oname
  FROM nb
  UNION
  SELECT nb.name2,nb.oname
  FROM nb
  ORDER BY cname ASC,oname DESC
);
DROP VIEW nb;
--5
INSERT INTO Query5(
  select newhdi.cid, cname, avg(newhdi.hdi_score) as avghdi
  from (select cid, hdi_score from hdi where year >= 2009 and year <= 2013) AS newhdi JOIN country on newhdi.cid = country.cid
  group by newhdi.cid,cname
  order by avghdi desc
  LIMIT 10
);

--6 --FIX

CREATE VIEW recentHDI as
SELECT *
FROM hdi
WHERE hdi.year >= 2009 AND hdi.year <= 2013;

INSERT INTO Query6(
  SELECT DISTINCT hdi.cid as cid,country.cname as cname
  FROM hdi,country
  WHERE hdi.cid=country.cid AND hdi.cid NOT IN(
    SELECT h1.cid
    FROM recentHDI as h1,recentHDI as h2
    WHERE h1.cid=h2.cid AND h1.year > h2.year AND h1.hdi_score<=h2.hdi_score
  )
  ORDER BY cname ASC
);

DROP VIEW recentHDI;
--7 --WRONG --FIXED

CREATE VIEW popul AS
SELECT r1.cid AS cid, r1.rid AS rid, r1.rname AS rname, r1.rpercentage AS rpercentage, (c.population * r1.rpercentage) AS foll_by
FROM religion as r1, country as c
WHERE r1.cid = c.cid;

INSERT INTO Query7(
  SELECT rid, rname, sum(foll_by) AS followers
  FROM popul
  GROUP BY rid, rname
  ORDER BY followers DESC
);

DROP VIEW popul;


-- 8
CREATE VIEW country_lang AS
SELECT l1.cid cid, l1.lname lname
FROM (SELECT cid, MAX(lpercentage) maxtable
      FROM language
      GROUP BY cid) AS t,language as l1
WHERE t.cid = l1.cid AND t.maxtable = l1.lpercentage;

INSERT INTO query8
SELECT c1.cname c1name, c2.cname c2name, m1.lname lname
FROM neighbour n, country_lang m1, country_lang m2, country c1, country c2
WHERE n.country = m1.cid AND n.neighbor = m2.cid AND m1.lname=m2.lname AND
      c1.cid = n.country AND c2.cid = n.neighbor
ORDER BY lname ASC, c1name DESC;

DROP VIEW country_lang;
--9

CREATE VIEW difference as
  SELECT c.cid,c.cname,abs(c.height - o.depth)
  FROM country as c,oceanAccess as oa,ocean as o
  WHERE c.cid=oa.cid AND oa.oid=o.oid AND(c.cid,o.oid) NOT IN (
    SELECT oa1.cid,oa2.oid
    FROM oceanAccess as oa1,oceanAccess as oa2,
          ocean as o1,ocean as o2
    WHERE oa1.cid=oa2.cid AND oa1.oid!=oa2.oid
    AND oa1.oid=o1.oid AND oa2.oid=o2.oid
    AND o1.depth < o2.depth
    ORDER BY oa1.cid
  );
INSERT INTO Query9(
  SELECT c.cname,c.height as totalspan
  FROM country as c
  WHERE c.cid NOT IN(
    SELECT cid
    FROM difference
  )
  UNION
  SELECT cname,abs as totalspan
  FROM difference
  ORDER BY totalspan DESC
  LIMIT 1
);
DROP VIEW difference;
--10
INSERT INTO Query10(
  select c1.cname, sum(n1.length) as borderslength
  from neighbour as n1 join country as c1 on n1.country = c1.cid
  group by n1.country,c1.cname
  order by borderslength desc
  LIMIT 1
);
