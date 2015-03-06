

-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW max_height(c2id, height) AS
        SELECT n.neighbor as c2id, max(height) as height
        FROM neighbour n, country c
        WHERE c.cid = n.neighbor
        GROUP BY n.neighbor;

CREATE VIEW find_c2name(c2id, c2name) AS
        SELECT m.c2id as c2id, c.cname as c2name
        FROM country c, max_height m
        WHERE c.cid = m.c2id;

CREATE VIEW find_c1id(c1id, c2id, c2name) AS
        SELECT n.country as c1id, f.c2id as c2id, f.c2name as c2name
        FROM neighbour n, find_c2name f
        WHERE n.neighbor = f.c2id;

INSERT INTO Query1(c1id, c1name, c2id, c2name)
        SELECT f.c1id as c1id, c.cname as c1name, f.c2id as c2id, f.c2name as c2name
        FROM country c, find_c1id f
        WHERE c.cid = f.c1id
        ORDER BY c1name ASC;

DROP VIEW max_height CASCADE;
DROP VIEW find_c2name CASCADE;
DROP VIEW find_c1name CASCADE;


-- Query 2 statements

INSERT INTO Query2(
        (SELECT cid, cname
        FROM country)
        EXCEPT
        (SELECT cid, cname
        FROM country NATURAL JOIN oceanAccess)
        ORDER BY cname ASC);



-- Query 3 statements

CREATE VIEW count_neighbor(c1id, count) AS
        SELECT q2.cid as c1id, count(n.neighbor) as count
        FROM query2 q2, neighbour n
        WHERE n.country = q2.cid
        GROUP BY q2.cid;

CREATE VIEW find_exactly1_neighbor(c1id, c2id) AS
        SELECT c.c1id as c1id, n.neighbor as c2id
        FROM neighbour n, count_neighbor c
        WHERE c.count = 1 AND c.c1id = n.country;

CREATE VIEW find_c1name(c1id, c1name, c2id) AS
        SELECT f.c1id as c1id, c.cname as c1name, f.c2id as c2id
        FROM country c, find_exactly1_neighbor f
        WHERE c.cid = f.c1id;
INSERT INTO Query3(
        SELECT f.c1id as c1id, f.c1name as c1name, f.c2id as c2id, c.cname as c2name
        FROM country c,find_c1name f
        WHERE c.cid = f.c2id
        ORDER BY c1name ASC);

DROP VIEW count_neighbor CASCADE;
DROP VIEW find_exactly1_neighbor CASCADE;
DROP VIEW find_c1name CASCADE;


-- Query 4 statements

INSERT INTO Query4(
        SELECT r.cname as cname, r.oname as oname
        FROM
                ((SELECT o1.cname as cname, o2.oname as oname
                FROM
                        (SELECT oa.cid as cid1, c.cname as cname, oa.oid
                        FROM country c, oceanAccess oa
                        WHERE oa.cid = c.cid) o1,
                        (SELECT oa.oid, o.oname as oname, oa.cid as cid2
                        FROM ocean o, oceanAccess oa
                        WHERE o.oid = oa.oid)o2
                WHERE o1.cid1 = o2.cid2)
                UNION
                (SELECT c1.cname as cname, c2.oname as oname
                FROM
                        (SELECT n.country, c.cname as cname, n.neighbor as neighbor
                        FROM country c, neighbour n
                        WHERE c.cid = n.country)c1,
                        (SELECT oa.oid, o.oname as oname, oa.cid as cid
                        FROM ocean o, oceanAccess oa
                        WHERE o.oid = oa.oid)c2
                WHERE c1.neighbor = c2.cid))r
		ORDER BY cname ASC, oname DESC);


-- Query 5 statements

INSERT INTO Query5(
        SELECT h.cid as cid, c.cname as cname, h.avghdi as avghdi
        FROM country c,
                (SELECT h.cid as cid, avg(hdi_score) as avghdi
                FROM hdi h
                WHERE h.year < 2014 AND h.year > 2008
                GROUP BY h.cid)h
        WHERE h.cid = c.cid
        ORDER BY avghdi DESC
        LIMIT 10);

-- Query 6 statements

-- Countries with cid, country name and hdi number.

CREATE VIEW countriesHDI(cid,cname,hdiNum) AS
SELECT c.cid as cid, c.cname as cname, hdi.hdiNum as hdiNum
FROM country c, hdiTable hdi
WHERE hdi.cid = c.cid and hdi.year >= 2009 and hdi.year <= 2013;

INSERT INTO Query6(
       SELECT DISTINCT c.cid, c.cname
       FROM country c, hdi hdi
       WHERE c.cid = hdi.cid and c.cid != all(
            SELECT DISTINCT c1.cid
            FROM countriesHDI c1, countriesHDI c2
            WHERE c1.cid =c2.cid and c1.year > c2.year and c1.hdiNum < c2.hdiNum
        )
       ORDER BY cname ASC
       );

DROP VIEW countriesHDI CASCADE;

-- Query 7 statements

CREATE VIEW countryreligion AS
SELECT c.cid as cid, r.rid as rid, r.rname as rname, r.rpercentage as rpercentage, c.population as population
FROM country c, religion r

CREATE VIEW countreligion AS
SELECT cr1.cid as cid, cr1.rid as rid, cr1.rpercentage*cr1.population as followers
FROM countryreligion cr1, countryreligion cr2
WHERE cr1.rid = cr2.rid and cr1.cid = cr2.cid 


INSERT INTO Query7(
       SELECT rid, rname, sum(countr.followers) as followers
       FROM countryreligion cr, countreligion countr
       WHERE cr.rid = countr.rid and cr.cid != countr.cid
       ORDER BY followers DESC
);

DROP VIEW countryreligion CASCADE;
DROP VIEW countreligion CASCADE;

-- Query 8 statements

CREATE VIEW languageNum AS
SELECT c.cid as cid, l.lname as lname, (l.lpercentage*c.population) as lnameNum, c.cname as cname
FROM country c, language l
WHERE c.cid = l.cid

CREATE VIEW popularLanguage AS
SELECT ln.lname as lname, ln.cid as cid, ln.cname as cname
FROM languageNum ln
WHERE ln.lnameNum = max(ln.lnameNum)

CREATE VIEW neighbourCountries AS
SELECT c1.cname as c1name, c2.cname as c2name
FROM country c1, country c2, neighbour nc
WHERE c1.cid = nc.country and c2.cid = nc.neighbor and c1.cid != c2.cid

CREATE VIEW countriesLanguage AS
SELECT pl.lname as lname, c.cname as c1name, nc.c2name as c2name
FROM neighbourCountries nc, country c, popularLanguage pl
WHERE  pl.cname = nc.c1name and pl.cname = nc.c2name

insert into Query8(
select c1name,c2name, lname
from countriesLanguage
ORDER BY lname ASC
ORDER BY c1name DESC
);

DROP VIEW languageNum CASCADE;
DROP VIEW popularLanguage CASCADE;
DROP VIEW neighbourCountries CASCADE;
DROP VIEW countriesLanguage CASCADE;

-- Query 9 statements

CREATE VIEW highestEP AS
SELECT c.cname as cname, c.height as ht
FROM country c

CREATE VIEW deepestO AS
SELECT c.cname as cname, o.depth as dp
FROM country c, ocean o, oceanAccess oA
WHERE o.oid = oA.oid and c.cid = oA.cid

CREATE VIEW total AS
SELECT c.cname as cname , ht.ht - dp.dp as totalspan
FROM highestEP ht, deepestO dp, country c
WHERE c.name = hr.cname and hr.cname = dp.cname

INSERT INTO Query9(
SELECT c.cname as cname, max(t.totalspan) as totalspan
FROM country c, highestEP h, deepestO d, total t
WHERE c.name = h.cname and h.cname = d.cname and h.cname = t.cname
);
 
DROP VIEW highestEP CASCADE;
DROP VIEW deepestO CASCADE;
DROP VIEW total CASCADE;
DROP VIEW deepestNone CASCADE;

-- Query 10 statements

-- countries with total border length
CREATE VIEW countryBL AS
SELECT nc.country as cid, sum(nc.length) as totalL
FROM neighbour nc

-- country with the max border length
CREATE VIEW maxcountryBL AS
SELECT max(cBL.totalL) as maxcBL
FROM countryBL cBL

INSERT INTO Query10(
      SELECT c.cname, cBL.totalL
      FROM country c, countryBL cBL, maxcountryBL mcBL
      WHERE c.cid = cBL.cid and cBL.totalL = mcBL.maxcBL
    
);

DROP VIEW countryBL CASCADE;
DROP VIEW maxcountryBL CASCADE;
