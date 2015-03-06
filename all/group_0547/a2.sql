-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

INSERT INTO Query1 (SELECT c1, cname as c1name, c2, c2name FROM (SELECT country as c1, cid as c2, cname as c2name FROM country A JOIN neighbour B on A.cid = B.neighbor WHERE height IN (SELECT max(height) FROM (SELECT * FROM country A JOIN neighbour B on A.cid = B.neighbor) as neighbours)) as X JOIN country as Y ON X.c1 = Y.cid ORDER BY c1name ASC);

-- Query 2 statements

INSERT INTO Query2 (SELECT cid, cname FROM country WHERE cid NOT IN (SELECT DISTINCT cid FROM oceanAccess) ORDER BY cname ASC);


-- Query 3 statements

INSERT INTO Query3 (SELECT N.cid as c1id, N.cname as c1name, O.cid as c2id, O.cname as c2name FROM (SELECT * FROM country as C JOIN (SELECT A.country, neighbor FROM (SELECT country FROM neighbour GROUP BY country HAVING count(*) = 1) as A JOIN neighbour as B on A.country = B.country WHERE A.country NOT IN (SELECT cid FROM country WHERE cid IN (SELECT DISTINCT cid FROM oceanAccess))) as R ON C.cid = R.country) as N JOIN country O on N.neighbor = O.cid ORDER BY c1name ASC);


-- Query 4 statements

INSERT INTO Query4 (SELECT cname, oname FROM country Y JOIN (SELECT country, oname FROM (SELECT country, neighbor, oid FROM (SELECT country, neighbor FROM NEIGHBOUR WHERE country IN (SELECT DISTINCT cid FROM oceanAccess) OR neighbor IN (SELECT DISTINCT cid FROM oceanAccess)) as A JOIN oceanAccess as B on A.country = B.cid) as C JOIN ocean O on C.oid = O.oid UNION SELECT neighbor as country, oname FROM (SELECT country, neighbor, oid FROM (SELECT country, neighbor FROM NEIGHBOUR WHERE country IN (SELECT DISTINCT cid FROM oceanAccess) OR neighbor IN (SELECT DISTINCT cid FROM oceanAccess)) as A JOIN oceanAccess as B on A.country = B.cid) as C JOIN ocean O on C.oid = O.oid) Z ON Y.cid = Z.country ORDER BY cname ASC, oname DESC);


-- Query 5 statements

INSERT INTO Query5 (SELECT country.cid, cname, avghdi FROM (SELECT cid, avg(hdi_score) as avghdi FROM (SELECT * FROM (SELECT country.cid, cname, year, hdi_score FROM country JOIN hdi ON country.cid = hdi.cid) as A WHERE year > 2008 AND year < 2014) AS Z GROUP BY cid) as R JOIN country ON R.cid = country.cid ORDER BY avghdi DESC LIMIT 10);


-- Query 6 statements

INSERT INTO Query6 (SELECT country.cid, country.cname FROM (SELECT AA.cid FROM (SELECT cid, year as year1, hdi_score as hdi_score1 FROM (SELECT country.cid, year, hdi_score FROM country JOIN hdi ON country.cid = hdi.cid WHERE year < 2014 AND year > 2008) as A WHERE year = 2009) as AA JOIN (SELECT cid, year as year2, hdi_score as hdi_score2 FROM (SELECT country.cid, year, hdi_score FROM country JOIN hdi ON country.cid = hdi.cid WHERE year < 2014 AND year > 2008) as A WHERE year = 2010) as BB ON AA.cid = BB.cid JOIN (SELECT cid, year as year3, hdi_score as hdi_score3 FROM (SELECT country.cid, year, hdi_score FROM country JOIN hdi ON country.cid = hdi.cid WHERE year < 2014 AND year > 2008) as A WHERE year = 2011) as CC ON BB.cid = CC.cid JOIN (SELECT cid, year as year4, hdi_score as hdi_score4 FROM (SELECT country.cid, year, hdi_score FROM country JOIN hdi ON country.cid = hdi.cid WHERE year < 2014 AND year > 2008) as A WHERE year = 2012) as DD ON CC.cid = DD.cid JOIN (SELECT cid, year as year5, hdi_score as hdi_score5 FROM (SELECT country.cid, year, hdi_score FROM country JOIN hdi ON country.cid = hdi.cid WHERE year < 2014 AND year > 2008) as A WHERE year = 2013) as EE ON DD.cid = EE.cid WHERE (hdi_score1 < hdi_score2) AND (hdi_score2 < hdi_score3) AND (hdi_score3 < hdi_score4) AND (hdi_score4 < hdi_score5)) as R JOIN country ON country.cid = R.cid ORDER BY cname ASC);


-- Query 7 statements

INSERT INTO Query7 (SELECT DISTINCT religion.rid, religion.rname, followers FROM (SELECT rid, sum(followers) as followers FROM (SELECT cid, rid, (rpercentage * population) as followers FROM (SELECT country.cid, religion.rid, rpercentage, population FROM religion JOIN country ON country.cid = religion.cid) as C) as R GROUP BY rid) as L JOIN religion ON L.rid = religion.rid ORDER BY followers DESC);


-- Query 8 statements

INSERT INTO Query8 (SELECT q.cname as c1name, t.cname as c2name, lname FROM (SELECT country, neighbor, R.lname FROM (SELECT A.cid, lname FROM (SELECT cid, max(lpercentage) as popular FROM language GROUP BY cid) as A JOIN language as B ON A.cid = B.cid and lpercentage = popular) as C JOIN neighbour as D ON c.cid = D.country JOIN (SELECT A.cid, lname FROM (SELECT cid, max(lpercentage) as popular FROM language GROUP BY cid) as A JOIN language as B ON A.cid = B.cid and lpercentage = popular) as R ON D.neighbor = R.cid WHERE C.lname = R.lname) as J JOIN country as Q ON Q.cid = J.country JOIN country as T ON T.cid = J.neighbor ORDER BY lname ASC, c1name DESC);


-- Query 9 statements

INSERT INTO Query9 (SELECT cname, totalspan FROM (SELECT cid, height as totalspan FROM country WHERE cid NOT IN (SELECT cid FROM oceanaccess) UNION SELECT A.cid, max(height + depth) as totalspan FROM (SELECT cid, height FROM country) as A JOIN oceanaccess B ON A.cid = B.cid JOIN ocean as C ON C.oid = B.oid GROUP BY A.cid) as X JOIN country as Y ON X.cid = Y.cid ORDER BY totalspan DESC LIMIT 1);


-- Query 10 statements

INSERT INTO Query10 (SELECT cname, borderslength  FROM (SELECT country, sum(sum) as borderslength FROM (SELECT country, sum(length) FROM neighbour GROUP BY country UNION ALL SELECT neighbor as country, sum(length) FROM neighbour GROUP BY neighbor) as H GROUP BY country) as R JOIN country as Z ON R.country = Z.cid ORDER BY borderslength DESC LIMIT 1);
