-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW V_T1 AS SELECT c1.cid AS c1id, c2.cid AS c2id, c2.height AS hgt FROM neighbour n ,country c1, country c2 WHERE n.country = c1.cid and n.neighbor = c2.cid;

CREATE VIEW V_T2 AS SELECT * FROM V_T1 EXCEPT SELECT a.c1id, a.c2id, a.hgt FROM V_T1 a, V_T1 b WHERE a.c1id = b.c1id AND a.hgt < b.hgt;

INSERT INTO Query1 (SELECT c1id, a.cname as c1name, c2id, b.cname asc2name FROM V_T2, country a, country b WHERE c1id = a.cid AND c2id = b.cid ORDER BY c1name ASC);

drop view V_T2;

drop view V_T1;


-- Query 2 statements

INSERT INTO Query2 (SELECT C.cid, cname FROM country C, (SELECT cid FROM country EXCEPT SELECT cid FROM oceanaccess) T WHERE C.cid = T.cid ORDER BY cname ASC);

-- Query 3 statements

CREATE VIEW V_T1 AS SELECT cid FROM query2 INTERSECT SELECT country as cid FROM neighbour GROUP BY country having count(neighbor) = 1;

INSERT INTO Query3 (SELECT V_T1.cid as c1id, C1.cname as c1name, C2.cid as c2id, C2.cname as c2name FROM V_T1, country C1, country C2, neighbour WHERE V_T1.cid = neighbour.country AND V_T1.cid = C1.cid AND neighbour.neighbor = C2.cid ORDER BY c1name ASC);

drop view V_T1;


-- Query 4 statements

CREATE VIEW V_T1 AS SELECT country as cid, oid FROM oceanaccess O, neighbour N WHERE O.cid = N.neighbor;

CREATE VIEW V_T2 AS SELECT * FROM V_T1 UNION SELECT * FROM oceanaccess;

INSERT INTO Query4 (SELECT cname, oname FROM V_T2, country, ocean WHERE V_T2.cid = country.cid AND V_T2.oid = ocean.oid ORDER BY cname ASC, oname DESC);

drop view V_T2;

drop view V_T1;


-- Query 5 statements

CREATE VIEW V_T1 AS SELECT cid, avg(hdi_score) as avghdi FROM hdi WHERE year >= 2009 AND year <= 2013 GROUP BY cid;

CREATE VIEW V_T2 AS SELECT V_T1.cid, cname, avghdi FROM V_T1, country where V_T1.cid = country.cid ORDER BY avghdi DESC;

INSERT INTO Query5 (SELECT * FROM V_T2 limit 10);

drop view V_T2;

drop view V_T1;


-- Query 6 statements

CREATE VIEW V_T1 AS SELECT h13.cid FROM hdi h9, hdi h10, hdi h11, hdi h12, hdi h13 WHERE h9.cid = h10.cid AND h10.cid = h11.cid AND h11.cid = h12.cid AND h12.cid = h13.cid AND h9.year = 2009 AND h10.year = 2010 AND h10.hdi_score - h9.hdi_score > 0 AND h11.year = 2011 AND h11.hdi_score - h10.hdi_score >0 AND h12.year = 2012 AND h12.hdi_score - h11.hdi_score > 0 AND h13.year = 2013 AND h13.hdi_score - h12.hdi_score > 0;

INSERT INTO Query6 (SELECT V_T1.cid, cname FROM V_T1, country WHERE V_T1.cid = country.cid ORDER BY cname ASC);

drop view V_T1;


-- Query 7 statements

INSERT INTO Query7 (SELECT rid, rname, sum(rpercentage * population) as followers FROM religion r, country c WHERE r.cid = c.cid GROUP BY rid, rname ORDER BY followers DESC);


-- Query 8 statements

CREATE VIEW V_T1 AS SELECT cid, lid FROM language EXCEPT SELECT l1.cid, l1.lid FROM language l1, language l2 WHERE l1.cid = l2.cid AND l1.lpercentage < l2.lpercentage;

CREATE VIEW V_T2 AS SELECT country, neighbor, T1.lid FROM neighbour n, V_T1 T1, V_T1 T2 WHERE n.country = T1.cid AND n.neighbor = T2.cid AND T1.lid = T2.lid;

INSERT INTO Query8 (SELECT a.cname AS c1name, b.cname AS c2name, lname FROM V_T2, country a, country b, language WHERE a.cid = V_T2.country AND b.cid = V_T2.neighbor AND V_T2.lid = language.lid AND V_T2.country = language.cid ORDER BY lname ASC, c1name DESC);

drop view V_T2;

drop view V_T1;


-- Query 9 statements

CREATE VIEW V_T1 AS SELECT oa.cid, max(depth + height) AS totalspan FROM oceanaccess oa, ocean o, country c WHERE oa.oid = o.oid AND c.cid = oa.cid GROUP BY oa.cid;

CREATE VIEW V_T2 AS SELECT cid, height AS totalspan FROM (SELECT cid, height FROM country c WHERE c.cid != ALL (SELECT cid FROM V_T1)) T0;

INSERT INTO Query9 (SELECT cname, totalspan FROM (SELECT * FROM V_T1 UNION SELECT * FROM V_T2) T, country c WHERE c.cid = T.cid);

drop view V_T2;

drop view V_T1;


-- Query 10 statements

CREATE VIEW V_T1 AS SELECT country AS cid, sum(length) AS borderslength FROM neighbour GROUP BY country ORDER BY borderslength DESC LIMIT 1;

INSERT INTO Query10 (SELECT cname, borderslength FROM V_T1 t, country c WHERE t.cid = c.cid);

