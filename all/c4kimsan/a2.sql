-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
-- INSERT INTO Query1 (
-- SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name FROM country c1 JOIN neighbour ne ON 
-- c1.cid=ne.country JOIN country c2 ON ne.neighbor=c2.cid WHERE c2.height IN (SELECT MAX(c2.height) FROM country c1 
-- JOIN neighbour ne ON c1.cid=ne.country JOIN country c2 ON ne.neighbor=c2.cid) ORDER by c1name DESC
-- );

CREATE VIEW V4 AS (SELECT c1.cid, max(c2.height) FROM country c1 JOIN neighbour ne ON c1.cid=ne.country JOIN country c2 ON 
c2.cid=ne.neighbor GROUP BY c1.cid);


INSERT INTO Query1(
SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name FROM Country c1 JOIN V4 view1 ON c1.cid=view1.cid 
JOIN neighbour ne ON view1.cid=ne.country JOIN country c2 ON c2.cid=ne.neighbor WHERE view1.max=c2.height ORDER BY c1.cname ASC
);

-- Query 2 statements
INSERT INTO Query2 (
SELECT cid, cname FROM country EXCEPT SELECT co.cid, co.cname FROM country co JOIN oceanAccess oa 
ON co.cid=oa.cid ORDER BY cname ASC
);


-- Query 3 statements
INSERT INTO Query3 (
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name FROM country c1 JOIN neighbour ne ON 
c1.cid=ne.country JOIN country c2 ON ne.neighbor=c2.cid WHERE c1.cid IN (SELECT c1.cid FROM country c1 JOIN neighbour 
ne ON c1.cid=ne.country JOIN country c2 ON ne.neighbor=c2.cid WHERE c1.cid NOT IN (SELECT co.cid FROM country co JOIN 
oceanAccess oa ON co.cid=oa.cid) GROUP BY c1.cid HAVING count(c1.cid)=1) ORDER BY c1name ASC
);

-- Query 4 statements
INSERT INTO Query4 (
SELECT co.cname, oc.oname FROM country co JOIN neighbour ne ON co.cid=ne.country JOIN country c2 ON ne.neighbor=c2.cid 
JOIN oceanAccess oa ON c2.cid=oa.cid JOIN ocean oc ON oa.oid=oc.oid UNION SELECT c2.cname, oc.oname FROM country co 
JOIN neighbour ne ON co.cid=ne.country JOIN country c2 ON ne.neighbor=c2.cid JOIN oceanAccess oa ON c2.cid=oa.cid 
JOIN ocean oc ON oa.oid=oc.oid ORDER BY cname ASC, oname DESC
);


-- Query 5 statements
INSERT INTO Query5 (
SELECT c1.cid as cid, c1.cname as cname, avg(hd.hdi_score) as avghdi FROM country c1 JOIN hdi hd ON c1.cid=hd.cid 
WHERE year>=2009 AND year<=2013 GROUP BY c1.cid HAVING count(year)=5 ORDER BY avghdi DESC LIMIT 10
);

-- Query 6 statements
INSERT INTO Query6 (
SELECT c1.cid, c1.cname FROM country c1 JOIN hdi hd ON c1.cid=hd.cid WHERE (SELECT hdi_score FROM hdi WHERE c1.cid=cid AND 
year=2013)>(SELECT hdi_score FROM hdi WHERE c1.cid=cid AND year=2012) AND (SELECT hdi_score FROM hdi WHERE c1.cid=cid AND 
year=2012) > (SELECT hdi_score FROM hdi WHERE c1.cid=cid AND year=2011) AND (SELECT hdi_score FROM hdi WHERE c1.cid=cid AND 
year=2011) > (SELECT hdi_score FROM hdi WHERE c1.cid=cid AND year=2010) AND (SELECT hdi_score FROM hdi WHERE c1.cid=cid AND 
year=2010) > (SELECT hdi_score FROM hdi WHERE c1.cid=cid AND year=2009) GROUP BY c1.cid ORDER BY c1.cname ASC 
);

-- Query 7 statements
INSERT INTO Query7 (
SELECT r1.rid as rid, r1.rname as rname, SUM(c1.population*r1.rpercentage) as followers FROM country c1 JOIN religion r1 ON 
c1.cid=r1.cid GROUP BY r1.rid, r1.rname ORDER BY followers DESC
);

-- Query 8 statements



-- Query 9 statements
CREATE VIEW V1 AS
(SELECT max(abs(oc.depth+c1.height)) as maxheight1, c1.cname FROM country c1 JOIN oceanAccess oa ON c1.cid=oa.cid JOIN ocean oc
ON oc.oid=oa.cid GROUP BY c1.cname ORDER BY maxheight1 DESC);

CREATE VIEW V2 AS (SELECT max(c1.height) as maxheight2, c1.cname FROM country c1 GROUP BY c1.cname ORDER BY maxheight2 DESC);

CREATE VIEW V3 AS (SELECT * FROM V1 UNION SELECT * FROM V2 ORDER BY maxheight1 DESC);

INSERT INTO Query9(
SELECT cname, maxheight1 FROM V3 LIMIT 1
);

-- Query 10 statements
INSERT INTO Query10(
 SELECT c1.cname, sum(ne.length) as borderslength FROM country c1 JOIN neighbour ne ON c1.cid=ne.country JOIN country c2 
ON ne.neighbor=c2.cid GROUP BY c1.cid ORDER BY borderslength DESC LIMIT 1
);


DROP VIEW V3 CASCADE;
DROP VIEW V2 CASCADE;
DROP VIEW V1 CASCADE;
DROP VIEW V4 CASCADE;
