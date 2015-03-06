-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW neighborheights as SELECT c1.cid,max(c2.height) FROM country c1, neighbour n1, country c2 WHERE n1.neighbor=c1.cid AND n1.country=c2.cid GROUP BY c1.cid;

CREATE VIEW neighborheights1 as SELECT n.cid,c.cname,n.max FROM neighborheights n,country c WHERE n.cid=c.cid;

INSERT INTO Query1 (SELECT nh.cid as c1id,nh.cname c1name,c.cid c2id,c.cname c2name FROM neighborheights1 nh,country c WHERE nh.max=c.height ORDER BY nh.cname);

DROP VIEW neighborheights CASCADE;

-- Query 2 statements

CREATE VIEW landlockedcountries as (SELECT DISTINCT cid FROM country) EXCEPT (SELECT DISTINCT cid FROM oceanaccess);

INSERT INTO Query2 (SELECT l.cid as cid,c.cname as cname FROM landlockedcountries l,country c WHERE l.cid=c.cid ORDER BY cname);

DROP VIEW landlockedcountries CASCADE;

-- Query 3 statements

CREATE VIEW numneighbour as (SELECT country,count(*) FROM neighbour GROUP BY country);

CREATE VIEW surroundedcountries as (SELECT n.country,n.neighbor from numneighbour nn,neighbour n WHERE nn.count=1 AND nn.country=n.country);

CREATE VIEW landlockedcountries1 as (SELECT DISTINCT cid FROM country) EXCEPT (SELECT DISTINCT cid FROM oceanaccess);

CREATE VIEW landlockedsurrounded as (SELECT s.country,s.neighbor FROM surroundedcountries s,landlockedcountries1 l WHERE s.country=l.cid);

INSERT INTO Query3 (select c1.cid as c1id,c1.cname as c1name,c2.cid as c2id,c2.cname as c2name from landlockedsurrounded lls,country c1,country c2 WHERE lls.country=c1.cid AND lls.neighbor=c2.cid ORDER BY c1name);

DROP VIEW numneighbour CASCADE;

DROP VIEW landlockedcountries1 CASCADE;

-- Query 4 statements

CREATE VIEW neighborocean as SELECT n.country,o.oid FROM neighbour n,oceanaccess o WHERE n.neighbor=o.cid;

CREATE VIEW oceanborder as (SELECT * FROM oceanaccess) UNION (SELECT * FROM neighborocean);

INSERT INTO Query4 (SELECT c.cname as cname,o.oname as oname FROM oceanborder ob,country c,ocean o WHERE ob.cid=c.cid AND ob.oid=o.oid ORDER BY cname,oname DESC);

DROP VIEW neighborocean CASCADE;

-- Query 5 statements

CREATE VIEW distincthdi AS SELECT cid,avg(hdi_score) FROM hdi WHERE year>2008 AND year<2014 GROUP BY cid;

CREATE VIEW top10hdi AS SELECT * FROM distincthdi ORDER BY avg DESC LIMIT 10;

INSERT INTO Query5 (SELECT c.cid,c.cname,t.avg as avghdi FROM top10hdi t,country c WHERE c.cid=t.cid ORDER BY avghdi DESC);

DROP VIEW distincthdi CASCADE;

-- Query 6 statements



-- Query 7 statements

CREATE VIEW religioncountrytotals AS SELECT r.rid,r.rname,((r.rpercentage * 0.01) * c.population) as followers FROM religion r,country c WHERE r.cid=c.cid;

INSERT INTO Query7 (SELECT rid,rname,sum(followers) as followers FROM religioncountrytotals GROUP BY rid,rname ORDER BY followers DESC);

DROP VIEW religioncountrytotals CASCADE;

-- Query 8 statements

CREATE VIEW popularlanguage AS SELECT cid,max(lname) as lname,max(lpercentage) FROM language GROUP BY cid;

INSERT INTO Query8 (SELECT n.country as c1name,n.neighbor as c2name,p1.lname FROM neighbour n,popularlanguage p1,popularlanguage p2 WHERE n.country=p1.cid AND n.neighbor=p2.cid AND p1.lname=p2.lname ORDER BY lname,c1name DESC);

DROP VIEW popularlanguage CASCADE;
-- Query 9 statements

INSERT INTO Query9 (SELECT cname,(o.depth+c.height) as totalspan FROM ocean o,oceanaccess oa,country c WHERE oa.cid=c.cid AND oa.oid=o.oid ORDER BY totalspan DESC LIMIT 1);

-- Query 10 statements

INSERT INTO Query10 (SELECT c.cname,sum(length) as totalspan FROM neighbour n,country c WHERE n.country=c.cid GROUP BY c.cname ORDER BY totalspan DESC LIMIT 1);
