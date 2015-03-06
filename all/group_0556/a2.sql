-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW max_height as SELECT n.country as country, max(c.height) as height from neighbour as n, country as c where n.neighbor=c.cid group by n.country;
-- Get country and its neighbor with heighest height

CREATE VIEW pre_ans as SELECT max_height.country as c1id, country.cname as c1name, max_height.height as height from max_height, country where max_height.country=country.cid;
-- Get country id and its name 

CREATE VIEW temp as SELECT neighbour.country as c1id, country.cid as c2id, country.cname as c2name, country.height as height from neighbour, country where neighbour.neighbor=country.cid;
-- Get neighbor id, name, and height

INSERT INTO Query1(SELECT pre_ans.c1id as c1id, pre_ans.c1name as c1name, temp.c2id as c2id, temp.c2name as c2name from pre_ans, temp where pre_ans.c1id=temp.c1id and pre_ans.height=temp.height order by c1name);
-- Put pre_ans view and temp view together 

DROP VIEW temp;
DROP VIEW pre_ans;
DROP VIEW max_height;
-- DROP all views


-- Query 2 statements

INSERT INTO Query2(SELECT cid, cname from country where cid not in (select cid from oceanAccess) order by cname);
-- Find countries in oceanAccess. Then find countries not in that.


-- Query 3 statements

CREATE VIEW no_water as SELECT country.cid as c1id, country.cname as c1name from country where cid not in (select cid from oceanAccess);
-- Find countries in oceanAccess. Then find countries not in that. Landlocked countries

CREATE VIEW one_n as SELECT country, count(neighbor) as num from neighbour group by country;
-- Find country and its number of neighbours

CREATE VIEW c2 as SELECT one_n.country as c1id, neighbour.neighbor as c2id, country.cname as c2name from one_n, country, neighbour where one_n.country=neighbour.country and neighbour.neighbor=country.cid and one_n.num=1;
-- select one neighbour country id and neighbor's id and name 

INSERT INTO Query3(SELECT no_water.c1id as c1id, no_water.c1name as c1name, c2.c2id as c2id, c2.c2name as c2name from no_water, c2 where no_water.c1id=c2.c1id order by c1name);
-- Combine no_water and c2 

DROP VIEW c2;
DROP VIEW one_n;
DROP VIEW no_water;
-- Drop all views


-- Query 4 statements

INSERT INTO Query4 (SELECT DISTINCT c.cname, o.oname FROM country c, ocean o, neighbour n, oceanAccess oa WHERE (c.cid = oa.cid AND oa.oid = o.oid) OR (c.cid = n.country AND n.neighbor = oa.cid AND oa.oid = o.oid) ORDER BY c.cname ASC, o.oname DESC);
-- Two cases, country has ocean access or its neighbor has ocean access. Used distinct to avoid duplicate


-- Query 5 statements

INSERT INTO Query5 (SELECT c.cid, c.cname, AVG(h.hdi_score) AS avghdi FROM country c, hdi h WHERE (h.year >= 2009 AND h.year <= 2013) AND c.cid=h.cid GROUP BY c.cid, c.cname ORDER BY avghdi DESC LIMIT 10);


-- Query 6 statements

CREATE VIEW fiveyear AS SELECT h.cid, h.year, h.hdi_score FROM hdi h WHERE (h.year >= 2009 AND h.year <=2013);

CREATE VIEW increase AS SELECT h1.cid, h1.year FROM fiveyear h1, fiveyear h2 WHERE h1.cid = h2.cid AND h1.year = h2.year + 1 AND h1.hdi_score > h2.hdi_score;

INSERT INTO Query6 (SELECT c.cid, c.cname FROM country c, increase i WHERE c.cid = i.cid GROUP BY c.cid, c.cname HAVING count(i.year) = 4 ORDER BY cname ASC);

DROP VIEW increase;
DROP VIEW fiveyear;


-- Query 7 statements

INSERT INTO Query7 (SELECT r.rid, r.rname, SUM(c.population * r.rpercentage * 0.01) AS followers FROM religion r, country c WHERE r.cid = c.cid GROUP BY rid, r.rname ORDER BY followers DESC);


-- Query 8 statements

CREATE VIEW maxper AS SELECT l.cid, max(l.lpercentage) FROM language l GROUP BY l.cid;

CREATE VIEW toplanguage AS SELECT m.cid, l.lname, c.cname FROM maxper m, language l, country c WHERE m.cid = l.cid AND m.max = l.lpercentage AND m.cid = c.cid;

INSERT INTO Query8 (SELECT t1.cname AS c1name, t2.cname AS c2name, t1.lname FROM toplanguage t1, toplanguage t2 WHERE t1.cid != t2.cid AND t1.lname = t2.lname ORDER BY t1.lname ASC, t1.cname DESC);

DROP VIEW toplanguage;
DROP VIEW maxper;


-- Query 9 statements
CREATE VIEW deep_yes as SELECT oceanAccess.cid as cid, ocean.depth as depth from ocean, oceanAccess where ocean.oid=oceanAccess.oid;

CREATE VIEW deep_zero as SELECT cid, 0 as depth from country where cid not in (select cid from deep_yes);

CREATE VIEW deep as (select * from deep_yes) UNION (select * from deep_zero);

CREATE VIEW deep2 as SELECT country.cname as cname, max(abs(country.height-deep.depth)) as totalspan from country, deep where country.cid=deep.cid group by country.cid;

CREATE VIEW max_deep as SELECT max(totalspan) as totalspan from deep2;

INSERT INTO Query9(SELECT deep2.cname, deep2.totalspan from max_deep, deep2 where max_deep.totalspan=deep2.totalspan);

DROP VIEW max_deep;
DROP VIEW deep2;
DROP VIEW deep;
DROP VIEW deep_zero;
DROP VIEW deep_yes;


-- Query 10 statements
CREATE VIEW border as SELECT neighbour.country as cid, sum(neighbour.length) as blength from neighbour group by neighbour.country;

CREATE VIEW not_max as SELECT border1.cid from border as border1, border as border2 where border1.blength<border2.blength;

INSERT INTO Query10(SELECT DISTINCT country.cname as cname, border.blength from border, not_max, country where border.cid not in (select * from not_max) and border.cid=country.cid);

DROP VIEW not_max;
DROP VIEW border;
