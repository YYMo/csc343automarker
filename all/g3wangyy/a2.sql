-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW heights  AS  (SELECT country AS c1id, neighbor AS c2id, height AS h FROM neighbour, country WHERE neighbor = cid );
CREATE VIEW max_heights  AS  (SELECT c1id, MAX(h) FROM heights GROUP BY c1id);
CREATE VIEW t1 AS (select heights.c1id, c2id, h from heights, max_heights where heights.c1id = max_heights. c1id and heights.h = max_heights.max);
create VIEW t2 AS(select c1id, cname as c1name, c2id from t1, country where c1id = cid);
INSERT INTO Query1 (select c1id, c1name, c2id, cname as c2name from t2, country where c2id = cid ORDER BY c1name ASC);
DROP VIEW t2;
DROP VIEW t1;
DROP VIEW max_heights;
DROP VIEW heights;

-- Query 2 statements
INSERT INTO Query2(select cid, cname from country, ((select cid as ccid from country) EXCEPT (select cid as ccid from oceanAccess))AS t1 where ccid = cid ORDER BY cname ASC);

-- Query 3 statements

CREATE VIEW locked AS (select cid AS c1id, cname AS c1name from country, ((select cid as ccid from country) EXCEPT (select cid as ccid from oceanAccess))AS t1 where ccid = cid);
CREATE VIEW hasOneNeighbor AS (select country from neighbour Group by country having COUNT(neighbor) = 1);
CREATE VIEW lockedbyOne AS ((select c1id from locked) intersect (select country from hasOneNeighbor ));
create view c1c2 AS (select c1id, neighbor as c2id from lockedbyOne, neighbour where c1id = country);

INSERT INTO Query3(select c1id, cname AS c1name, c2id, c2name from ( select c1id, c2id, cname as c2name from c1c2, country where c2id = cid) AS temp, country where c1id = cid ORDER BY c1name ASC); 

DROP VIEW c1c2;
DROP VIEW lockedbyOne;
DROP VIEW hasOneNeighbor;
DROP VIEW locked;

-- Query 4 statements
CREATE VIEW neighbors AS (select country as ccid, neighbor as ncid from neighbour);
CREATE VIEW neighborHasAccess AS (select ccid, ncid, oid from neighbors, oceanAccess where ncid = cid);
CREATE VIEW t1 AS ((select ccid, oid from neighborHasAccess) union (select cid, oid from oceanAccess));
Insert into Query4(select distinct cname, oname from country , t1, ocean where country.cid = ccid and ocean.oid = t1.oid ORDER BY cname ASC, oname DESC);

DROP VIEW t1;
DROP VIEW neighborHasAccess;
DROP VIEW neighbors;

-- Query 5 statements

CREATE VIEW fiveyr AS (SELECT cid, hdi_score from hdi where year > 2008 and year < 2014);

CREATE VIEW cs AS (select cid, AVG(hdi_score) AS avehdi from fiveyr group by cid order by AVG(hdi_score));

INSERT INTO Query5(SELECT country.cid, cname, avehdi FROM cs, country WHERE country.cid = cs.cid ORDER BY avehdi DESC LIMIT 10); 

DROP VIEW cs;
DROP VIEW fiveyr;

-- Query 6 statements

CREATE VIEW t09 AS (select cid, hdi_score from hdi where year = 2009 );
CREATE VIEW t10 AS (select cid, hdi_score from hdi where year = 2010 );
CREATE VIEW t11 AS (select cid, hdi_score from hdi where year = 2011 );
CREATE VIEW t12 AS (select cid, hdi_score from hdi where year = 2012 );
CREATE VIEW t13 AS (select cid, hdi_score from hdi where year = 2013 );


 CREATE VIEW allt AS (select t09.cid as ccid, t09.hdi_score as t9h, t10.hdi_score as t10h, t11.hdi_score as t11h, t12.hdi_score as t12h, t13.hdi_score as t13h from t09, t10, t11, t12, t13 where t09.cid = t10.cid and t10.cid= t11.cid and t11.cid = t12.cid and t12.cid = t13.cid);

INSERT INTO Query6 (select ccid, cname from allt, country where t9h < t10h and t10h< t11h and t11h < t12h and t12h < t13h and ccid = cid ORDER BY cname);

DROP VIEW allt;
DROP VIEW t09;
DROP VIEW t10;
DROP VIEW t11;
DROP VIEW t12;
DROP VIEW t13;

-- Query 7 statements
CREATE VIEW t1 AS (
select rid, sum(followers) as followers from ( select rid, rname, (rpercentage/100*population) AS followers from religion, country where religion.cid = country.cid) as temp group by rid);

INSERT INTO Query7 (select DISTINCT t1.rid, rname, followers from t1, religion where t1.rid = religion.rid ORDER by followers DESC );

DROP VIEW t1;

-- Query 8 statements

CREATE VIEW mostpopular AS (select cid, max(lpercentage) as t1 from language group by language.cid);

CREATE VIEW lnames AS (
select language.cid, lname, lid, t1 from mostpopular, language where language.cid = mostpopular.cid and t1 = lpercentage
);
CREATE VIEW country1 AS (
select cid as c1id, lid as l1id, lname as l1name, neighbor as c2id from lnames, neighbour where lnames.cid = neighbour.country); 
CREATE VIEW country2 AS (
select neighbor as c2id ,lid as l2id, lname as l2name from lnames, neighbour where lnames.cid = neighbour.neighbor);

CREATE VIEW temp1 as (select c1id, l1id, l1name, country2.c2id, l2id, l2name from country1, country2 where country1.c2id = country2.c2id);
INSERT INTO Query8(
select distinct c1name, cname as c2name, l1name as lname from (select cname as c1name, l1name, c2id from (select l1name, c2id, c1id from temp1 where l1id = l2id) as c1, country where c1id = cid) as c2, country where c2id = cid) ORDER BY lname ASC, c1name DESC;
DROP VIEW temp1, country2, country1, lnames, mostpopular;
 
-- Query 9 statements


CREATE VIEW dirAccess AS(select country.cid, cname, depth, height from oceanAccess, country, ocean where oceanAccess.cid = country.cid and ocean.oid = oceanAccess.oid);
CREATE VIEW nodirAccess AS((select cid, cname from country) except (select cid, cname from dirAccess));

CREATE VIEW set0 AS (select nodirAccess.cid, nodirAccess.cname, height as totalspan from nodirAccess, country where nodirAccess.cid = country.cid);

CREATE VIEW combined AS (select cid, cname, totalspan from set0) union (select cid, cname, abs(height - depth) as totalspan from dirAccess);

INSERT INTO Query9(
select cname, largest from (select max (totalspan) as largest from combined) as t1, combined where totalspan = largest);
DROP VIEW combined, set0, nodirAccess, dirAccess;

-- Query 10 statements
create view totals AS(select country as cid, sum(length) as total from  neighbour group by country);
INSERT INTO Query10(
select cname, borderslength from (select cid, borderslength from (select max(total) as borderslength from totals) as t, totals where borderslength =  total) as t1, country where country.cid = t1.cid);
DROP VIEW totals;