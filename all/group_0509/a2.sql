-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW countrynameneighbour AS SELECT c.cid AS c1id, c.cname AS c1name, n.neighbor AS c2id FROM country c join neighbour n ON c.cid = n.country;
CREATE VIEW neighbourname AS SELECT c1id, c1name, c2id, c.cname AS c2name, c.height as c2height FROM country c join countrynameneighbour cn ON cn.c2id = c.cid;

--c1id, c1name, c2id, c2name, c2height
CREATE VIEW getMaxHeight AS SELECT c1id, c1name, c2id, c2name from neighbourname x where c2height >= ALL (Select c2height FROM neighbourname WHERE c1id = x.c1id);


INSERT INTO Query1 SELECT c1id, c1name, c2id, c2name FROM getMaxHeight ORDER BY c1name ASC;

DROP VIEW getMaxHeight;
DROP VIEW neighbourname;
DROP VIEW countrynameneighbour;





-- Query 2 statements
INSERT INTO Query2 SELECT cid, cname FROM country c1 where not exists (SELECT cid FROM oceanAccess where cid=c1.cid) ORDER BY cname ASC;


-- Query 3 statements
CREATE VIEW landlocked AS SELECT cid, cname FROM country WHERE not exists (SELECT cid FROM oceanaccess where country.cid = oceanaccess.cid);

CREATE VIEW loners AS SELECT cid as c1id, cname as c1name, neighbour.neighbor AS c2id FROM neighbour join country ON neighbour.country = country.cid  WHERE (select count(neighbor) from neighbour where neighbour.country=cid group by cid) = 1;

CREATE VIEW nameloners AS SELECT c1id, c1name, c2id, cname AS c2name FROM loners join country ON loners.c2id = country.cid;

INSERT INTO Query3 SELECT c1id, c1name, c2id, c2name FROM nameloners WHERE exists (SELECT cid FROM landlocked where c1id = cid) ORDER BY c1name ASC;
DROP VIEW nameloners;
DROP VIEW loners;
DROP VIEW landlocked;



-- Query 4 statements
CREATE VIEW directAccessNoNames AS SELECT cid, oname from oceanaccess a, ocean where ocean.oid = a.oid;

CREATE VIEW directAccessNames as select country.cid, cname, oname from directAccessNoNames JOIN country ON directAccessNoNames.cid = country.cid;

CREATE VIEW DAneighbours as select cname, oname, n.neighbor as c2id from directAccessNames join neighbour n on directAccessNames.cid = n.country;

CREATE VIEW IndirectAccessNames as select DAneighbours.cname, oname, country.cname AS c2name from DAneighbours join country on DAneighbours.c2id = country.cid;

CREATE VIEW HasAccess as select cname, oname from directAccessNames;
CREATE VIEW HasAccess1 as Select c2name as cname, oname from IndirectAccessNames;
CREATE VIEW final as select * from ((SELECT * FROM HasAccess) union (SELECT * FROM HasAccess1))as c;

INSERT INTO Query4 Select * from final order by cname ASC, oname DESC;
DROP VIEW final;
DROP VIEW HasAccess1;
DROP VIEW HasAccess;
DROP VIEW IndirectAccessNames;
DROP VIEW DAneighbours;
DROP VIEW directAccessNames;
DROP VIEW directAccessNoNames;


-- Query 5 statements
CREATE VIEW filteredYears AS Select cid, avg(hdi_score) AS avghdi FROM hdi WHERE year < 2014 and year > 2008 GROUP BY cid;
CREATE VIEW getNames AS SELECT country.cid, country.cname, avghdi FROM filteredYears join country on filteredyears.cid = country.cid order by avghdi DESC;
INSERT INTO Query5 SELECT * FROM getNames order by avghdi DESC limit 10;
DROP VIEW getNames;
DROP VIEW filteredYears;


-- Query 6 statements

CREATE VIEW diff as SELECT country.cid, country.cname, 

(select hdi_score from hdi where hdi.cid = country.cid and year=2010) - (select hdi_score from hdi where hdi.cid = country.cid and year=2009) as diff1, 

(select hdi_score from hdi where hdi.cid = country.cid and year=2011) - (select hdi_score from hdi where hdi.cid = country.cid and year=2010) as diff2, 

(select hdi_score from hdi where hdi.cid = country.cid and year=2012) - (select hdi_score from hdi where hdi.cid = country.cid and year=2011) as diff3, 

(select hdi_score from hdi where hdi.cid = country.cid and year=2013) - (select hdi_score from hdi where hdi.cid = country.cid and year=2012) as diff4 

FROM country;


INSERT INTO Query6 SELECT cid, cname from diff where (diff1 > 0 and diff2 > 0 and diff3 > 0 and diff4 > 0 and cid=diff.cid) order by cname ASC;
DROP VIEW diff;



-- Query 7 statements
CREATE VIEW popPercent AS  SELECT rid, rname, sum(rpercentage* population) as followers  FROM religion join country on religion.cid = country.cid group by rid, rname order by followers DESC;

INSERT INTO Query7 SELECT * FROM popPercent order by followers DESC;

DROP VIEW popPercent;

-- Query 8 statements
CREATE VIEW  temp1 AS SELECT country.cid as c1id, cname as c1name, lname as c1lang from country, language where country.cid = language.cid and language.lpercentage >= ALL (select lpercentage from language where language.cid=country.cid);

CREATE VIEW temp2 AS SELECT country.cid as c2id, cname as c2name, lname as c2lang from country, language where country.cid = language.cid and language.lpercentage >= ALL (select lpercentage from language where language.cid=country.cid);

CREATE VIEW combined as select c1name, c2name, c1lang as lname from temp1, neighbour, temp2 where c1id = neighbour.country and c2id=neighbour.neighbor and c1lang=c2lang;

INSERT INTO Query8(SELECT * FROM combined) order by lname ASC, c1name DESC;
DROP VIEW combined;
DROP VIEW temp2;
DROP VIEW temp1;

-- Query 9 statements
CREATE VIEW temp1 as SELECT cname, oid, height from country join oceanAccess ON country.cid = oceanAccess.cid;

CREATE VIEW temp2 AS SELECT cname, avg(height) as height, max(depth) as depth from temp1 join ocean on temp1.oid = ocean.oid group by temp1.cname;

CREATE VIEW temp3 AS SELECT cname, max(height+depth) AS totalspan from temp2 group by cname;

CREATE VIEW temp4 AS (SELECT cname, height AS totalspan from country where not exists  (select cname from temp3 where cname = country.cname));

CREATE VIEW final as select * from ((SELECT * FROM temp3) union (SELECT * FROM temp4))as c;

INSERT INTO Query9 SELECT * FROM final WHERE totalspan >= ALL (SELECT totalspan from final);

DROP VIEW final;
DROP VIEW temp4;
DROP VIEW temp3;
DROP VIEW temp2;
DROP VIEW temp1;



-- Query 10 statements
CREATE VIEW totalBorder AS SELECT cid, sum(length) AS sumBorder FROM country join neighbour ON country.cid = neighbour.neighbor GROUP BY cid;

CREATE VIEW maxBorder AS SELECT cname, sumBorder AS borderslength FROM country join totalBorder ON country.cid = totalBorder.cid where sumBorder >= ALL (Select sumBorder from totalBorder where totalBorder.cid=country.cid) order by borderslength DESC; 

INSERT INTO Query10 SELECT * FROM maxBorder LIMIT 1;

DROP VIEW maxborder;
DROP VIEW totalBorder;

