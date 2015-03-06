-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW tmp1 AS
SELECT * FROM country JOIN neighbour ON country=cid; 

CREATE VIEW Pair AS
SELECT tmp1.cid AS c1_cid, max(c2.height) as c2_height FROM tmp1 JOIN country c2 ON c2.cid=neighbor GROUP BY tmp1.cid;

CREATE VIEW Almost AS
SELECT DISTINCT c1_cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name FROM (Pair JOIN country ON Pair.c1_cid = country.cid) c1 JOIN country c2 on c2_height = c2.height ORDER BY c1name ASC;

INSERT INTO Query1 (SELECT * FROM Almost where c2id IN (SELECT neighbor FROM neighbour WHERE country=c1id));

DROP VIEW Almost;
DROP VIEW Pair;
DROP VIEW tmp1;
-- Query 2 statements

INSERT INTO Query2 (select DISTINCT cid, cname from
((select cid from country) except (select cid from oceanAccess where oid is not null)) as A natural join country order by cname ASC);

-- Query 3 statements
CREATE VIEW Landlocked AS
select cid, cname from
((select cid from country) except (select cid from oceanAccess where oid is not null)) as A natural join country;

CREATE VIEW tmp AS
   select cid as c1id, cname as c1name
   from Landlocked join neighbour on country=Landlocked.cid
   group by cid,cname having count(neighbor)=1;

create view tmp2 as
   select c1id, c1name, neighbor as c2id
   from tmp join neighbour on c1id=country;

INSERT INTO Query3 (
   select DISTINCT c1id, c1name, c2id, cname as c2name
   from tmp2 join country on c2id=cid
   order by c1name ASC);

DROP view tmp2;
DROP view tmp;
DROP view Landlocked;

-- Query 4 statements

CREATE VIEW SeaBoarder AS
SELECT cid from oceanAccess where oid is not null;

CREATE VIEW SeaBoarderMore AS
SELECT cname,oname,cid from SeaBoarder NATURAL JOIN country NATURAL JOIN oceanAccess NATURAL JOIN ocean;

CREATE VIEW SeaBoarderBoarder AS
SELECT country.cname AS cname,oname FROM neighbour,SeaBoarderMore,country WHERE SeaBoarderMore.cid = neighbor AND country.cid = country;

INSERT INTO Query4
SELECT * FROM ((SELECT cname,oname FROM SeaBoarderMore) UNION (SELECT * FROM SeaBoarderBoarder))A ORDER BY cname ASC, oname DESC;
DROP VIEW SeaBoarderBoarder;
DROP VIEW SeaBoarderMore;
DROP VIEW SeaBoarder;

-- Query 5 statements
CREATE VIEW AvgScore AS
SELECT cid, avg(hdi_score) as avghdi FROM hdi WHERE year>=2009 AND year <=2013 GROUP BY cid ORDER BY avg(hdi_score) DESC LIMIT 10;

INSERT INTO Query5(SELECT cid, cname, avghdi FROM AvgScore NATURAL JOIN country ORDER BY avghdi DESC);

DROP VIEW AvgScore;

-- Query 6 statements

CREATE VIEW RightTime AS
SELECT * FROM hdi WHERE year>=2009 AND year <=2013;


INSERT INTO Query6(select DISTINCT cid, cname from (select h1.cid from RightTime h1, RightTime h2, RightTime h3, RightTime h4, RightTime h5
where (h1.cid = h2.cid) and (h2.cid=h3.cid) and (h3.cid=h4.cid) and (h4.cid=h5.cid) and
(h1.year < h2.year) and (h2.year<h3.year) and (h3.year<h4.year) and (h4.year<h5.year) and
(h1.hdi_score < h2.hdi_score) and (h2.hdi_score < h3.hdi_score) and (h3.hdi_score < h4.hdi_score) and
(h4.hdi_score < h5.hdi_score)
) hdi_increase natural join country order by cname ASC);

DROP VIEW RightTime;

-- Query 7 statements
CREATE VIEW Followers AS
SELECT rid,sum(rpercentage*population) as followers FROM religion NATURAL JOIN country GROUP BY rid;

INSERT INTO Query7(SELECT DISTINCT rid, rname, followers FROM Followers NATURAL JOIN religion ORDER BY followers DESC);
DROP VIEW Followers;

-- Query 8 statements

create view C as
select cname, cid, lname from
country natural join (select B.cid,lname from language, (select cid, max(lpercentage) from language group by cid) B
where language.cid=B.cid and lpercentage=max) B;

INSERT INTO Query8 (select DISTINCT C1.cname as c1name, C2.cname as c2name, C1.lname as lname from C C1, C C2 
where C1.lname=C2.lname and C2.cid in (select neighbor from neighbour where country=C1.cid) ORDER BY lname ASC, c1name DESC);


DROP VIEW C;
-- Query 9 statements

CREATE VIEW SeaBoarders AS
SELECT cid, max(depth)+ max(height) AS totalspan from oceanAccess NATURAL JOIN ocean NATURAL JOIN country GROUP BY cid;


CREATE VIEW LandBoarder AS
SELECT cid, height AS totalspan from ((SELECT cid FROM country) EXCEPT (SELECT cid FROM oceanAccess)) A NATURAL JOIN country;


CREATE VIEW AllSpan AS 
SELECT cname, totalspan FROM ((SELECT * FROM SeaBoarders NATURAL JOIN country) UNION (SELECT * FROM LandBoarder NATURAL JOIN country)) A; 

INSERT INTO Query9 (SELECT cname, totalspan FROM AllSpan WHERE totalspan = (SELECT max(totalspan) FROM AllSpan));

DROP VIEW AllSpan;
DROP VIEW SeaBoarders;
DROP VIEW LandBoarder;



-- Query 10 statements

create view A as                          
select country, sum(length) as borderslength from neighbour group by country;

INSERT INTO Query10 (select DISTINCT cname, borderslength from A join country on cid=A.country where
borderslength >= all(select borderslength from A));

DROP VIEW A;
