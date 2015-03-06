-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
create view temp as (select country, max(height) from neighbour natrual join country on neighbor=cid group by country);
create view temp2 as (select * from neighbour join country on neighbor=cid);
create view temp3 as select temp.country, temp2.neighbor, max from temp join temp2 on temp.country=temp2.country and max=height;
create view temp4 as select country, neighbor, cname as c1name, max from temp3 join country as c1  on country=c1.cid;

insert into query1
select country as c1id, c1name, neighbor as c2id, cname as c2name from temp4 join country on neighbor=country.cid order by c1name;
drop view temp CASCADE;
drop view temp2;

-- Query 2 statements
insert into Query2 select cid, cname from country as c1
where c1.cid not in(select cid from oceanaccess) order by cname;



-- Query 3 statements
Create VIEW NOOA as (Select c1.cid from country as c1 where c1.cid not in (select cid from oceanaccess));
create view ONEN as (select c1.cid from country c1, neighbour n1 Where c1.cid = n1.country group by c1.cid having count(neighbor) = 1);

insert into query3
SELECT c1.cid c1id, c1.cname c1name, c2.cid c2id, c2.cname c2name FROM NOOA v1, ONEN v2, country c1, neighbour n, country c2 WHERE v1.cid = v2.cid AND v1.cid = c1.cid AND v1.cid = n.country AND n.neighbor = c2.cid order by c1name;
drop view NOOA;
drop view ONEN;

-- Query 4 statements
insert into query4
(select cid as cname, oid as oname from oceanaccess) UNION (select country as cname, oid as oname from neighbour join oceanaccess on neighbor=cid) order by cname, oname desc;


-- Query 5 statements
insert into query5 select c1.cid, c1.cname, AVG(H1.hdi_score) AS avghdi
FROM country AS c1, hdi As H1
WHERE c1.cid = h1.cid AND H1.year <= 2013 AND H1.year >= 2009
GROUP BY c1.cid, c1.cname ORDER BY avghdi DESC LIMIT 10;



-- Query 6 statements
insert into query6 select c1.cid, c1.cname
from country c1, hdi h1, hdi h2, hdi h3, hdi h4, hdi h5
where c1.cid = h1.cid and c1.cid = h2.cid
and c1.cid = h3.cid and c1.cid = h4.cid and c1.cid = h5.cid
and h1.year = 2009 and h2.year = 2010
and h3.year = 2011 and h4.year = 2012
and h5.year = 2013 and h1.hdi_score < h2.hdi_score
and h2.hdi_score < h3.hdi_score and h3.hdi_score < h4.hdi_score
and h4.hdi_score < h5.hdi_score order by c1.cname;


-- Query 7 statements
CREATE VIEW ReliPop as
SELECT R.rid, R.cid, C.population*R.rpercentage AS pop
FROM COUNTRY AS C, Religion AS R
WHERE C.cid = R.cid;

INSERT INTO query7
SELECT R.rid, R.rname, SUM(R2.pop) AS followers
FROM Religion AS R, ReliPop AS R2
WHERE R.rid = R2.rid AND R.cid = R2.cid
GROUP BY R.rid, R.rname
ORDER BY followers DESC;

DROP VIEW ReliPop;

-- Query 8 statements
CREATE VIEW Popular as
SELECT L.cid, MAX(L.lpercentage) AS per
FROM Language AS L, COUNTRY AS C
WHERE L.cid = C.cid
GROUP BY L.cid;

CREATE VIEW PopularL as
SELECT L.cid, L.lname
FROM Language AS L, Popular AS P
WHERE L.cid = P.cid AND L.lpercentage = P.per;

CREATE VIEW SamePopular as
SELECT P1.cid cid1, P2.cid cid2, P1.lname
FROM PopularL P1, PopularL P2
WHERE P1.cid != P2.cid AND P1.lname = P2.lname
AND (P1.cid, P2.cid) IN (SELECT country, neighbor FROM Neighbour);

Insert INTO query8
SELECT C1.cname c1name, C2.cname c2name, S.lname
FROM COUNTRY C1, COUNTRY C2, SamePopular S
WHERE S.cid1 = C1.cid AND S.cid2 = C2.cid
ORDER BY lname, c1name DESC;

DROP VIEW Popular CASCADE;

-- Query 9 statements
insert into query9
(select cname, depth+height as totalspan from country, oceanaccess, ocean where country.cid=oceanaccess.cid and oceanaccess.oid=ocean.oid) UNION ALL (select cname, height as totalspan from country) order by totalspan desc limit 1;


-- Query 10 statements
insert into query10
select cname, sum(length) as borderslength from country, neighbour where cid=country group by cname order by borderslength desc limit 1;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             