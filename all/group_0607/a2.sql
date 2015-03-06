-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1(
select t3.cid as c1id, t3.cname as c1name, t1.cid as c2id, t1.cname as c2name 
from country t1 JOIN (select n1.country, n1.neighbor, n2.max from neighbour n1 JOIN 
	(select c1.cid, max(c2.height) 
	from neighbour, country c1, country c2 
	where c1.cid = country and c2.cid = neighbor 
	group by c1.cid) n2 
ON n1.country = n2.cid) t2 
ON t1.cid = t2.neighbor and t2.max = t1.height 
JOIN country t3 ON t3.cid = t2.country order by c1name);

-- Query 2 statements
INSERT INTO Query2(
select t1.cid, t1.cname 
from country t1 
JOIN (select cid from country EXCEPT (select cid from oceanAccess)) t2 
ON t1.cid = t2.cid order by t1.cname);

-- Query 3 statements
INSERT INTO Query3(
select c1.cid as c1id, c1.cname as c1name, c3.cid as c2id, c3.cname as c2name from country c1 JOIN (select n1.country, n1.neighbor from neighbour n1 JOIN (select country from neighbour JOIN (select t1.cid, t1.cname from country t1 JOIN (select cid from country EXCEPT (select cid from oceanAccess)) t2 ON t1.cid = t2.cid order by t1.cname) landlocked ON neighbour.country = landlocked.cid group by country having count(neighbor) = 1) n2 ON n1.country = n2.country) c2 ON c1.cid = c2.country JOIN country c3 ON c3.cid = c2.neighbor order by c1.cname);

-- Query 4 statements
INSERT INTO Query4((select c0.cname, o1.oname from country c0 JOIN (select c2.country, c1.oid from oceanaccess c1 JOIN neighbour c2 ON c1.cid = c2.neighbor JOIN (select country.cid as country from neighbour, oceanaccess, country where country.cid = neighbour.country and oceanaccess.cid = neighbour.neighbor group by country.cid) c3 ON c3.country = c2.country group by c2.country, c1.oid order by c2.country) c4 ON c0.cid = c4.country JOIN ocean o1 ON o1.oid = c4.oid) UNION (select cname, oname from oceanaccess natural join country natural join ocean) order by cname asc, oname desc);

-- Query 5 statements
create view RightYears as select * from hdi where year between 2009 and 2013;
create view  AvgScores  as select AVG(hdi_score), RightYears.cid from RightYears group by RightYears.cid;
INSERT INTO Query5 (select c.cname, A.cid, A.avghdi from AvgScores A JOIN country c ON c.cid = A.cid order by avghdi DESC limit 10);
drop view RightYears cascade;
drop view AvgScores cascade;

-- Query 6 statements
create view RightYears as select * from hdi where year between 2009 and 2013;
create view Compare as select r1.cid, r1.year as year1, r2.year as year2, r1.hdi_Score as score1, r2.hdi_score as score2 from  RightYears r1 JOIN RightYears r2 ON r1.cid=r2.cid AND r2.year-r1.year=1 order by cid, year1;
create view NotIncreasing as select c.cid from Compare c where c.score1>c.score2;
create view  Increasing as (select c.cid from Compare c) except (select * from  NotIncreasing);
INSERT INTO Query6 (select c.cid as cid, c.cname as cname from country c JOIN Increasing i ON c.cid = i.cid order by cname);

drop view RightYears cascade;
drop view Compare cascade;
drop view NotIncreasing cascade;
drop view Increasing cascade;

-- Query 7 statements
INSERT INTO Query7 (select rid, rname, sum(population*rpercentage) as followers from country natural join religion group by rid, rname order by followers DESC);

-- Query 8 statements
create view MostPopular as select * from (select max(lpercentage), l.cid from language l group by l.cid) t1 NATURAL JOIN language;
create view SameLanguage as select m1.cid as c1id, m2.cid as c2id, m1.lname from MostPopular m1 JOIN MostPopular m2 ON m1.cid != m2.cid and m1.lname = m2.lname;
create view Countries as select c1.cname as c1name, c2.cname as c2name, c1.cid as c1id, c2.cid as c2id from country c1, country c2  where c1.cid != c2.cid;
INSERT INTO Query8 (select c1name, c2name, lname from Countries c NATURAL JOIN SameLanguage s order by lname asc, c1name desc);

drop view MostPopular cascade;
drop view SameLanguage cascade;
drop view Countries cascade;

-- Query 9 statements
INSERT INTO Query9 (select cname, height as totalspan from country where height in (select max(c4.height) as maxheight from country c3 JOIN ((select c1.cid, c1.height from country c1 JOIN (select cid from country where cid not in (select cid from oceanaccess)) c2 ON c2.cid = c1.cid) UNION (select cid, abs(height-depth) from country natural join ocean natural join oceanaccess)) c4 ON c3.cid = c4.cid));

-- Query 10 statements
INSERT INTO Query10(select c1.cname, c2.borderslength from country c1 JOIN (select country, sum(length) as borderslength from neighbour group by country order by sum(length)) c2 ON c1.cid = c2.country order by c2.borderslength DESC LIMIT 1);

