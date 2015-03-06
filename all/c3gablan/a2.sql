-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW Nheight (country, neighbor) as
	select neighbour.country as country, neighbour.neighbor as neighbor, max(height)
	from country, neighbour
	where country.cid = neighbour.neighbor
	group by neighbour.country, neighbour.neighbor;

INSERT INTO Query1(
	select h.country as c1id, a.cname as c1name, h.neighbor as c2id, b.cname as c2name
	from Nheight h, country a, country b
	where h.country = a.cid and h.neighbor = b.cid
	);
DROP VIEW Nheight;


-- Query 2 statements
INSERT INTO Query2(
	select cid, cname
	from country natural join ( select cid from country
								except
								select cid from oceanaccess
								) as a
	order by cname ASC
);


-- Query 3 statements
CREATE VIEW Landlock as
	select cid, cname
	from country natural join ( select cid from country
								except
								select cid from oceanaccess
								) as a
	order by cname ASC;

CREATE VIEW Surrounded as
	select country, neighbor
	from Landlock, neighbour
	where Landlock.cid = neighbour.country
	group by country, neighbor
	having count(neighbor) = 1;

INSERT INTO Query3(
	select country as c1id, c0.cname as c1name, neighbor as c2id, c1.cname as c2name
	from Surrounded, country c0, country c1
	where country = c0.cid and neighbor = c1.cid
);
DROP VIEW Surrounded;
DROP VIEW Landlock CASCADE;


-- Query 4 statements
CREATE VIEW Landddd as 
	select cid from country
	except
	select cid from oceanaccess;

CREATE VIEW Nocean as
	select neighbour.country as ctr, oid
	from Landddd, neighbour, oceanaccess
	where Landddd.cid = neighbour.country and
			neighbour.neighbor = oceanaccess.cid;

INSERT INTO Query4(
	select cname, oname
	from Nocean natural join oceanaccess natural join country natural join ocean
	order by cname ASC, oname DESC
);
DROP VIEW Nocean;
DROP VIEW Landddd CASCADE;


-- Query 5 statements
CREATE VIEW Fiveyr as
	select *
	from hdi
	where year >= 2009 and year <= 2013;

CREATE VIEW Avehdi as
	select cid, avg(hdi_score) as avghdi
	from Fiveyr
	group by cid
	order by avghdi DESC;

INSERT INTO Query5(
	select cid, cname, avghdi
	from country natural join Avehdi limit 10
);
DROP VIEW Avehdi CASCADE;
DROP VIEW Fiveyr CASCADE;


-- Query 6 statements
INSERT INTO Query6(
	select cid, cname
	from country natural join ( select a.cid as cid
								from hdi a, hdi b, hdi c, hdi d, hdi e
								where a.year=2009 and b.year=2010 and c.year=2011 and d.year=2012 and e.year=2012
									and a.hdi_score<b.hdi_score and b.hdi_score<c.hdi_score and c.hdi_score<d.hdi_score
									and d.hdi_score<e.hdi_score
								) as t
	order by cname ASC
);


-- Query 7 statements
create view Pcount as
	select cid, rid, rname, (rpercentage*population/100) as count
	from religion natural join country
	group by cid, rid, rname, population
	order by count DESC;

INSERT INTO Query7(
	select rid, rname, sum(count) as followers
	from Pcount
	group by rid, rname
	order by followers DESC
);
DROP VIEW Pcount;


-- Query 8 statements
create view Favelang as
	select cid, lname, max(lpercentage) as max
	from language
	group by cid, lname;

create view Pairs as
	select a.cid as a, b.cid as b, a.lname as lname
	from Favelang a, Favelang b
	where a.lname = b.lname and a.cid <> b.cid;

INSERT INTO Query8(
	select e.cname as c1name, f.cname as c2name, lname
	from Pairs, country e, country f
	where a = e.cid and b = f.cid
	order by lname ASC, c1name DESC
);
DROP VIEW Pairs CASCADE;
DROP VIEW Favelang CASCADE;


-- Query 9 statements
INSERT INTO Query9(
	select cid, (height - depth) as totalspan
	from country natural join oceanaccess natural join ocean
	UNION
	select cid, height as totalspan
	from country natural join (select cid from country
								EXCEPT
								select cid from oceanaccess
								) as a
);


-- Query 10 statements
INSERT INTO Query10(
	select cname, max(len) as borderslength
	from country, ( select country, sum(length) as len
					from neighbour
					group by country
					) as a
	where country.cid = a.country
	group by cname
);

