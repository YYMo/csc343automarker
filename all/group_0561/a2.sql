-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW neighbours_with_heights AS
	SELECT country AS cid1, neighbor AS cid2, height
	FROM neighbour N, country C
	WHERE N.neighbor = C.cid;

CREATE VIEW height_pairs AS
	SELECT n.cid1, n.cid2, n.height FROM
	(
		SELECT cid1, max(height) AS highest
		FROM neighbours_with_heights
		GROUP BY cid1
	) AS x
	INNER JOIN neighbours_with_heights AS n
	ON (n.cid1 = x.cid1 AND n.height = x.highest);
	
INSERT INTO Query1 (
	SELECT v.cid AS c1id, v.cname AS c1name, w.cid AS c2id, w.cname AS c2name
	FROM country AS v, height_pairs AS u, country AS w
	WHERE v.cid = u.cid1 AND u.cid2 = w.cid
	ORDER BY v.cid ASC
);
	
DROP VIEW height_pairs;
DROP VIEW neighbours_with_heights;

-- Query 2 statements

CREATE VIEW landlocked AS
	SELECT * FROM country
	WHERE cid NOT IN
	(
		SELECT cid FROM oceanaccess
	);

INSERT INTO Query2 (
	SELECT cid, cname
	FROM landlocked
	ORDER BY cname ASC
);	

DROP VIEW landlocked;

-- Query 3 statements

CREATE VIEW landlocked AS
	SELECT * FROM country
	WHERE cid NOT IN
	(
		SELECT cid FROM oceanaccess
	);

CREATE VIEW enclosed AS
	SELECT l.cid
	FROM landlocked AS l, neighbour AS n
	WHERE l.cid = n.country
	GROUP BY l.cid
	HAVING count(*) = 1;

CREATE VIEW pairs AS
	SELECT cid AS c1id, neighbor AS c2id
	FROM enclosed AS e, neighbour AS n
	WHERE e.cid = n.country;

	
INSERT INTO Query3 (
	SELECT v.cid AS c1id, v.cname AS c1name, w.cid AS c2id, w.cname AS c2name
	FROM country AS v, pairs AS u, country AS w
	WHERE v.cid = u.c1id AND u.c2id = w.cid
	ORDER BY v.cname ASC
);

DROP VIEW pairs;
DROP VIEW enclosed;
DROP VIEW landlocked;

-- Query 4 statements

CREATE VIEW accessable_from_c1id AS
	select n.country as c1id,
		o1.oid as c1ocean,
		n.neighbor as c2id,
		o2.oid as c2ocean
	from neighbour as n,
		oceanaccess as o1,
		oceanaccess as o2
	where n.country = o1.cid and n.neighbor = o2.cid;

CREATE VIEW accessable_union AS
	(select c1id, c2ocean as ocean
		from accessable_from_c1id)
	union
	(select c1id, c1ocean as ocean
		from accessable_from_c1id)
	order by c1id;

INSERT INTO Query4 (
	select cname, oname
	from country as c, ocean as o, accessable_union as a
	where c.cid = a.c1id and o.oid = a.ocean
	order by cname ASC, oname DESC
);

DROP VIEW accessable_union;
DROP VIEW accessable_from_c1id;

-- Query 5 statements

create view avghdi_view as
	select cid, avg(hdi_score) as avghdi
	from hdi
	where year >= 2009 and year <= 2013
	group by cid;
	
INSERT INTO Query5 (
	select c.cid, cname, avghdi
	from country as c, avghdi_view as h
	where c.cid = h.cid
	order by avghdi desc
	limit 10
);
	
DROP VIEW avghdi_view;

-- Query 6 statements

CREATE VIEW first_differences as
	select a.cid, a.year, b.hdi_score - a.hdi_score as diff
	from hdi as a, hdi as b
	where a.cid = b.cid
		and a.year = b.year-1
		and a.year >= 2009
		and a.year <= 2013;

INSERT INTO Query6 (
	select cid, cname
	from country
	where cid not in (
		select cid
		from first_differences
		where diff <= 0
	)
	order by cname asc
);

DROP VIEW first_differences;

-- Query 7 statements

INSERT INTO Query7 (
	select rid, rname, round(sum(population*rpercentage/100)) as followers
	from religion, country
	where country.cid = religion.cid
	group by rid, rname
	order by followers desc
);

-- Query 8 statements

CREATE VIEW cid_with_poplang AS
	select c.cid, c.lid
	FROM (
		select cid, max(lpercentage) as lpercentage
		from language
		group by cid
	) as x
	inner join language as c
	on (x.cid = c.cid and x.lpercentage = c.lpercentage);

CREATE VIEW lang_pairs AS
	select c1.cid as c1id, c2.cid as c2id, c1.lid
	from neighbour as n, cid_with_poplang as c1, cid_with_poplang as c2
	where n.country = c1.cid and n.neighbor = c2.cid and c1.lid = c2.lid;

CREATE VIEW languages AS
	select distinct lid, lname
	from language
	order by lid;

INSERT INTO Query8 (
	select c1.cname as c1name, c2.cname as c2name, lname
	from country as c1, country as c2, languages as l, lang_pairs as p
	where c1.cid = p.c1id and c2.cid = p.c2id and l.lid = p.lid
	order by lname asc, c1name desc
);

DROP VIEW languages;
DROP VIEW lang_pairs;
DROP VIEW cid_with_poplang;

-- Query 9 statements

CREATE VIEW country_ocean AS
	SELECT c.cid, max(depth) AS maxdepth
	FROM country AS c, oceanaccess AS o, ocean AS o2
	WHERE c.cid = o.cid AND o.oid = o2.oid
	GROUP BY c.cid;
	
INSERT INTO Query9 (	
	SELECT c.cname, abs(COALESCE(o.maxdepth, 0) - c.height) AS totalspan
	FROM country AS c
	FULL OUTER JOIN country_ocean AS o
	ON c.cid = o.cid
	ORDER BY totalspan DESC
	LIMIT 1
);
	
DROP VIEW country_ocean;

-- Query 10 statements

INSERT INTO Query10 (
	SELECT cname, sum(length) AS borderslength
	FROM neighbour n, country c
	WHERE n.country = c.cid
	GROUP BY cname
	order by borderslength desc
	limit 1
)
