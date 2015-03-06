-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.



-- Query 1 statements
INSERT INTO Query1(
	SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name
	FROM country c1, neighbour n, country c2
	WHERE c1.cid = n.country and c2.cid = n.neighbor and c2.height >= all
		(
		SELECT c3.height
		FROM country c3, neighbour n1
		WHERE c3.cid = n1.neighbor and n1.country = c1.cid
		)
);









--Query 2 statements
INSERT INTO Query2(
 SELECT cid, cname
	FROM country	
	WHERE cid != all(
		SELECT cid
		FROM oceanAccess
		) 
	ORDER BY cname ASC
);









-- Query 3 statements
INSERT INTO Query3(
 SELECT c1.cid as c1id, c1.cname as c1name, c2.cid as c2id, c2.cname as c2name
 FROM country c1, country c2, (
	SELECT lc.cid as cid, c3.cid as nid
	FROM Query2 lc, neighbour n, country c3
	WHERE lc.cid = n.country and c3.cid = n.neighbor and lc.cid != all(
		SELECT n1.country as cid
		FROM neighbour n1, neighbour n2
		WHERE n1.country = n2.country and n1.neighbor != n2.neighbor
		)
 ) an
 WHERE c1.cid = an.cid and c2.cid = an.nid
 ORDER BY c1name ASC
);








--Query 4 statements
INSERT INTO Query4(
SELECT co.cname as cname, o.oname as oname
FROM ocean o, (
	SELECT c.cname as cname, oa.oid
	FROM oceanAccess oa, neighbour n, country c
	WHERE oa.cid = n.country and n.neighbor = c.cid
	UNION
	SELECT c1.cname as cname, oa1.oid
	FROM oceanAccess oa1, country c1
	WHERE oa1.cid = c1.cid
	) co
WHERE co.oid = o.oid
ORDER BY cname ASC, oname DESC
);








--Query 5 statements
INSERT INTO Query5(
SELECT c.cid, c.cname, avg(h.hdi_score) as avghdi
FROM country c, hdi h
WHERE c.cid = h.cid and h.year >= 2009 and h.year <= 2013
GROUP BY c.cid, c.cname
ORDER BY avghdi DESC LIMIT 10
);










	
--Query 6 statements

CREATE VIEW five_year AS
SELECT h.year, c.cid, c.cname, h.hdi_score as hdi
FROM hdi h, country c 
WHERE h.cid = c.cid and h.year >= 2009 and h.year <= 2013;


INSERT INTO Query6(
SELECT DISTINCT c.cid, c.cname
FROM country c, hdi h
WHERE c.cid = h.cid and c.cid != all (
	SELECT DISTINCT f1.cid
	FROM five_year f1, five_year f2
	WHERE f1.cid = f2.cid and f1.year > f2.year and f1.hdi < f2.hdi
	)
ORDER BY cname ASC
);


--Query 7 statements
INSERT INTO Query7(
SELECT r.rid, r.rname, sum(c.population * r.rpercentage) as followers
FROM religion r, country c
WHERE r.cid = c.cid
GROUP BY r.rid, r.rname
ORDER BY followers DESC
);








--Query 8 statements
CREATE VIEW country_language AS
SELECT * 
FROM country c
	NATURAL JOIN
	(
	SELECT l.cid, max(l.lpercentage) as lpercentage
	FROM language l
	GROUP BY l.cid
	) mp
	NATURAL JOIN 
	(
	SELECT l1.cid, l1.lid, l1.lname, l1.lpercentage
	FROM language l1
	) mp1
;

INSERT INTO Query8(
SELECT c1.cname as c1name, c2.cname as c2name, c1.lname
FROM neighbour n, country_language c1, country_language c2
WHERE n.country = c1.cid and n.neighbor = c2.cid and c1.lname = c2.lname
ORDER BY lname ASC, c1name DESC
);











--Query 9 statements
CREATE VIEW name_span AS
SELECT c.cname, MAX(o.depth + c.height) as totalspan
FROM oceanAccess oa, ocean o, country c
WHERE oa.cid = c.cid and oa.oid = o.oid
GROUP BY c.cname
UNION
SELECT c1.cname, c1.height as totalspan
FROM Query2 q2, country c1
WHERE q2.cid = c1.cid;


INSERT INTO Query9(
SELECT ns.cname, ns.totalspan
FROM name_span ns NATURAL JOIN
	(
	SELECT MAX(ns1.totalspan) as totalspan
	FROM name_span ns1
	) nn
)
;









--Query 10 statements

CREATE VIEW cid_len AS
SELECT n.country as cid, sum(n.length) as borderslength
FROM neighbour n
GROUP BY n.country;

INSERT INTO Query10(
SELECT c.cname, cl.borderslength
FROM cid_len cl, country c, 
	(
	SELECT MAX(c1.borderslength) as borderslength
	FROM cid_len c1
	) b
WHERE cl.cid = c.cid and cl.borderslength = b.borderslength
);

DROP VIEW five_year;
DROP VIEW name_span;
DROP VIEW cid_len;
DROP VIEW country_language;