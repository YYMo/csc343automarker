-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

INSERT INTO Query1(SELECT c1id, cname AS c1name, c2id, c2name
FROM country, (SELECT neighbor AS c2id, cname AS c2name, country AS c1id
FROM (SELECT *
FROM country, neighbour
WHERE cid=neighbor) neb
WHERE neb.height >= ALL (SELECT height
                        FROM country, neighbour
						WHERE cid=neighbor AND neb.country = country)) ax
WHERE ax.c1id=cid
ORDER BY c1name);

-- Query 2 statements

INSERT INTO Query2(SELECT cid, cname FROM country WHERE cid NOT IN
(SELECT cid FROM (oceanAccess NATURAL JOIN country))
ORDER BY cname);


-- Query 3 statements

INSERT INTO Query3(SELECT n.c1id, n.c1name, n.c2id, n.c2name
FROM (SELECT c1id, c1name, c2id, c2name, count(c2id) AS numb
FROM 
(SELECT country AS c1id, neighbor AS c2id, cname AS c2name
FROM country, neighbour
WHERE cid=neighbor) nei, (SELECT cid, cname AS c1name FROM country WHERE cid NOT IN
(SELECT cid FROM (oceanAccess NATURAL JOIN country))) noto 
WHERE nei.c1id=noto.cid
GROUP BY c1id) n
WHERE numb=1
ORDER BY n.c1name);

-- Query 4 statements

INSERT INTO Query4((SELECT nei.cname, oname
FROM (SELECT cid AS dcid, oname, cname
FROM ((oceanAccess NATURAL JOIN ocean) NATURAL JOIN country)) dc, 
(SELECT * FROM country, neighbour
WHERE cid=neighbor) nei
WHERE nei.country = dc.dcid
)
UNION
(SELECT cname, oname
FROM ((oceanAccess NATURAL JOIN ocean) NATURAL JOIN country))
ORDER BY cname ASC, oname DESC);

-- Query 5 statements

INSERT INTO Query5(SELECT cid, cname, avg(hdi_score) AS avghdi
FROM (country NATURAL JOIN hdi)
WHERE year >=2009 AND year <= 2013
GROUP BY cid
ORDER BY avghdi DESC
LIMIT 3);

-- Query 6 statements

INSERT INTO Query6(SELECT country.cid AS cid, cname
FROM country, (
		SELECT cid
		FROM hdi
		WHERE cid NOT IN (
			SELECT h1.cid
			FROM hdi h1, hdi h2
			WHERE h1.cid = h2.cid AND h1.year >=2009 AND h1.year <= 2013
			AND h2.year>=2009 AND h2.year <=2013
			AND h1.year > h2.year AND h1.hdi_score <= h2.hdi_score)) r1
WHERE country.cid = r1.cid
GROUP BY country.cid
ORDER BY cname);


-- Query 7 statements

INSERT INTO Query7(SELECT m.rid , m.rname, sum(m.f) AS followers
FROM (SELECT *, population*rpercentage AS f
FROM (country NATURAL JOIN religion))m
GROUP BY rid
ORDER BY followers DESC);

-- Query 8 statements

INSERT INTO Query8(SELECT r1.coname AS c1name, r1.nename AS c2name, r1.colname AS lname
FROM (SELECT coid, coname, neid, nename, la1.lname AS colname , la1.lpercentage AS colp, la2.lname AS nelname , la2.lpercentage AS nelp
FROM (SELECT n1.country AS coid, c2.cname AS coname, n1.neighbor AS neid, c1.cname AS nename
FROM country c1, country c2, neighbour n1
WHERE c1.cid=n1.neighbor AND c2.cid=n1.country AND c2.cid < c1.cid) nei,
(SELECT p1.cid,p1.cname, p1.lname, p1.lpercentage
FROM (SELECT *
FROM (country NATURAL JOIN language)) p1
WHERE p1.lpercentage >= ALL (SELECT lpercentage
							FROM (country NATURAL JOIN language)
                             WHERE p1.cid=cid
                            )) la1, (SELECT p1.cid,p1.cname, p1.lname, p1.lpercentage
FROM (SELECT *
FROM (country NATURAL JOIN language)) p1
WHERE p1.lpercentage >= ALL (SELECT lpercentage
							FROM (country NATURAL JOIN language)
                             WHERE p1.cid=cid
                            )) la2
WHERE nei.coid=la1.cid AND nei.neid=la2.cid) r1
WHERE r1.colname=r1.nelname
ORDER BY lname ASC, c1name DESC);

-- Query 9 statements

INSERT INTO Query9((SELECT c1.cname, c1.height AS totalspan
FROM country c1
WHERE c1.cid NOT IN (
    SELECT cid
    FROM oceanAccess))
UNION
(SELECT c1.cname, c1.height+o1.depth AS totalspan
FROM country c1, oceanAccess oa1, ocean o1
WHERE c1.cid = oa1.cid AND o1.oid= oa1.oid));

-- Query 10 statements

INSERT INTO Query10(SELECT cname, bld AS borderslength
FROM (SELECT cname, sum(length) AS bld
FROM country, neighbour
WHERE cid=neighbor
GROUP BY cname) r1
WHERE r1.bld >= ALL (SELECT sum(length) AS bld
                                  FROM country, neighbour
                                  WHERE cid=neighbor
                    			GROUP BY cname));
