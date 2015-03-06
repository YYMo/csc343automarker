CREATE VIEW neighbourheights AS
        SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name, c2.height AS c2height
        FROM neighbour AS n, country AS c1, country AS c2
        WHERE n.country = c1.cid AND n.neighbor = c2.cid;

INSERT INTO Query1(

SELECT c1id, c1name, c2id, c2name
FROM neighbourheights WHERE
(c1id, c2height) IN
(SELECT c1id, MAX(c2height) FROM neighbourheights GROUP BY c1id) ORDER BY c1name


);

-- Query 2 statements
INSERT INTO Query2(

SELECT cid, cname
FROM country
WHERE cid NOT IN (SELECT cid FROM oceanAccess) ORDER BY cname

);

CREATE VIEW landlocked AS
        SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name
        FROM neighbour AS n, country AS c1, country AS c2
        WHERE (n.country = c1.cid) AND (n.neighbor = c2.cid) AND (c1.cid NOT IN(SELECT cid FROM oceanAccess));
        
-- Query 3 statements
INSERT INTO Query3(
        
SELECT * FROM landlocked
WHERE c1id IN (SELECT c1id FROM landlocked GROUP BY c1id HAVING COUNT(c1id) = 1) ORDER BY c1name

);

CREATE VIEW directocean AS
        SELECT cid, oid
        FROM country NATURAL JOIN oceanAccess;

CREATE VIEW undirectocean AS
        SELECT n.country, oa.oid
        FROM neighbour AS n, oceanAccess AS oa
        WHERE n.neighbor = oa.cid AND 
        n.neighbor IN (SELECT cid FROM oceanAccess);

CREATE VIEW total AS
        (SELECT * FROM directocean) UNION (SELECT * FROM undirectocean);
        
-- Query 4 statements
INSERT INTO Query4(

SELECT cname , oname
FROM total NATURAL JOIN country NATURAL JOIN ocean
ORDER BY cname ASC, oname DESC
);

CREATE VIEW period AS
        SELECT *
        FROM hdi
        WHERE 2009 <= year AND year <= 2013;

CREATE VIEW top10 AS
        SELECT cid, AVG(hdi_score) AS avghdi
        FROM period
        GROUP BY cid
        ORDER BY avghdi DESC
        LIMIT 10;

-- Query 5 statements
INSERT INTO Query5(

SELECT Distinct cid, cname, avghdi
FROM top10 NATURAL JOIN country

);

CREATE VIEW compare AS
        SELECT p1.cid
        FROM period AS p1, period AS p2
        WHERE (p1.cid = p2.cid) AND (p2.year - p1.year = 1) AND (p2.hdi_score > p1.hdi_score);

CREATE VIEW satisfy AS
        SELECT cid
        FROM compare
        GROUP BY cid
        HAVING COUNT(cid) = 4;
        
INSERT INTO Query6(

SELECT Distinct cid, cname
FROM satisfy NATURAL JOIN country
ORDER BY cname ASC

);

CREATE VIEW number AS
        SELECT rid,  population * rpercentage AS follower
        FROM religion NATURAL JOIN country;
        
CREATE VIEW religionnumber AS
        SELECT rid, SUM(follower) AS followers
		FROM number GROUP BY rid;
        
-- Query 7 statements
INSERT INTO Query7(

SELECT DISTINCT rid, rname, followers FROM
religionnumber NATURAL JOIN religion
order by followers DESC

);

CREATE VIEW most AS
        SELECT cid, lid, lname
        FROM language
        WHERE (cid, lpercentage) IN
        (SELECT cid, MAX(lpercentage)
        FROM language
        GROUP BY cid);

CREATE VIEW neighlanguage AS
        SELECT m1.cid AS m1id, m2.cid AS m2id, m1.lname AS lname
        FROM most AS m1, most AS m2, neighbour AS n
        WHERE m1.cid = n.country AND m2.cid = n.neighbor
        AND m1.lid = m2.lid;

-- Query 8 statements
INSERT INTO Query8(

SELECT c1.cname AS c1name, c2.cname AS c2name, n.lname AS lname
FROM neighlanguage AS n, country AS c1, country AS c2
WHERE n.m1id = c1.cid AND n.m2id = c2.cid
ORDER BY lname ASC, c1name DESC

);

CREATE VIEW depest AS
        SELECT cid, MAX(depth) AS depth
        FROM oceanAccess NATURAL JOIN ocean
        GROUP BY cid;

CREATE VIEW span1 AS
        SELECT cid, (height + depth) AS totalspan
        FROM depest NATURAL JOIN country;

CREATE VIEW span2 AS
        SELECT c.cid, c.height AS totalspan
        FROM country AS c
        WHERE c.cid NOT IN
        (SELECT cid FROM oceanAccess);
        
CREATE VIEW maxspan AS
		SELECT MAX(s.totalspan) AS totalspan
		FROM ((SELECT * FROM span1) UNION (SELECT * FROM span2)) AS s;

-- Query 9 statements
INSERT INTO Query9(

	SELECT cname, s.totalspan AS totalspan
	FROM ((SELECT * FROM span1) UNION (SELECT * FROM span2)) AS s NATURAL JOIN country
	WHERE s.totalspan in (SELECT * FROM maxspan)

);

CREATE VIEW total1 AS
        SELECT country AS cid, SUM(length) AS borderslength
        FROM neighbour
        GROUP BY country;

CREATE VIEW total2 AS
        SELECT cid, 0 AS borderslength
        FROM country
        WHERE cid NOT IN
        (SELECT country FROM neighbour);
        
-- Query 10 statements
INSERT INTO Query10(
	SELECT country.cname, t.borderslength AS borderslength
	FROM ((SELECT * FROM total1) UNION (SELECT * FROM total2)) AS t NATURAL JOIN country
	WHERE t.borderslength in 
	(SELECT MAX(u.borderslength) FROM ((SELECT * FROM total1) UNION (SELECT * FROM total2)) AS u)


);

DROP VIEW neighbourheights CASCADE;
DROP VIEW landlocked CASCADE;
DROP VIEW directocean CASCADE;
DROP VIEW undirectocean CASCADE;
DROP VIEW period CASCADE;
DROP VIEW number CASCADE;
DROP VIEW most CASCADE;
DROP VIEW depest CASCADE;
DROP VIEW span2 CASCADE;
DROP VIEW total1 CASCADE;
DROP VIEW total2 CASCADE;

