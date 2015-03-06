-- CSC343 Assignment 2
-- Chaoyu Zhang g4zhange
-- Theresa Ma g2potato

-- Query 1 statements
INSERT INTO Query1(
SELECT country.cid as c1id, country.cname as c2name, h1.cid as c2id, h1.cname as c2name
FROM country JOIN (
SELECT n.country as c1id, max(c2.height) as max
FROM (neighbour n JOIN country c1 on c1.cid = n.country
JOIN country c2 on c2.cid = n.neighbor)
GROUP BY n.country
) as r1 on r1.c1id = country.cid
JOIN country h1 on h1.height = r1.max
ORDER BY country.cname);

-- Query 2 statements
INSERT INTO Query2(
SELECT cid, cname
FROM country
WHERE
cid NOT IN
(SELECT cid from oceanAccess)
ORDER BY cname);

-- Query 3 statements
INSERT INTO Query3(
SELECT P.country as c1id, c1.cname as c1name, neighbor as c2id, c2.cname as c2name
FROM 
	(SELECT country, count(neighbor) count
	FROM neighbour 
	GROUP BY country) P
	JOIN neighbour n on n.country = P.country
	JOIN country c1 on c1.cid = P.country
	JOIN country c2 on c2.cid = neighbor
WHERE P.count = 1
ORDER BY c1name);

-- Query 4 statements
INSERT INTO Query4(
SELECT * 
FROM (
SELECT cname, oname FROM oceanAccess NATURAL JOIN country NATURAL JOIN ocean
UNION
SELECT cname as country, ocean.oname as neighbourcoast
FROM oceanAccess oa1 JOIN neighbour n on n.country = oa1.cid
JOIN oceanAccess oa2 on oa2.cid = neighbor
JOIN country on country.cid = n.country
JOIN ocean on oa2.oid = ocean.oid
WHERE oa1.cid != neighbor
) oa3
ORDER BY oa3.cname ASC, oa3.oname DESC);

-- Query 5 statements
INSERT INTO Query5(
SELECT cid, cname, avg(hdi_score) as avghdi
FROM 
(
SELECT *
FROM hdi
WHERE year >= 2009 AND year <= 2013
) hd NATURAL JOIN country
GROUP BY cid, country.cname
ORDER BY avghdi DESC
LIMIT 10);

-- Query 6 statements
CREATE VIEW h09 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2009;

CREATE VIEW h10 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2010;

CREATE VIEW h11 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2011;

CREATE VIEW h12 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2012;

CREATE VIEW h13 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2013;

CREATE VIEW increase09to10 as
SELECT h09.cid
FROM h09, h10
WHERE h09.cid = h10.cid AND h09.hdi_score < h10.hdi_score;

CREATE VIEW increase10to11 as
SELECT h10.cid
FROM h10, h11
WHERE h10.cid = h11.cid AND h10.hdi_score < h11.hdi_score;

CREATE VIEW increase11to12 as
SELECT h11.cid
FROM h11, h12
WHERE h11.cid = h12.cid AND h11.hdi_score < h12.hdi_score;

CREATE VIEW increase12to13 as
SELECT h12.cid
FROM h12, h13
WHERE h12.cid = h13.cid AND h12.hdi_score < h13.hdi_score;

INSERT INTO Query6(
SELECT c.cid as cid, c.cname as cname
FROM country c,
    (SELECT DISTINCT cid
    FROM hdi
    WHERE cid IN (SELECT * FROM increase09to10) AND 
          cid IN (SELECT * FROM increase10to11) AND 
          cid IN (SELECT * FROM increase11to12) AND 
          cid IN (SELECT * FROM increase12to13)) increasing
WHERE increasing.cid = c.cid
ORDER BY c.cname);

DROP VIEW increase09to10;
DROP VIEW increase10to11;
DROP VIEW increase11to12;
DROP VIEW increase12to13;
DROP VIEW h09;
DROP VIEW h10;
DROP VIEW h11;
DROP VIEW h12;
DROP VIEW h13;

-- Query 7 statements
INSERT INTO Query7 
(SELECT rid, rname, sum(rpercentage * population) AS followers
FROM religion NATURAL JOIN country
GROUP BY rname, rid
ORDER BY followers DESC);

-- Query 8 statements
CREATE VIEW popularLang AS
SELECT lang.lname as popular, lang.cname as country, lang.cid
FROM
(
SELECT cid, cname, lname, lpercentage * population as langpop
FROM language NATURAL JOIN country ORDER BY cname, langpop DESC)
lang
WHERE langpop IN
(
SELECT max(lpercentage * population) as langpop
FROM language NATURAL JOIN country GROUP BY cname 
);

CREATE VIEW neighbouringC as
SELECT popularLang.popular, popularLang.country, c2.cname as neighbours
FROM
popularLang JOIN neighbour n on n.country = popularLang.cid
JOIN country c2 on c2.cid = neighbor;

INSERT INTO Query8
(SELECT pl.country as c1name, nc.country as c2name, pl.popular as lname
FROM
neighbouringC nc JOIN popularLang pl on nc.neighbours = pl.country
WHERE pl.popular = nc.popular
ORDER BY lname ASC, c1name DESC);

DROP VIEW neighbouringC;
DROP VIEW popularLang;

-- Query 9 statements
CREATE VIEW countryMaxDepth as
(SELECT cid, max(depth) as depth
FROM ocean o, oceanAccess oa
WHERE o.oid = oa.oid
GROUP BY cid)
            UNION
(SELECT cid, height-height as depth
FROM country
WHERE cid NOT IN (SELECT cid
                  FROM ocean o, oceanAccess oa
                  WHERE o.oid = oa.oid
                  GROUP BY cid));        

CREATE VIEW MaxSpan as
SELECT max(depth+height) as totalspan 
FROM country c, countryMaxDepth cm
WHERE c.cid = cm.cid;

CREATE VIEW countrySpan as
SELECT c.cid, depth+height as totalspan 
FROM country c, countryMaxDepth cm
WHERE c.cid = cm.cid;

CREATE VIEW cidMaxSpan as
SELECT cs.cid, cs.totalspan
FROM countrySpan cs, MaxSpan m
WHERE cs.totalspan = m.totalspan;

INSERT INTO Query9(
SELECT c.cname, cm.totalspan
FROM cidMaxSpan cm, country c
where cm.cid = c.cid);

DROP VIEW cidMaxSpan;
DROP VIEW countrySpan;
DROP VIEW MaxSpan;
DROP VIEW countryMaxDepth;

-- Query 10 statements
INSERT INTO Query10(
SELECT * 
FROM 
	(SELECT c.cname as cname, SUM(length) as sum
	FROM neighbour n JOIN country c on c.cid = n.country
	GROUP BY c.cname) borders
	WHERE sum IN
		(SELECT MAX (longest.sum) as borderslength
		FROM
			(SELECT c.cname as cname, SUM(length) as sum
			FROM neighbour n JOIN country c on c.cid = n.country
			GROUP BY c.cname) longest)
); 