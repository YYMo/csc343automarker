-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1 (SELECT c.cid AS c1id, c.cname AS c1name, n.cid AS c2id, n.cname AS c2name
FROM
(SELECT c1.cid AS cid, c1.cname AS cname, n1.neighbor AS neighbor FROM neighbour n1, country c1 WHERE n1.country=c1.cid) c,
(SELECT c2.cid AS cid, c2.cname AS cname, c2.height AS height FROM neighbour n2, country c2 WHERE n2.neighbor=c2.cid) n 
WHERE c.neighbor=n.cid 
AND n.height=(SELECT MAX(n0.height) 
FROM
(SELECT c3.cid AS cid, c3.cname AS cname, n3.neighbor AS neighbor FROM neighbour n3, country c3 WHERE n3.country=c3.cid) c0,
(SELECT c4.cid AS cid, c4.cname AS cname, c4.height AS height FROM neighbour n4, country c4 WHERE n4.neighbor=c4.cid) n0 
WHERE c0.neighbor=n0.cid GROUP BY c0.cid)
ORDER BY c1name ASC);



-- Query 2 statements

INSERT INTO Query2 (SELECT cid, cname
FROM country
WHERE NOT EXISTS(SELECT * FROM oceanAccess WHERE country.cid = oceanAccess.cid)
ORDER BY cname ASC);


-- Query 3 statements

INSERT INTO Query3 (SELECT cid, cname
FROM country
WHERE NOT EXISTS(SELECT * FROM oceanAccess WHERE country.cid = oceanAccess.cid) AND 1=(SELECT count(neighbor) FROM neighbour WHERE country.cid=neighbour.country GROUP BY country.cid)
ORDER BY cname ASC);


-- Query 4 statements


--SELECT cid, oid FROM oceanAccess WHERE EXISTS(SELECT * FROM country WHERE oceanAccess.cid=country.cid;

--SELECT country AS cid, oid FROM oceanAccess, neighbour WHERE cid=neighbor;


-- Query 5 statements

INSERT INTO Query5 (SELECT c.cid, c.cname, h.score AS avghdi
FROM
(SELECT a.cid, AVG(hdi_score) AS score FROM hdi a WHERE a.year>=2009 AND a.year <=2013 GROUP BY a.cid ORDER BY AVG(a.hdi_score) DESC LIMIT 10) h,
country c
WHERE h.cid=c.cid);

-- Query 6 statements
INSERT INTO Query6 (SELECT c.cid, c.cname
FROM country c,
(SELECT cid FROM hdi WHERE year>=2009 AND year<=2013 GROUP BY cid HAVING count(cid)=5) sh
WHERE sh.cid=c.cid 
AND (SELECT hdi_score FROM hdi WHERE year=2009 and hdi.cid=sh.cid) < (SELECT hdi_score FROM hdi WHERE year=2010 and hdi.cid=sh.cid)
AND (SELECT hdi_score FROM hdi WHERE year=2010 and hdi.cid=sh.cid) < (SELECT hdi_score FROM hdi WHERE year=2011 and hdi.cid=sh.cid)
AND (SELECT hdi_score FROM hdi WHERE year=2011 and hdi.cid=sh.cid) < (SELECT hdi_score FROM hdi WHERE year=2012 and hdi.cid=sh.cid)
AND (SELECT hdi_score FROM hdi WHERE year=2012 and hdi.cid=sh.cid) < (SELECT hdi_score FROM hdi WHERE year=2013 and hdi.cid=sh.cid)
ORDER BY cname ASC);

-- Query 7 statements



-- Query 8 statements


-- SELECT * FROM country c, neighbour n, (SELECT m.cid AS cid, l.lname AS lname FROM (SELECT cid, MAX(lpercentage) AS maxpercent FROM language GROUP BY cid) m, language l WHERE m.cid=l.cid AND l.lpercentage=m.maxpercent) pop;

-- Query 9 statements



-- Query 10 statements

INSERT INTO Query10 (SELECT c.cname, b.border AS borderslength
FROM
(SELECT cid, sum(length) AS border FROM neighbour, country WHERE cid=country GROUP BY cid ORDER BY sum(length) DESC LIMIT 1) b,
country c
WHERE c.cid=b.cid
LIMIT 1);