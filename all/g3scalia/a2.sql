-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1(SELECT country.cid AS "c1id", country.cname AS "c1name", neighbour.neighbor AS "c2id", cnew.cname AS "c2name"
FROM country, neighbour, country AS cnew
WHERE neighbour.country = country.cid and neighbour.neighbor = cnew.cid and neighbour.neighbor
NOT IN (SELECT n1.neighbor FROM neighbour AS n1, neighbour AS n2, country as c1, country AS c2, country AS c3 WHERE n1.neighbor = c1.cid and n2.neighbor = c2.cid and n1.country = c3.cid and n2.country = c3.cid and c1.height < c2.height) ORDER BY country.cid ASC);



-- Query 2 statements
INSERT INTO Query2(SELECT country.cid AS "cid", country.cname AS "cname"
FROM country WHERE country.cid
NOT IN (SELECT oceanAccess.cid FROM oceanAccess) ORDER BY country.cname ASC);



-- Query 3 statements
INSERT INTO Query3(SELECT c1.cid AS "c1id", c1.cname AS "c1name",  c2.cid AS "c2id", c2.cname AS "c2name"
FROM country AS c1, neighbour, country AS c2
WHERE c1.cid = neighbour.country and c2.cid = neighbour.neighbor and c1.cid
NOT IN (SELECT oceanAccess.cid FROM oceanAccess) and c1.cid
IN (SELECT country.cid FROM neighbour, country WHERE country.cid = neighbour.country GROUP BY country.cid HAVING COUNT(neighbor) = 1) ORDER BY c1.cname ASC);


-- Query 4 statements
INSERT INTO Query4(SELECT c.cname AS "cname", o.oname AS "oname"
FROM country AS c, neighbour AS n, ocean AS o, oceanAccess AS oA
WHERE (o.oid = oA.oid and oA.cid = c.cid) OR (c.cid = n.country and n.neighbor = oA.cid and o.oid = oA.cid) GROUP BY o.oname, c.cname ORDER BY c.cname ASC, o.oname DESC);



-- Query 5 statements
INSERT INTO Query5((SELECT country.cid AS "cid", country.cname AS "cname", AVG(hdi.hdi_score) AS "avghdi" FROM hdi, country WHERE country.cid = hdi.cid and hdi.year > 2008 and hdi.year < 2014 GROUP BY country.cid ORDER BY AVG(hdi.hdi_score) DESC) LIMIT 10);


-- Query 6 statements
INSERT INTO Query6(SELECT country.cid AS "cid", country.cname AS "cname"
FROM hdi AS h1, hdi AS h2, hdi AS h3, hdi AS h4, hdi AS h5, country
WHERE country.cid = h1.cid and h1.cid = h2.cid and h2.cid = h3.cid and h3.cid = h4.cid and h4.cid = h5.cid and h1.year = 2009 and h2.year = 2010 and h3.year = 2011 and h4.year = 2012 and h5.year = 2013 and h1.hdi_score < h2.hdi_score and h2.hdi_score < h3.hdi_score and h3.hdi_score < h4.hdi_score and h4.hdi_score < h5.hdi_score ORDER BY country.cname ASC);


-- Query 7 statements
INSERT INTO Query7(SELECT r1.cid AS "rid", r1.rname AS "rname", SUM(r1.rpercentage * c1.population) AS "followers" FROM religion AS r1, country AS c1 WHERE c1.cid = r1.cid GROUP BY r1.cid, r1.rname ORDER BY  SUM(r1.rpercentage * c1.population) DESC);


-- Query 8 statements
INSERT INTO Query8((SELECT lang1.cname AS "c1name", lang2.cname AS "c2name", lang1.name AS "lname"
FROM (SELECT p.cid AS "cid", lang.lname AS "name", p.name AS "cname"
FROM language AS lang,
(SELECT country.cid AS "cid", MAX(l1.lpercentage) AS "percent", country.cname AS "name"
FROM country, language AS l1, language AS l2
WHERE country.cid = l1.cid GROUP BY country.cid) p
WHERE lang.cid = p.cid and lang.lpercentage = p.percent) lang1,
(SELECT p.cid AS "cid", lang.lname AS "name", p.name AS "cname"
FROM language AS lang,
(SELECT country.cid AS "cid", MAX(l1.lpercentage) AS "percent", country.cname AS "name"
FROM country, language AS l1, language AS l2
WHERE country.cid = l1.cid GROUP BY country.cid) p
WHERE lang.cid = p.cid and lang.lpercentage = p.percent) lang2, neighbour AS n
WHERE lang1.cid = n.country and lang2.cid = n.neighbor and lang1.cid <> lang2.cid and lang1.name = lang2.name)ORDER BY lang1.name ASC, lang1.cname DESC);



-- Query 9 statements
INSERT INTO Query9(SELECT c.cname AS "cname", MAX(c.height + oceand.odepth) AS "totalspan"
FROM country AS c,
(SELECT c.cid AS "cid", case WHEN c.cid NOT IN (SELECT cid FROM oceanAccess) then 0
WHEN MAX(ocean.depth) > 0 then MAX(ocean.depth) END "odepth"
FROM country AS c, oceanAccess AS oA, ocean
WHERE case WHEN c.cid NOT IN (SELECT cid FROM oceanAccess) then oA.oid = ocean.oid ELSE c.cid = oA.cid and oA.oid = ocean.oid END GROUP BY c.cid) oceand
WHERE c.cid = oceand.cid GROUP BY c.cname ORDER BY MAX(c.height + oceand.odepth) DESC LIMIT 1);



-- Query 10 statements
INSERT INTO Query10(SELECT c.cname AS "cname", SUM(n.length) AS "borderslength"
FROM country AS c, neighbour AS n
WHERE c.cid = n.country GROUP BY c.cid ORDER BY SUM(n.length) DESC LIMIT 1);



