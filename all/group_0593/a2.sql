-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW tallest AS SELECT cid, neighbor, max(height) FROM neighbour, country WHERE neighbor = cid GROUP BY cid, neighbor;

INSERT INTO Query1(SELECT c1.cid AS c1id, c1.cname AS c1name, t.neighbor AS c2id, c2.cname AS c2name FROM tallest t, country AS c1, country AS c2 WHERE c1.cid = t.cid AND t.neighbor = c2.cid ORDER BY c1name);

DROP VIEW tallest;

-- Query 2 statements

INSERT INTO Query2(SELECT cid, cname FROM country WHERE NOT EXISTS (SELECT cid from oceanAccess) ORDER BY cname);

-- Query 3 statements

CREATE VIEW land AS SELECT cid, cname FROM country WHERE NOT EXISTS (SELECT cid FROM oceanAccess);
  
INSERT INTO Query3(SELECT l.cid AS c1id, l.cname AS c1name, n.neighbor AS c2id, c.cname AS c2name FROM land l, neighbour n, country c WHERE l.cid = n.country AND n.neighbor = c.cid AND EXISTS (SELECT country FROM neighbour GROUP BY country HAVING COUNT(neighbor) = 1) ORDER BY c1name);

DROP VIEW land;

-- Query 4 statements

INSERT INTO Query4(SELECT c.cname AS cname, o.oname AS oname FROM neighbour n, ocean o, oceanAccess a, country c WHERE c.cid = n.country AND n.neighbor = a.cid AND a.cid = o.oid UNION SELECT c.cname AS cname, o.oname AS oname FROM neighbour n, ocean o, oceanAccess a, country c WHERE c.cid = a.cid AND a.oid = o.oid ORDER BY cname, oname DESC);

-- Query 5 statements

INSERT INTO Query5(SELECT hdi.cid AS cid, c.cname AS cname, AVG(hdi_score) AS avghdi FROM hdi, country c WHERE year <= 2013 AND year >= 2009 AND c.cid = hdi.cid GROUP BY hdi.cid, c.cname ORDER BY avghdi DESC LIMIT 10);

-- Query 6 statements

INSERT INTO Query6(SELECT h1.cid AS cid FROM hdi h1, hdi h2, hdi h3, hdi h4, hdi h5, COUNTRY c WHERE h1.cid = h2.cid AND h2.cid = h3.cid AND h3.cid= h4.cid AND h4.cid = h5.cid AND h1.year = 2009 AND h2.year = 2010 AND h3.year = 2011 AND h4.year = 2012 AND h5.year = 2013 AND h1.hdi_score < h2.hdi_score AND h2.hdi_score < h3.hdi_score AND h3.hdi_score < h4.hdi_score AND h4.hdi_score < h5.hdi_score);

-- Query 7 statements

INSERT INTO Query7(SELECT r.rid, r.rname, sum(rpercentage/100*(SELECT SUM(population) FROM country c WHERE r.cid = c.cid)) AS followers FROM religion r, country c WHERE r.cid = c.cid GROUP BY r.rid, r.rname ORDER BY followers DESC);

-- Query 8 statements

CREATE VIEW popular AS SELECT DISTINCT cid, lname, MAX(lpercentage) FROM language GROUP BY cid, lname;

INSERT INTO Query8(SELECT c1.cname AS c1name, c2.cname AS c2name, p1.lname FROM popular p1, popular p2, neighbour n, country c1, country c2 WHERE p1.cid = n.country and p2.cid = n.neighbor AND p1.lname = p2.lname AND p1.cid <> n.neighbor AND p1.cid = c1.cid AND p2.cid = c2.cid);

DROP VIEW popular;

-- Query 9 statements

CREATE VIEW deepestOcean AS SELECT DISTINCT cid, MAX(o.depth) AS depth FROM oceanAccess a, ocean o WHERE a.oid = o.oid GROUP BY cid;

INSERT INTO Query9(SELECT c.cname, (d.depth+c.height) AS totalspan FROM country c, deepestOcean d WHERE d.cid = c.cid ORDER BY totalspan DESC LIMIT 1);

DROP VIEW deepestOcean;

-- Query 10 statements

INSERT INTO Query10(SELECT c.cname, SUM(length) AS borderslength FROM country c, neighbour n WHERE c.cid = n.country GROUP BY c.cname ORDER BY borderslength DESC LIMIT 1);
