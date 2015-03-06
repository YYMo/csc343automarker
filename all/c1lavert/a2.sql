-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements



-- Query 2 statements
INSERT INTO Query2(SELECT c.cid, c.cname FROM a2.country c WHERE NOT EXISTS (SELECT * FROM a2.oceanAccess o WHERE o.cid = c.cid) ORDER BY c.cname);


-- Query 3 statements
INSERT INTO Query3(SELECT y.country, y.cname, y.neighbor, h.cname FROM (SELECT f.country, x.cname, f.neighbor FROM (SELECT ne.country, ne.neighbor FROM (SELECT n.country, count(n.neighbor) FROM neighbour n WHERE EXISTS (SELECT * FROM Query2 q WHERE n.country = q.cid) GROUP BY n.country HAVING (count(n.neighbor) = 1)) s JOIN neighbour ne ON ne.country=s.country) f JOIN country x on f.country=x.cid) y JOIN country h ON y.neighbor=h.cid ORDER BY y.cname);


-- Query 4 statements
INSERT INTO Query4(SELECT c.cname, t.oname FROM (select distinct f.country, f.oname FROM (select distinct n.country, o.oname from neighbour n join (SELECT oa.cid, oa.oid, oc.oname FROM oceanAccess oa JOIN ocean oc on oa.oid=oc.oid) o ON n.neighbor=o.cid UNION ALL select oa.cid, oc.oname from oceanAccess oa JOIN ocean oc on oa.oid=oc.oid) f) t join country c on t.country=c.cid ORDER BY c.cname, t.oname DESC);


-- Query 5 statements
INSERT INTO Query5(SELECT c.cid, c.cname, a.avghdi FROM (SELECT cid, avg(hdi_score) AS avghdi FROM hdi WHERE year=2009 OR year=2010 OR year=2011 OR year=2012 OR year=2013 GROUP BY cid) a JOIN country c ON a.cid = c.cid ORDER BY a.avghdi DESC LIMIT 10);


-- Query 6 statements



-- Query 7 statements
INSERT INTO Query7(SELECT f.rid, s.rname, f.followers FROM (SELECT r.rid, sum(c.population*r.rpercentage) followers FROM religion r JOIN country c ON r.cid=c.cid GROUP BY r.rid) f JOIN (SELECT DISTINCT rid, rname FROM religion) s ON f.rid=s.rid ORDER BY f.followers DESC);


-- Query 8 statements
--INSERT INTO Query8(SELECT c.cid, max(percentage) FROM country c JOIN language l ON c.cid=l.cid)


-- Query 9 statements
INSERT INTO Query9(SELECT co.cname, d.totalspan FROM (SELECT oc.cid, max(oc.depth + c.height) as totalspan FROM (SELECT oa.cid, oa.oid, o.depth FROM ocean o JOIN oceanAccess oa on o.oid=oa.oid) oc JOIN country c on oc.cid=c.cid GROUP BY oc.cid) d JOIN country co on d.cid=co.cid ORDER BY d.totalspan DESC LIMIT 1);


-- Query 10 statements
INSERT INTO Query10(SELECT c.cname, n.borderslength FROM (SELECT country, sum(length) as borderslength FROM neighbour group by country) n JOIN country c on n.country=c.cid ORDER BY n.borderslength DESC LIMIT 1);

