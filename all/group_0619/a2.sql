-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to DROP VIEW these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW Neighbours AS
	SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name, c2.height AS neighbour_height
	FROM country c1, neighbour n, country c2
	WHERE n.country=c1.cid AND n.neighbor=c2.cid
	ORDER BY c1name ASC;

INSERT INTO Query1 (
	SELECT n.c1id AS c1id, n.c1name AS c1name, n.c2id AS c2id, n.c2name AS c2name
	FROM Neighbours n
	WHERE neighbour_height = ( SELECT MAX(neighbour_height) FROM Neighbours GROUP BY c1id HAVING c1id = n.c1id )
);

DROP VIEW Neighbours;

-- Query 2 statements
INSERT INTO Query2 (
	SELECT c.cid AS cid, c.cname AS cname
	FROM country c
	WHERE c.cid NOT IN (SELECT cid FROM oceanAccess)
	ORDER BY cname ASC
);

-- Query 3 statements
CREATE VIEW LANDLOCKED AS
	SELECT c.cid AS cid, c.cname AS cname
	FROM country c
	WHERE c.cid NOT IN (SELECT cid FROM oceanAccess);

CREATE VIEW OneNeighbour AS
	SELECT L.cid AS c1id, L.cname AS c1name
	FROM country c1, LANDLOCKED L, neighbour n 
	WHERE L.cid = n.country AND c1.cid = n.neighbor 
	GROUP BY c1id, c1name
	HAVING COUNT(*) = 1;


INSERT INTO Query3 (
	SELECT L.cid AS c1id, L.cname AS c1name, c1.cid AS c2id, c1.cname AS c2name
	FROM country c1, LANDLOCKED L, neighbour n 
	WHERE L.cid = n.country AND c1.cid = n.neighbor AND L.cid in 
	(SELECT L.cid FROM OneNeighbour) 
	ORDER BY c1name ASC
);

DROP VIEW OneNeighbour;
DROP VIEW LANDLOCKED;

-- Query 4 statements
CREATE VIEW DIRECTOCEAN AS
	SELECT c.cname AS cname, o.oname AS oname
	FROM country c, oceanAccess oa, ocean o
	WHERE c.cid = oa.oid AND o.oid = oa.oid;

CREATE VIEW INDIRECTOCEAN AS
	SELECT c.cname AS cname, o.oname AS oname
	FROM country c, neighbour n, oceanAccess oa, ocean o
	WHERE oa.cid = n.country AND c.cid = n.neighbor AND o.oid = oa.oid;

CREATE VIEW COMBINEDOCEAN AS
	(SELECT cname, oname
	FROM DIRECTOCEAN)
	UNION
	(SELECT cname, oname
	FROM INDIRECTOCEAN);
	
INSERT INTO Query4 (
	SELECT *
	FROM COMBINEDOCEAN
	ORDER BY cname ASC, oname DESC
);

DROP VIEW COMBINEDOCEAN;
DROP VIEW INDIRECTOCEAN;
DROP VIEW DIRECTOCEAN;


-- Query 5 statements
INSERT INTO Query5 (
	SELECT c.cid AS cid, c.cname AS cname, AVG(h.hdi_score) AS avghdi 
	FROM country c, hdi h 
	WHERE c.cid = h.cid AND h.year > '2008' AND h.year < '2014' 
	GROUP BY c.cid, c.cname 
	ORDER BY avghdi DESC LIMIT 10 
);


-- Query 6 statements
CREATE VIEW HDI09 AS
	SELECT c.cname AS cname, c.cid AS cid, h.hdi_score AS hscore09
	FROM country c, hdi h
	WHERE c.cid = h.cid AND h.year = '2009';

CREATE VIEW HDI10 AS
	SELECT c.cname AS cname, c.cid AS cid, h.hdi_score AS hscore10
	FROM country c, hdi h
	WHERE c.cid = h.cid AND h.year = '2010';

CREATE VIEW HDI11 AS
	SELECT c.cname AS cname, c.cid AS cid, h.hdi_score AS hscore11
	FROM country c, hdi h
	WHERE c.cid = h.cid AND h.year = '2011';	

CREATE VIEW HDI12 AS
	SELECT c.cname AS cname, c.cid AS cid, h.hdi_score AS hscore12
	FROM country c, hdi h
	WHERE c.cid = h.cid AND h.year = '2012';

CREATE VIEW HDI13 AS
	SELECT c.cname AS cname, c.cid AS cid, h.hdi_score AS hscore13
	FROM country c, hdi h
	WHERE c.cid = h.cid AND h.year = '2013';
	
INSERT INTO Query6 (
	SELECT h13.cid AS cid, h13.cname AS cname
	FROM HDI09 h09, HDI10 h10, HDI11 h11, HDI12 h12, HDI13 h13
	WHERE h09.cid = h10.cid AND h09.cid = h11.cid AND h09.cid = h12.cid AND h09.cid = h13.cid AND h09.cname = h10.cname AND h09.cname = h11.cname AND h09.cname = h12.cname AND h09.cname = h13.cname AND h09.hscore09 < h10.hscore10 AND h10.hscore10 < h11.hscore11 AND h11.hscore11 < h12.hscore12 AND h12.hscore12 < h13.hscore13
	ORDER BY h13.cname ASC 
);

DROP VIEW HDI13;
DROP VIEW HDI12;
DROP VIEW HDI11;
DROP VIEW HDI10;
DROP VIEW HDI09;


-- Query 7 statements
INSERT INTO Query7 (
	SELECT r.rid AS rid, r.rname AS rname, sum(c.population * r.rpercentage) AS followers
	FROM country c, religion r
	WHERE c.cid = r.cid
	GROUP BY r.rid, r.rname
	ORDER BY followers DESC
);


-- Query 8 statements
CREATE VIEW CountryLang AS 
	SELECT c.cid AS cid, c.cname AS cname, l.lname AS lname, l.lpercentage AS lpercentage 
	FROM country c, language l 
	WHERE c.cid = l.cid;

CREATE VIEW PopCountryLang AS
	SELECT cname, lname 
	FROM CountryLang t 
	WHERE lpercentage = ( SELECT MAX(lpercentage) FROM CountryLang  WHERE cid = t.cid );

CREATE VIEW NeighbourLang AS
	SELECT c1.cname AS c1name, c2.cname AS c2name, l.lname AS lname, l.lpercentage AS lpercentage 
	FROM country c1, country c2, neighbour n, language l 
	WHERE c1.cid = n.country AND n.neighbor = c2.cid AND n.neighbor = l.cid;
	
CREATE VIEW PopNeighbourLang AS 
	SELECT c1name, c2name, lname 
	FROM NeighbourLang t 
	WHERE lpercentage = ( SELECT MAX(lpercentage) FROM NeighbourLang WHERE c1name = t.c1name AND c2name = t.c2name );	
	
INSERT INTO Query8 (
	SELECT c.cname AS c1name, n.c2name AS c2name, c.lname AS lname 
	FROM PopCountryLang c, PopNeighbourLang n 
	WHERE c.cname = n.c1name AND c.lname = n.lname 
	ORDER BY c.lname ASC, c.cname DESC
);

DROP VIEW PopNeighbourLang;
DROP VIEW NeighbourLang;
DROP VIEW PopCountryLang;
DROP VIEW CountryLang;


-- Query 9 statements
CREATE VIEW NoOceanAccess AS 
	SELECT cname, height AS totalspan 
	FROM country 
	WHERE cid NOT IN ( SELECT cid FROM oceanAccess );
 
CREATE VIEW YesOceanAccess AS 
	SELECT c.cname AS cname, (c.height + o.depth) AS totalspan 
	FROM country c, oceanAccess oc, ocean o 
	WHERE c.cid = oc.cid AND oc.oid = o.oid;
 
Create View AllAccess AS 
	(SELECT * 
	FROM YesOceanAccess) 
	UNION ALL 
	(SELECT * 
	FROM NoOceanAccess);
 
INSERT INTO Query9 (
	SELECT cname, totalspan 
	FROM AllAccess 
	Where totalspan = ( Select MAX(totalspan) FROM AllAccess ) 
);

DROP VIEW AllAccess;
DROP VIEW YesOceanAccess;
DROP VIEW NoOceanAccess;


-- Query 10 statements
CREATE VIEW borders AS 
	SELECT c.cname AS cname, SUM(n.length) AS sumlength
	FROM country c, neighbour n 
	WHERE c.cid = n.country 
	GROUP BY c.cname;


INSERT INTO Query10 (
	SELECT cname, MAX(sumlength) AS borderslength 
	FROM borders 
	GROUP BY cname
);

DROP VIEW borders;
