-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW neighbourWithElevation AS
	SELECT n.country AS c1id, c2.cname AS c1name, n.neighbor AS c2id, c1.cname AS c2name, c1.height AS neighborElevation, rank() OVER (PARTITION BY n.country ORDER BY c1.height DESC) AS ranked
	FROM neighbour n JOIN country c1 ON n.neighbor = c1.cid
					 JOIN country c2 ON n.country = c2.cid;

INSERT INTO Query1
SELECT DISTINCT ON (c1name) c1id, c1name, c2id, c2name 
FROM neighbourWithElevation
WHERE ranked = 1
ORDER BY c1name ASC;

DROP VIEW neighbourWithElevation;

-- Query 2 statements
INSERT INTO Query2
SELECT c.cid, c.cname
FROM country c LEFT OUTER JOIN oceanAccess oa ON c.cid = oa.cid
WHERE oa.CID IS NULL
ORDER BY c.cname ASC;


-- Query 3 statements
CREATE VIEW landlockedCountries AS
	SELECT c.cid, c.cname
	FROM country c LEFT OUTER JOIN oceanAccess oa ON c.cid = oa.cid
	WHERE oa.CID IS NULL;

CREATE VIEW landlockedCountriesLonely AS
	SELECT llc.cid, llc.cname
	FROM landlockedCountries llc JOIN neighbour n ON llc.cid = n.country
	GROUP BY llc.cid, llc.cname
	HAVING COUNT(n.neighbor) = 1;

INSERT INTO Query3
SELECT llcl.cid AS c1id, llcl.cname AS c1name, c.cid AS c2id, c.cname AS c2name
FROM landlockedCountriesLonely llcl JOIN neighbour n ON llcl.cid = n.country
									JOIN country c ON n.neighbor = c.cid
ORDER BY llcl.cname ASC;

DROP VIEW landlockedCountriesLonely;
DROP VIEW landlockedCountries;

-- Query 4 statements
CREATE VIEW neighbourOceanAccess AS
	SELECT n.country, n.neighbor, oa.oid
	FROM neighbour n JOIN oceanAccess oa ON n.neighbor = oa.cid;

INSERT INTO Query4
SELECT cname, oname
FROM (SELECT c.cname AS cname, o.oname AS oname
FROM neighbourOceanAccess noa JOIN ocean o ON noa.oid = o.oid
							  JOIN country c ON noa.country = c.cid
UNION
SELECT c.cname AS cname, o.oname AS oname
FROM oceanAccess oa JOIN ocean o ON oa.oid = o.oid
					JOIN country c ON oa.cid = c.cid) a
ORDER BY cname ASC, oname DESC;

DROP VIEW neighbourOceanAccess;

-- Query 5 statements
CREATE VIEW avghdi AS
	SELECT cid, AVG(hdi_score) AS avghdi
	FROM hdi
	WHERE year >= 2009 AND year <=2013
	GROUP BY cid;

INSERT INTO Query5
SELECT ah.cid AS cid, c.cname AS cname, ah.avghdi AS avghdi
FROM avghdi ah JOIN country c ON ah.cid = c.cid
ORDER BY avghdi DESC
LIMIT 10;

DROP VIEW avghdi;

-- Query 6 statements
CREATE VIEW hdi5 AS
	SELECT cid, year, hdi_score
	FROM hdi
	WHERE year >= 2009 AND year <= 2013;

CREATE VIEW hdiDifference AS
	SELECT h51.cid, (h52.year - h51.year) AS yeardifference, (h52.hdi_score - h51.hdi_score) AS hdidifference
	FROM hdi5 h51 JOIN hdi5 h52 ON h51.cid = h52.cid
	WHERE h52.year > h51.year;

CREATE VIEW hdiFinal AS
	SELECT cid, yeardifference, hdidifference
	FROM hdiDifference
	WHERE cid NOT IN (SELECT cid FROM hdiDifference WHERE hdidifference <= 0);

INSERT INTO Query6
SELECT DISTINCT c.cid, c.cname
FROM hdiFinal hd JOIN country c ON hd.cid = c.cid
ORDER BY c.cname ASC;

DROP VIEW hdiFinal;
DROP VIEW hdiDifference;
DROP VIEW hdi5;

-- Query 7 statements
CREATE VIEW religionPopulationPerCountry AS
	SELECT r.cid AS cid, r.rid AS rid, r.rname AS rname, (r.rpercentage * c.population) AS followers
	FROM religion r JOIN country c ON r.cid = c.cid;

INSERT INTO Query7
SELECT rid, rname, SUM(followers)
FROM religionPopulationPerCountry
GROUP BY rid, rname;

DROP VIEW religionPopulationPerCountry;

-- Query 8 statements
CREATE VIEW countryMostPopularLanguage AS
	SELECT DISTINCT ON (cid) cid, lid, lname
	FROM language
	ORDER BY cid, lpercentage DESC;

CREATE VIEW neighbourMostPopularLanguage AS
	SELECT cmpl1.cid AS c1id, cmpl2.cid AS c2id, cmpl1.lname AS lname
	FROM neighbour n JOIN countryMostPopularLanguage cmpl1 ON n.country = cmpl1.cid
					 JOIN countryMostPopularLanguage cmpl2 ON n.neighbor = cmpl2.cid
	WHERE cmpl1.lid = cmpl2.lid;

INSERT INTO Query8
SELECT c1.cname AS c1name, c2.cname AS c2name, lname
FROM neighbourMostPopularLanguage nmpl JOIN country c1 ON nmpl.c1id = c1.cid
									   JOIN country c2 ON nmpl.c2id = c2.cid;

DROP VIEW neighbourMostPopularLanguage;
DROP VIEW countryMostPopularLanguage;

-- Query 9 statements
CREATE VIEW oceanAccessDepth AS
	SELECT DISTINCT ON (oa.cid) oa.cid AS cid, o.depth as depth
	FROM oceanAccess oa JOIN ocean o ON oa.oid = o.oid
	ORDER BY oa.cid, o.depth DESC;

INSERT INTO Query9
SELECT c.cname AS cname, (c.height + COALESCE(oad.depth,0)) AS totalspan
FROM oceanAccessDepth oad RIGHT OUTER JOIN country c ON oad.cid = c.cid
ORDER BY (c.height + COALESCE(oad.depth,0)) DESC
LIMIT 1;

DROP VIEW oceanAccessDepth;

-- Query 10 statements
INSERT INTO Query10
SELECT c.cname AS cname, SUM(n.length) AS borderslength
FROM neighbour n JOIN country c ON n.country = c.cid
GROUP BY c.cid
ORDER BY SUM(n.length) DESC
LIMIT 1;



