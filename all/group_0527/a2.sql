-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW FirstJoin AS
SELECT C.cid AS c1id, C.cname AS c1name, N.neighbor AS neighbor
FROM  country C INNER JOIN  neighbour N ON C.cid = N.country;

CREATE VIEW SecondJoin AS
SELECT F.c1id AS c1id, F.c1name AS c1name, C.cid AS c2id, C.cname AS c2name, C.height AS nheight
FROM  FirstJoin F INNER JOIN  country  C ON F.neighbor = C.cid;

CREATE VIEW AlmostDone AS
SELECT c1id, c1name, MAX(nheight) AS maxheight
FROM SecondJoin 
GROUP BY c1name, c1id
ORDER BY c1name ASC;

INSERT INTO QUERY1(
SELECT AD.c1id AS c1id, AD.c1name as c1name, SJ.c2id AS c2id, SJ.c2name as c2name
FROM AlmostDone AD JOIN SecondJoin SJ ON AD.c1id = SJ.c1id AND SJ.nheight = AD.maxheight
ORDER BY c1name ASC
);
DROP VIEW  AlmostDone;
DROP VIEW SecondJoin;
DROP VIEW FirstJoin;


-- Query 2 statements +
CREATE VIEW landlocked AS
SELECT 	cid, cname
FROM 	country c
WHERE 	NOT EXISTS(
	SELECT null
	FROM oceanAccess o
	WHERE o.cid = c.cid 
	)
ORDER BY cname ASC;

INSERT INTO QUERY2(
SELECT *
FROM landlocked
);

DROP VIEW landlocked;
-- Query 3 statements +

CREATE VIEW landlocked AS
SELECT 	cid, cname
FROM 	country c
WHERE 	NOT EXISTS(
	SELECT null
	FROM oceanAccess o
	WHERE o.cid = c.cid 
	)
ORDER BY cname ASC;

CREATE VIEW oneNeighbour AS
SELECT	country as cid, count(neighbor)
FROM 	neighbour
GROUP BY country
HAVING 	count(neighbor) = 1;

CREATE VIEW oneSurrounded AS
SELECT	landlocked.cid, landlocked.cname
FROM 	landlocked JOIN oneNeighbour ON landlocked.cid = oneNeighbour.cid;

CREATE VIEW oneSurroundedNeighborId AS 
SELECT 	c1.cid as c1id, c1.cname as c1name, c2.neighbor as c2id
FROM	oneSurrounded c1 JOIN neighbour c2 ON c1.cid = c2.country;

INSERT INTO QUERY3(
SELECT 	c1id, c1name, c2id, c2.cname as c2name
FROM	oneSurroundedNeighborId o
JOIN country c2 ON o.c2id = c2.cid
ORDER BY c1name ASC
);

DROP VIEW oneSurroundedNeighborId;
DROP VIEW oneSurrounded;
DROP VIEW oneNeighbour;
DROP VIEW landlocked;
-- Query 4 statements +

CREATE VIEW indirectAccessID AS
SELECT	n.country AS cid, o.oid AS oid
FROM	oceanAccess o JOIN neighbour n ON n.neighbor = o.cid;

CREATE VIEW generalAccessID AS
SELECT oceanAccess.cid, oceanAccess.oid FROM oceanAccess 
UNION 
SELECT indirectAccessID.oid , indirectAccessID.cid FROM indirectAccessID;

INSERT INTO QUERY4(
SELECT c.cname AS cname, o.oname AS oname
FROM (generalAccessID g JOIN country c ON g.cid = c.cid) 
JOIN ocean o ON g.oid = o.oid
ORDER BY cname ASC, oname DESC
);

DROP VIEW generalAccessID;
DROP VIEW indirectAccessID;
-- Query 5 statements +

INSERT INTO QUERY5(
SELECT 	hdi.cid AS cid, country.cname AS cname, AVG(hdi.hdi_score) as avghdi
FROM 	hdi JOIN country ON country.cid=hdi.cid
WHERE 	hdi.year <=2013 AND hdi.year >=2009
GROUP BY hdi.cid, country.cname
ORDER BY avghdi DESC LIMIT 11
);

-- Query 6 statements -
CREATE VIEW HDI2009 AS
SELECT cid, hdi_score as hdi2009
FROM hdi
WHERE year = 2009;

CREATE VIEW HDI2010 AS
SELECT cid, hdi_score as hdi2010
FROM hdi
WHERE year = 2010;

CREATE VIEW HDI2011 AS
SELECT cid, hdi_score as hdi2011
FROM hdi
WHERE year = 2011;

CREATE VIEW HDI2012 AS
SELECT cid, hdi_score as hdi2012
FROM hdi
WHERE year = 2012;

CREATE VIEW HDI2013 AS
SELECT cid, hdi_score as hdi2013
FROM hdi
WHERE year = 2013;

CREATE VIEW JOIN1 AS
SELECT HDI2009.cid AS cid, hdi2009, hdi2010
FROM HDI2009 JOIN HDI2010 ON HDI2009.cid=HDI2010.cid;

CREATE VIEW JOIN2 AS
SELECT HDI2011.cid AS cid, hdi2011, hdi2012
FROM HDI2011 JOIN HDI2012 ON HDI2011.cid=HDI2012.cid;

CREATE VIEW JOIN3 AS
SELECT JOIN1.cid AS cid, hdi2009, hdi2010, hdi2011, hdi2012
FROM JOIN1 JOIN JOIN2 ON JOIN1.cid = JOIN2.cid;

CREATE VIEW JOIN4 AS
SELECT JOIN3.cid AS cid, hdi2009,hdi2010,hdi2011,hdi2012,hdi2013
FROM JOIN3 JOIN HDI2013 on JOIN3.cid = HDI2013.cid;

CREATE VIEW JOIN5 AS
SELECT cid 
FROM JOIN4
WHERE hdi2009 < hdi2010 AND hdi2010 < hdi2011 AND hdi2011 < hdi2012 AND hdi2012 < hdi2013;

INSERT INTO QUERY6(
SELECT JOIN5.cid AS cid, country.cname AS cname
FROM JOIN5 JOIN country on JOIN5.cid = country.cid
ORDER BY cname ASC
);

DROP VIEW JOIN5;
DROP VIEW JOIN4;
DROP VIEW JOIN3;
DROP VIEW JOIN2;
DROP VIEW JOIN1;
DROP VIEW HDI2013;
DROP VIEW HDI2012;
DROP VIEW HDI2011;
DROP VIEW HDI2010;
DROP VIEW HDI2009;
-- Query 7 statements +
INSERT INTO QUERY7(
SELECT foo.rid AS rid, foo.rname AS rname, sum(foo.notsummed) AS followers
FROM(
SELECT (R.rpercentage*C.population) as notsummed, R.rname as rname, R.rid as rid
FROM  religion R INNER JOIN  country C on R.cid = C.cid
) AS foo
GROUP BY foo.rname, foo.rid
ORDER BY followers DESC
);


-- Query 8 statements +

CREATE VIEW languagepop AS
SELECT 	cid, lid, lname, max(lpercentage) as poplang
FROM 	language
GROUP BY language.cid, language.lid;

CREATE VIEW langsameID AS
SELECT 	l1.cid as c1id, l2.cid as c2id, l1.lname as lname
FROM 	languagepop l1 JOIN languagepop l2 ON l1.poplang = l2.poplang;

CREATE VIEW langneighID AS
SELECT	l.c1id, l.c2id, l.lname
FROM 	langsameID l JOIN neighbour n ON (l.c1id = n.country AND l.c2id = n.neighbor);

INSERT INTO QUERY8(
SELECT	c1.cname as c1name, c2.cname as c2name, l.lname as lname
FROM 	(country c1 JOIN langneighID l ON c1.cid = l.c1id) JOIN country c2 ON c2.cid = l.c2id
ORDER BY lname ASC, c1name DESC
);

DROP VIEW langneighID;
DROP VIEW langsameID;
DROP VIEW languagepop;
-- Query 9 statements + -
CREATE VIEW largestdifferenceID AS
SELECT	c.cid AS cid, c.cname as cname, MAX(o.depth - c.height) AS totalspan
FROM	(oceanAccess oa JOIN ocean oc ON oc.oid = oa.oid) o JOIN country c ON o.cid = c.cid
GROUP BY c.cid;

CREATE VIEW largestdifference AS
SELECT  MAX(totalspan) AS maxspan
FROM largestdifferenceID;

INSERT INTO QUERY9(
SELECT LDID.cname as cname, LD.maxspan AS totalspan
FROM largestdifferenceID LDID JOIN largestdifference LD ON LDID.totalspan = LD.maxspan
);

DROP VIEW largestdifference;
DROP VIEW largestdifferenceID;
-- Query 10 statements +
CREATE VIEW borderlength AS
SELECT	c.cid as cid, c.cname as cname, sum(length) as solong
FROM	neighbour n JOIN country c on n.country = c.cid
GROUP BY cid;

CREATE VIEW midquery AS
	SELECT cid, cname, max(solong) as borderslength
	FROM borderlength
	GROUP BY borderlength.cid, borderlength.cname;

CREATE VIEW maxborderlength AS
SELECT MAX(borderslength) as longestborder
FROM midquery;

INSERT INTO QUERY10(
SELECT MQ.cname AS cname, MBL.longestborder AS longestborder
FROM maxborderlength MBL join midquery MQ ON MBL.longestborder = MQ.borderslength
);

DROP VIEW maxborderlength;
DROP VIEW midquery;
DROP VIEW borderlength;
