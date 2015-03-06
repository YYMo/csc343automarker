-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW MaxHeight AS
(SELECT CO.cid AS c1id, CO.cname AS c1name, height
FROM country AS CO);

CREATE VIEW Combined AS
(SELECT MH.c1id AS c1id, MH.c1name AS c1name, NB.neighbor AS c2id, 
CO.cname AS c2name, MH.height
FROM MaxHeight MH JOIN neighbour NB ON MH.c1id=NB.country 
JOIN country CO ON NB.neighbor=CO.cid);

CREATE VIEW DistMaxHeight AS
(SELECT CB.c1id, max(CB.c1name) AS c1name, 
max(CB.c2id) AS c2id, max(CB.c2name) AS c2name, max(CB.height)
FROM Combined CB LEFT JOIN Combined CB2 ON CB.c1id = CB2.c1id AND
CB.height < CB2.height
WHERE CB2.height IS NULL
GROUP BY CB.c1id);

INSERT INTO Query1
(SELECT c1id, c1name, c2id, c2name
FROM DistMaxHeight
ORDER BY c1name ASC);

DROP VIEW DistMaxHeight;
DROP VIEW Combined;
DROP VIEW MaxHeight;


-- Query 2 statements
INSERT INTO Query2
(SELECT CO.cid AS cid, CO.cname AS cname
FROM (country CO LEFT JOIN oceanAccess OA ON CO.cid=OA.cid)
WHERE OA.cid IS NULL
ORDER BY CO.cname ASC);

-- Query 3 statements
CREATE VIEW Landlocked AS 
(SELECT CO.cid AS cid, CO.cname AS cname
FROM (country CO LEFT JOIN oceanAccess OA ON CO.cid=OA.cid)
WHERE OA.cid IS NULL);

CREATE VIEW OneCountry AS 
(SELECT LL.cid AS c1id, LL.cname AS c1name,
count(NB.neighbor) AS one
FROM (Landlocked LL JOIN neighbour NB ON LL.cid = NB.country)
GROUP BY LL.cid, LL.cname
HAVING count(NB.neighbor) = 1);

INSERT INTO Query3 
(SELECT OC.c1id, OC.c1name, NB.neighbor AS c2id, CO.cname AS c2name
FROM OneCountry OC JOIN neighbour NB ON OC.c1id=NB.country JOIN 
country CO ON NB.neighbor=CO.cid
WHERE OC.one = 1
ORDER BY OC.c1name ASC);

DROP VIEW OneCountry;
DROP VIEW Landlocked;


-- Query 4 statements
CREATE VIEW CountryOA AS 
SELECT OA.cid AS cid, OC.oid AS oid
FROM (oceanAccess OA JOIN ocean OC ON OC.oid=OA.oid);

CREATE VIEW NeighbourOA AS 
SELECT NB.neighbor AS cid, OA.oid AS oid
FROM (oceanAccess OA JOIN neighbour NB ON NB.country=OA.cid);

CREATE VIEW BothOA AS 
SELECT *
FROM CountryOA 
UNION 
SELECT * 
FROM NeighbourOA;

INSERT INTO Query4
(SELECT  CO.cname AS cname, O.oname AS oname
FROM BothOA JOIN country CO ON CO.cid=BothOA.cid JOIN ocean O ON 
BothOA.oid=O.oid
GROUP BY CO.cname, O.oname
ORDER BY CO.cname ASC, O.oname DESC);

DROP VIEW BothOA;
DROP VIEW CountryOA;
DROP VIEW NeighbourOA;

 
-- Query 5 statements
CREATE VIEW Average AS 
(SELECT CO.cid, cname, avg(H.hdi_score) AS avghdi
FROM (hdi H JOIN country CO ON H.cid=CO.cid)
WHERE year<=2013 AND year >=2009
GROUP BY CO.cid, cname);

INSERT INTO Query5
(SELECT cid, cname, max(avghdi) AS avghdi
FROM Average
GROUP BY cid, cname
ORDER BY avghdi DESC
LIMIT 10);

DROP VIEW Average;

-- Query 6 statements
CREATE VIEW decreased AS
( SELECT a.cid
FROM hdi a, hdi b	
WHERE a.year<b.year and a.year>2008 and a.year<2013 and b.year<2014 and b.year>2008 and a.hdi_score > b.hdi_score and a.cid = b.cid
);

CREATE VIEW everycountry AS
( Select cid
FROM hdi
);

CREATE VIEW increased AS
( Select DISTINCT everycountry.cid as cid
FROM everycountry left join decreased on (everycountry.cid = decreased.cid)
where decreased.cid is NULL
);

CREATE VIEW Final AS
(SELECT increased.cid AS cid, CO.cname AS cname
FROM increased JOIN country CO ON increased.cid=CO.cid
GROUP BY increased.cid, CO.cname);

INSERT INTO Query6
(SELECT *
FROM Final
ORDER BY cname ASC);

DROP VIEW Final;
DROP VIEW increased;
DROP VIEW everycountry;
DROP VIEW decreased;


-- Query 7 statements
CREATE VIEW inter1 AS
(SELECT religion.rname, religion.rid as rid, religion.rpercentage/100*country.population as followers
FROM religion, country 
WHERE religion.cid = country.cid
);

CREATE VIEW inter2 AS
(Select rid, rname, sum(followers) as followers
FROM inter1
GROUP BY rid, rname
ORDER BY followers desc);

INSERT INTO Query7
(SELECT rid, rname, followers
FROM inter2);

DROP VIEW inter2;
DROP VIEW inter1;



-- Query 8 statements
CREATE VIEW CountLang AS
(SELECT LA.cid AS cid, count(LA.lname) AS lname
FROM language LA JOIN neighbour NB ON LA.cid=NB.country
GROUP BY LA.cid);

CREATE VIEW MostPopLang AS
(SELECT cid AS mcid, max(lname) AS mlname
FROM CountLang
GROUP BY cid);

CREATE VIEW WithNeighbour AS
(SELECT WN.cid, WN.lname, WN.neighbor
FROM (CountLang CL JOIN neighbour NB ON CL.cid=NB.country) AS WN
);

CREATE VIEW NBPopLang AS
(SELECT NBP.cid AS pcid, NBP.neighbor AS pncid
FROM (WithNeighbour WN JOIN MostPopLang MPL ON WN.cid=MPL.mcid) AS NBP
);

CREATE VIEW CountryName AS
(SELECT PL.pcid, PL.pncid, PL.cname AS c1name
FROM (NBPopLang NBP JOIN country CO ON NBP.pcid=CO.cid) AS PL);

CREATE VIEW NeighbourName AS
(SELECT PL2.pcid, Pl2.c1name, PL2.cname AS c2name
FROM (CountryName CN JOIN country CO ON CN.pncid=CO.cid) AS PL2);

INSERT INTO Query8
(SELECT Final.c1name, Final c2name, Final.mlname AS lname
FROM (NeighbourName NN JOIN MostPopLang MPL ON NN.pcid=MPL.mcid) 
AS Final
ORDER BY Final.mlname ASC, Final.c1name DESC);

DROP VIEW NeighbourName;
DROP VIEW CountryName;
DROP VIEW NBPopLang;
DROP VIEW WithNeighbour;
DROP VIEW MostPopLang;
DROP VIEW CountLang;

-- Query 9 statements
CREATE VIEW HighestElev AS
(SELECT cid, cname, max(height) AS maxheight
FROM country 
GROUP BY cid, cname);

CREATE VIEW OceanAcc AS
(SELECT CO.cid, CO.cname, OA.oid, oname, depth
FROM country CO JOIN oceanAccess OA ON CO.cid=OA.cid JOIN ocean OC ON
OA.oid=OC.oid);

CREATE VIEW LowestDepth AS 
(SELECT CO.cid, CO.cname, max(depth) AS lowest
FROM country CO JOIN OceanACC ON CO.cid=OceanACC.cid
GROUP BY CO.cid, CO.cname);

CREATE VIEW HandL AS 
(SELECT HE.cid, HE.cname, max(maxheight) AS highestH, max(lowest)
AS lowestD
FROM HighestElev HE LEFT JOIN LowestDepth LD ON HE.cid=LD.cid
GROUP BY HE.cid, HE.cname);

INSERT INTO Query9
(SELECT HL1.cname, (HL1.highestH + HL1.lowestD) AS totalspan 
FROM HandL AS HL1, Handl AS Hl2
WHERE (HL1.highestH + HL1.lowestD) > (HL2.highestH + HL2.lowestD)
LIMIT 1);

DROP VIEW HandL;
DROP VIEW LowestDepth;
DROP VIEW OceanAcc;
DROP VIEW HighestElev;


-- Query 10 statements
CREATE VIEW LongestPerCountry AS
(SELECT country, sum(length) AS borderslength
FROM neighbour 
GROUP BY country);
 
INSERT INTO Query10
(SELECT cname, max(borderslength) AS borderslength
FROM LongestPerCountry LPC JOIN country CO ON LPC.country=CO.cid
GROUP BY cname
ORDER BY borderslength DESC
LIMIT 1);

DROP VIEW LongestPerCountry;
