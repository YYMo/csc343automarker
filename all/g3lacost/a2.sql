-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Set the DB search path
SET search_path TO a2;

-- Query 1 statements

-- drop views if they exist
DROP VIEW IF EXISTS topNeighbourHeight2;
DROP VIEW IF EXISTS topNeighbourHeight;
DROP VIEW IF EXISTS neighbourInfo;
DROP VIEW IF EXISTS countryInfo;

-- create views
CREATE VIEW countryInfo AS (SELECT CO.cid AS c1id, CO.cname AS c1name, CO.height AS c1height, NE.neighbor AS c2id FROM country CO JOIN neighbour NE ON NE.country = CO.cid);
CREATE VIEW neighbourInfo AS (SELECT CI.c1id AS c1id, CI.c1name AS c1name, CI.c1height AS c1height, CI.c2id AS c2id, CO.cname AS c2name, CO.height AS c2height FROM countryInfo CI JOIN country CO ON CI.c2id = CO.cid);
CREATE VIEW topNeighbourHeight AS (select c1id as c1id, MAX(c2height) AS c2height from neighbourinfo group by c1id);
CREATE VIEW topNeighbourHeight2 AS (SELECT NI.c1id AS c1id, NI.c1name AS c1name, NI.c2id AS c2id, NI.c2name AS c2name FROM neighbourInfo NI JOIN topNeighbourHeight TNH ON NI.c2height = TNH.c2height WHERE NI.c1id = TNH.c1id ORDER BY NI.c1name ASC);

-- insert
INSERT INTO Query1 (SELECT * FROM topNeighbourHeight2);

DROP VIEW IF EXISTS topNeighbourHeight2;
DROP VIEW IF EXISTS topNeighbourHeight;
DROP VIEW IF EXISTS neighbourInfo;
DROP VIEW IF EXISTS countryInfo;

-- Query 2 statements

-- Drop views if they already exist
DROP VIEW IF EXISTS landlocked CASCADE;

-- Get all the landlocked countries
CREATE VIEW landlocked AS (SELECT DISTINCT CO.cid AS cid, CO.cname AS cname FROM country CO WHERE CO.cid NOT IN (SELECT DISTINCT CO.cid AS cid FROM country CO JOIN oceanaccess OA ON CO.cid = OA.cid) ORDER BY CO.cname ASC);

-- Store the values into the table
INSERT INTO Query2(SELECT * FROM landlocked);

-- Drop temp views
DROP VIEW landlocked;

-- Query 3 statements

-- Drop views if they already exist
DROP VIEW IF EXISTS finalView;
DROP VIEW IF EXISTS semiFinalView;
DROP VIEW IF EXISTS landlockedOneNeighbourNames;
DROP VIEW IF EXISTS landlockedOneNeighbour;
DROP VIEW IF EXISTS oneNeighbour;
DROP VIEW IF EXISTS landlocked;

-- create views
CREATE VIEW landlocked AS (SELECT DISTINCT CO.cid AS cid, CO.cname AS cname FROM country CO WHERE CO.cid NOT IN (SELECT DISTINCT CO.cid AS cid FROM country CO JOIN oceanaccess OA ON CO.cid = OA.cid) ORDER BY CO.cname ASC);
CREATE VIEW oneNeighbour AS (SELECT NE.country AS cid, COUNT(NE.country) AS numNeighbours FROM neighbour NE GROUP BY NE.country HAVING COUNT(NE.country) = 1);
CREATE VIEW landlockedOneNeighbour AS (SELECT LD.cid AS cid FROM landlocked LD JOIN oneNeighbour NE ON LD.cid = NE.cid);
CREATE VIEW landlockedOneNeighbourNames AS (SELECT LON.cid AS cid, CO.cname AS cname FROM landlockedOneNeighbour LON JOIN country CO ON LON.cid = CO.cid);
CREATE VIEW semiFinalView AS (SELECT LON.cid AS c1id, LON.cname AS c1name, NE.neighbor AS c2id FROM landlockedOneNeighbourNames LON JOIN neighbour NE ON LON.cid = NE.country);
CREATE VIEW finalView AS (SELECT SFV.c1id AS c1id, SFV.c1name AS c1name, SFV.c2id AS c2id, CO.cname AS c2name FROM semiFinalView SFV JOIN country CO ON SFV.c2id = CO.cid);

-- store the values into the table
INSERT INTO Query3 (SELECT * from finalView);

-- drop temp views
DROP VIEW finalView;
DROP VIEW semiFinalView;
DROP VIEW landlockedOneNeighbourNames;
DROP VIEW landlockedOneNeighbour;
DROP VIEW oneNeighbour;
DROP VIEW landlocked;

-- Query 4 statements



-- Query 5 statements

-- Drop views if they already exist
DROP VIEW IF EXISTS topTenAvg CASCADE;
DROP VIEW IF EXISTS topTenNamed CASCADE;

-- Get the top ten countries with the highest HDI average scores from 2009 to 2013
CREATE VIEW topTenAvg (cid, avg) AS(SELECT HD.cid AS cid, AVG(HD.hdi_score) AS avg FROM hdi HD WHERE year >= 2009 AND year <= 2013 GROUP BY cid ORDER BY avg DESC LIMIT 10);

-- Get the top ten countries with each country's name
CREATE VIEW topTenNamed (cid, cname, avghdi) AS(SELECT TTA.cid AS cid, CO.cname AS cname, TTA.avg AS avghdi FROM country CO JOIN topTenAvg TTA ON CO.cid = TTA.cid);

-- Store the values into the table
INSERT INTO Query5(SELECT * FROM topTenNamed);

-- Drop temp views
DROP VIEW topTenNamed;
DROP VIEW topTenAvg;

-- Query 6 statements

-- drop views if they already exist
DROP VIEW IF EXISTS increasinghdi;
DROP VIEW IF EXISTS Incr2013 CASCADE;
DROP VIEW IF EXISTS Incr2012 CASCADE;
DROP VIEW IF EXISTS Incr2011 CASCADE;
DROP VIEW IF EXISTS Incr2010 CASCADE;

-- create views
CREATE VIEW Incr2010 (cid, year, hdi) AS (SELECT HD.cid AS cid, HD.year AS year, HD.hdi_score AS hdi FROM hdi HD INNER JOIN hdi HD1 ON HD.cid = HD1.cid WHERE HD.year = '2010' AND HD1.year = '2009' AND HD.hdi_score > HD1.hdi_score);
CREATE VIEW Incr2011 (cid, year, hdi) AS (SELECT HD.cid AS cid, HD.year AS year, HD.hdi_score AS hdi FROM hdi HD INNER JOIN hdi HD1 ON HD.cid = HD1.cid WHERE HD.year = '2011' AND HD1.year = '2010' AND HD.hdi_score > HD1.hdi_score);
CREATE VIEW Incr2012 (cid, year, hdi) AS (SELECT HD.cid AS cid, HD.year AS year, HD.hdi_score AS hdi FROM hdi HD INNER JOIN hdi HD1 ON HD.cid = HD1.cid WHERE HD.year = '2012' AND HD1.year = '2011' AND HD.hdi_score > HD1.hdi_score);
CREATE VIEW Incr2013 (cid, year, hdi) AS (SELECT HD.cid AS cid, HD.year AS year, HD.hdi_score AS hdi FROM hdi HD INNER JOIN hdi HD1 ON HD.cid = HD1.cid WHERE HD.year = '2013' AND HD1.year = '2012' AND HD.hdi_score > HD1.hdi_score);
CREATE VIEW increasinghdi (cid) AS (SELECT cid FROM Incr2013 WHERE cid IN (SELECT cid FROM Incr2012 WHERE cid IN (SELECT cid FROM Incr2011 WHERE cid IN (SELECT cid FROM Incr2010))));

-- store the values into the table
INSERT INTO Query6 (SELECT IH.cid AS cid, CO.cname AS cname FROM increasinghdi IH JOIN country CO ON IH.cid = CO.cid ORDER BY cname ASC);

-- drop temp views
DROP VIEW increasinghdi;
DROP VIEW Incr2013 CASCADE;
DROP VIEW Incr2012 CASCADE;
DROP VIEW Incr2011 CASCADE;
DROP VIEW Incr2010 CASCADE;

-- Query 7 statements

-- drop views if they already exist
DROP VIEW IF EXISTS finalView;
DROP VIEW IF EXISTS religionInfo;
DROP VIEW IF EXISTS totalFollowers;
DROP VIEW IF EXISTS countryAndReligion;

-- create views
CREATE VIEW countryAndReligion AS (SELECT RE.rid AS rid, (RE.rpercentage * CO.population) AS numFollowers FROM religion RE JOIN country CO ON RE.cid = CO.cid);
CREATE VIEW totalFollowers AS (SELECT CAR.rid AS rid, SUM(CAR.numfollowers) AS followers FROM countryAndReligion CAR GROUP BY CAR.rid);
CREATE VIEW religionInfo AS (SELECT DISTINCT RE.rid as rid, RE.rname AS rname FROM religion RE);
CREATE VIEW finalView AS (SELECT TF.rid AS rid, RI.rname AS rname, TF.followers AS followers FROM totalFollowers TF JOIN religionInfo RI ON TF.rid = RI.rid ORDER BY TF.followers DESC);

-- store the values into the table
INSERT INTO Query7 (SELECT * FROM finalView);

-- drop temp views
DROP VIEW finalView;
DROP VIEW religionInfo;
DROP VIEW totalFollowers;
DROP VIEW countryAndReligion;

-- Query 8 statements



-- Query 9 statements

-- drop views if they already exist
DROP VIEW IF EXISTS finalView;
DROP VIEW IF EXISTS countryOceanInfo;
DROP VIEW IF EXISTS countryOceanDepth;

-- create views
CREATE VIEW countryOceanDepth AS (SELECT OA.cid AS cid, OA.oid AS oid, OC.oname AS oname, OC.depth AS depth FROM oceanaccess OA JOIN ocean OC ON OA.oid = OC.oid);
CREATE VIEW countryOceanInfo AS (SELECT CO.cid AS cid, CO.cname AS cname, CO.height AS height, COD.oid AS oid, COD.oname AS oname, COD.depth AS depth FROM countryOceanDepth COD JOIN country CO ON COD.cid = CO.cid);
CREATE VIEW finalView AS (SELECT COI.cname AS cname, MAX (COI.height - COI.depth) AS totalSpan FROM countryOceanInfo COI GROUP BY COI.cname ORDER BY MAX (COI.height - COI.depth) DESC LIMIT 1);

-- store the values into the table
INSERT INTO Query9 (SELECT * FROM finalView);

-- drop temp views
DROP VIEW finalView;
DROP VIEW countryOceanInfo;
DROP VIEW countryOceanDepth;

-- Query 10 statements

-- drop views if they already exist
DROP VIEW IF EXISTS countryMaxBorder;
DROP VIEW IF EXISTS totalBorderLength;

-- create views
CREATE VIEW totalBorderLength AS (SELECT NE.country AS cid, SUM(NE.length) AS totalBorderLength FROM neighbour NE GROUP BY NE.country);
CREATE VIEW countryMaxBorder AS (SELECT TB.cid AS cid, TB.totalborderlength AS maxBorderLength FROM totalborderlength TB ORDER BY maxBorderLength DESC LIMIT 1);

-- store the values into the table
INSERT INTO Query10 (SELECT CO.cname AS cname, CMB.maxborderlength AS borderslength FROM countrymaxborder CMB JOIN country CO ON CMB.cid = CO.cid);

-- drop temp views
DROP VIEW countryMaxBorder;
DROP VIEW totalBorderLength;