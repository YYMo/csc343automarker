-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Drop if Exists Statements

Drop View IF EXISTS maxHeightNeighbour;
Drop View IF EXISTS neighbourHeight;
Drop View IF EXISTS cidNeighbourPair;
Drop View IF EXISTS cid1name;

Drop View IF EXISTS cidCountry;
Drop View IF EXISTS oceanCountry;
Drop View IF EXISTS landlockedcid;

Drop View IF EXISTS cidCountry;
Drop View IF EXISTS oceanCountry;
Drop View IF EXISTS landlockedcid;
Drop View IF EXISTS landlockedNeighbours;
Drop View IF EXISTS landlockedNeighboursv2;
Drop View IF EXISTS c1name;

Drop View IF EXISTS directAccess;
Drop View IF EXISTS indirectAccess;
Drop View IF EXISTS Access;
Drop View IF EXISTS Accessv2;
Drop View IF EXISTS cidName;

Drop View IF EXISTS AvgHDI;
Drop View IF EXISTS Top10AvgHDI;

Drop View IF EXISTS HDI2009;
Drop View IF EXISTS HDI2010;
Drop View IF EXISTS HDI2011;
Drop View IF EXISTS HDI2012;
Drop View IF EXISTS HDI2013;

Drop View IF EXISTS numFollowers;
Drop View IF EXISTS numFollowersByReligion;

Drop View IF EXISTS cid2name;
Drop View IF EXISTS cid1name;
Drop View IF EXISTS PopLanguageNeighbor;
Drop View IF EXISTS PopLanguagec1;
Drop View IF EXISTS PopLanguage;
Drop View IF EXISTS PerPopLanguage;

Drop View IF EXISTS countryDeep;
Drop View IF EXISTS countryHigh;
Drop View IF EXISTS countrySpanWithOcean;
Drop View IF EXISTS countryWithOcean;
Drop View IF EXISTS countryWithoutOcean;
Drop View IF EXISTS countrySpanWithoutOcean;
Drop View IF EXISTS countrySpan;
Drop View IF EXISTS countryMaxSpan;

Drop View IF EXISTS borderLength;

-- Query 1 statements

CREATE VIEW maxHeightNeighbour AS
Select country.cid AS cid, max(height) AS maxHeightNeighbour
From country JOIN neighbour ON neighbor = country.cid
Group By cid;

CREATE VIEW neighbourHeight AS
Select country.cid, neighbor, height
From country JOIN neighbour ON country.cid = neighbor;

CREATE VIEW cidNeighbourPair AS
Select maxHeightNeighbour.cid AS c1id, neighbourHeight.neighbor AS c2id
From maxHeightNeighbour JOIN neighbourHeight ON maxHeightNeighbour.cid =
neighbourHeight.cid
Where height = maxHeightNeighbour;

CREATE VIEW cid1Name AS
Select c1id, cname AS c1name, c2id
From cidNeighbourPair JOIN country ON cidNeighbourPair.c1id = country.cid;

Insert Into Query1
Select c1id, c1name, c2id, cname AS c2name
From cid1Name JOIN country ON cid1Name.c2id = country.cid
Order By c1name ASC;

Drop View cid1name;
Drop View cidNeighbourPair;
Drop View neighbourHeight;
Drop View maxHeightNeighbour;

-- Query 2 statements

CREATE VIEW cidCountry AS
Select cid
From country;

CREATE VIEW oceanCountry AS
Select cid
From OceanAccess;

CREATE VIEW landlockedcid AS
Select cid
From 
(Select cid
From country) AS A
EXCEPT 
(Select cid
From OceanAccess);

Insert Into Query2
Select landlockedcid.cid AS cid, cname
From landlockedcid JOIN country ON landlockedcid.cid = country.cid;

Drop View landlockedcid;
Drop View oceanCountry;
Drop View cidCountry;


-- Query 3 statements

CREATE VIEW cidCountry AS
Select cid
From country;

CREATE VIEW oceanCountry AS
Select cid From OceanAccess;

CREATE VIEW landlockedcid AS
Select cid
From 
(Select cid
From country) AS A
EXCEPT 
(Select cid
From OceanAccess);

CREATE VIEW landlockedNeighbours AS
Select landlockedcid.cid AS cid
From landlockedcid JOIN Neighbour ON landlockedcid.cid = neighbour.country
Group By cid
Having count(neighbor) = 1;

CREATE VIEW landlockedNeighboursv2 AS
Select landlockedNeighbours.cid AS cid, neighbor
From	landlockedNeighbours JOIN neighbour ON landlockedNeighbours.cid = neighbor;

CREATE VIEW c1name AS
Select landlockedNeighboursv2.cid AS c1id, cname AS c1name, neighbor AS c2id
From landlockedNeighboursv2 JOIN country ON landlockedNeighboursv2.cid = country.cid;

Insert Into Query3
Select c1id, c1name, c2id, cname AS c2name
From c1name JOIN country ON c1name.c2id = country.cid
ORDER By c1name ASC;

Drop View c1name;
Drop View landlockedNeighboursv2;
Drop View landlockedNeighbours;
Drop View landlockedcid;
Drop View oceanCountry;
Drop View cidCountry;


-- Query 4 statements

CREATE VIEW directAccess AS
Select cid, oid
From oceanAccess;

CREATE VIEW indirectAccess AS
Select neighbour.neighbor AS cid, directAccess.oid AS oid
From directAccess JOIN Neighbour ON directAccess.cid = neighbour.country;

CREATE VIEW Access AS
Select cid, oid
From 
(Select cid, oid
From oceanAccess) AS A 
UNION 
(Select neighbour.neighbor AS cid, directAccess.oid AS oid
From directAccess JOIN Neighbour ON directAccess.cid = neighbour.country);

CREATE VIEW Accessv2 AS
Select cid, oid
From Access
Group By cid, oid;

CREATE VIEW cidName AS
Select cname, oid
From Accessv2 JOIN country ON Accessv2.cid = country.cid;

Insert Into Query4
Select cname, oname
From cidName JOIN ocean ON cidName.oid = ocean.oid;

Drop View cidName;
Drop View Accessv2;
Drop View Access;
Drop View indirectAccess;
Drop View directAccess;


-- Query 5 statements

CREATE VIEW AvgHDI AS
Select country.cid AS cid, avg(hdi_score) AS avghdi
From country JOIN hdi ON country.cid = hdi.cid
Where year > 2008 AND year < 2014
Group By country.cid
Order By avg(hdi_score) DESC;

CREATE VIEW Top10AvgHDI AS
Select *
From AvgHDI LIMIT 10;

Insert Into Query5
Select country.cid AS cid, cname, avghdi
From country JOIN Top10AvgHDI ON country.cid = Top10AvgHDI.cid;

Drop View Top10AvgHDI;
Drop View AvgHDI;

-- Query 6 statements

CREATE VIEW HDI2009 AS
Select country.cid AS cid, cname, hdi_score AS score2009
From country JOIN hdi ON country.cid = hdi.cid
Where year = 2009;

CREATE VIEW HDI2010 AS
Select country.cid AS cid, cname, hdi_score AS score2010
From country JOIN hdi ON country.cid = hdi.cid
Where year = 2010;

CREATE VIEW HDI2011 AS
Select country.cid AS cid, cname, hdi_score AS score2011
From country JOIN hdi ON country.cid = hdi.cid
Where year = 2011;

CREATE VIEW HDI2012 AS
Select country.cid AS cid, cname, hdi_score AS score2012
From country JOIN hdi ON country.cid = hdi.cid
Where year = 2012;

CREATE VIEW HDI2013 AS
Select country.cid AS cid, cname, hdi_score AS score2013
From country JOIN hdi ON country.cid = hdi.cid
Where year = 2013;

Insert Into Query6
Select HDI2009.cid AS cid, HDI2009.cname
From HDI2009, HDI2010, HDI2011, HDI2012, HDI2013
Where score2009 < score2010 AND score2010 < score2011 AND 
score2011 < score2012 AND score2012 < score2013 
AND HDI2009.cid = HDI2010.cid AND HDI2010.cid = HDI2011.cid
AND HDI2011.cid = HDI2012.cid AND HDI2012.cid = HDI2013.cid
Order By HDI2009.cname ASC;

Drop View HDI2013;
Drop View HDI2012;
Drop View HDI2011;
Drop View HDI2010;
Drop View HDI2009;

-- Query 7 statements

CREATE VIEW numFollowers AS
Select rid, (rpercentage * population) AS numFollowers
From country JOIN religion ON country.cid = religion.cid;

CREATE VIEW numFollowersByReligion AS
Select rid, sum(numFollowers) AS followers
From numFollowers
Group By rid;

Insert Into Query7
Select religion.rid, rname, followers
From religion JOIN numFollowersByReligion ON religion.rid = numFollowersByReligion.rid
Order By followers DESC;

Drop View numFollowersByReligion;
Drop View numFollowers;

-- Query 8 statements

CREATE VIEW PerPopLanguage AS
Select cid, max(lpercentage) AS populanguage
From language
Group By cid;

CREATE VIEW PopLanguage AS
Select language.cid AS cid, lid
From language JOIN PerPopLanguage ON language.cid = PerPopLanguage.cid
Where language.lpercentage = PerPopLanguage.populanguage;

CREATE VIEW PopLanguagec1 AS
Select cid AS cid1, lid AS lid1, neighbor AS cid2
From neighbour JOIN PopLanguage ON neighbour.country = PopLanguage.cid;

CREATE VIEW PopLanguageNeighbor AS
Select cid1, cid2, lid
From PopLanguagec1 JOIN PopLanguage ON cid2 = cid
Where lid1 = lid;

CREATE VIEW cid1name AS
Select cname as c1name, cid2, lid
From PopLanguageNeighbor JOIN country ON cid1 = cid;

CREATE VIEW cid2name AS
Select c1name, cname AS c2name, lid
From cid1name JOIN country ON cid2 = cid;

Insert Into Query8
Select c1name, c2name, lname
From cid2name JOIN language ON cid2name.lid = language.lid
Order By lname ASC, c1name DESC;

Drop View cid2name;
Drop View cid1name;
Drop View PopLanguageNeighbor;
Drop View PopLanguagec1;
Drop View PopLanguage;
Drop View PerPopLanguage;


-- Query 9 statements

CREATE VIEW countryDeep AS
Select country.cid, max(depth) AS maxDepth
From country, ocean, oceanAccess
Where country.cid = oceanAccess.cid AND ocean.oid = oceanAccess.oid
Group By country.cid;

CREATE VIEW countryHigh AS
Select cid, height
From country;

CREATE VIEW countrySpanWithOcean AS
Select countryDeep.cid AS cid, (height + maxDepth) AS totalspan
From countryDeep JOIN countryHigh ON countryDeep.cid = countryHigh.cid;

CREATE VIEW countryWithOcean AS
Select cid
From CountrySpanWithOcean;

CREATE VIEW countryWithoutOcean AS
Select cid
From (Select cid From country) AS A
EXCEPT 
(Select cid
From CountrySpanWithOcean);

CREATE VIEW countrySpanWithoutOcean AS
Select country.cid AS cid, height AS totalspan
From countryWithoutOcean JOIN country ON countryWithoutOcean.cid = country.cid;

CREATE VIEW countrySpan AS
Select cid, totalspan
From (
Select countryDeep.cid AS cid, (height + maxDepth) AS totalspan
From countryDeep JOIN countryHigh ON countryDeep.cid = countryHigh.cid) AS A
UNION
(Select country.cid AS cid, height AS totalspan
From countryWithoutOcean JOIN country ON countryWithoutOcean.cid = country.cid)
Order By totalspan DESC;

CREATE VIEW countryMaxSpan AS
Select cid, totalspan
From countrySpan LIMIT 1;

Insert Into Query9
Select cname, totalspan
From countryMaxSpan JOIN country ON countryMaxSpan.cid = country.cid;

Drop View countryMaxSpan;
Drop View countrySpan;
Drop View countrySpanWithoutOcean;
Drop View countryWithoutOcean;
Drop View countryWithOcean;
Drop View countrySpanWithOcean;
Drop View countryHigh;
Drop View countryDeep;




-- Query 10 statements

CREATE VIEW borderLength AS
Select neighbour.country, sum(length) AS borderslength
From neighbour
Group By neighbour.country Order By sum(length) DESC;

Insert Into Query10
Select cname, borderslength
From country JOIN borderLength ON cid = borderLength.country LIMIT 1;

Drop View borderLength;
