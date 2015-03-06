-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW neighborsinfo as SELECT cid as neighbor, cname as nname, height as nheight FROM country;
CREATE VIEW countryinfo as SELECT cid as country, cname, height FROM country;
CREATE VIEW listneighbors as SELECT * FROM neighborsinfo natural join neighbour;
CREATE VIEW biglisting as SELECT * FROM listneighbors natural join countryinfo;

CREATE VIEW countryAnswers as SELECT country, max(nheight) FROM biglisting GROUP BY country;
INSERT INTO Query1 (
		SELECT country as c1id, cname as c1name, neighbor as c2id, nname as c2name 
		FROM biglisting natural join countryanswers 
		WHERE countryanswers.max = biglisting.nheight 
		ORDER BY c1name
);

DROP VIEW IF EXISTS neighborsinfo CASCADE;
DROP VIEW IF EXISTS countryinfo CASCADE;
DROP VIEW IF EXISTS listneighbors CASCADE;
DROP VIEW IF EXISTS biglisting CASCADE;
DROP VIEW IF EXISTS countryanswers CASCADE;

-- Query 2 statements
INSERT INTO Query2 (SELECT cid
					FROM country
					WHERE cid NOT IN 
								(SELECT cid FROM oceanAccess)); 

-- Query 3 statements
-- Used as Reference
CREATE VIEW CODETONAME as SELECT cid, cname FROM country;


CREATE VIEW landLocked as SELECT cid
						  FROM country
						  WHERE cid NOT IN
						  				(SELECT cid FROM oceanAccess);
ALTER TABLE landLocked RENAME COLUMN cid TO country;
CREATE VIEW landNeighbor as SELECT * FROM neighbour natural join landLocked;
CREATE VIEW winners as SELECT country FROM landNeighbor GROUP BY country HAVING count(neighbor) = 1;
CREATE VIEW winnerview as (SELECT * FROM neighbour natural join winners);
CREATE VIEW COUNTRYWIN as SELECT cid, cname FROM CODETONAME, (SELECT country FROM winnerview) v WHERE cid=v.country;
CREATE VIEW NEIGHBORWIN as SELECT cid, cname FROM CODETONAME, (SELECT neighbor FROM winnerview) v WHERE cid=v.neighbor;

ALTER TABLE COUNTRYWIN RENAME COLUMN cid TO c1id;
ALTER TABLE COUNTRYWIN RENAME COLUMN cname TO c1name;
ALTER TABLE NEIGHBORWIN RENAME COLUMN cname TO c2name;
ALTER TABLE NEIGHBORWIN RENAME COLUMN cid TO c2id;

INSERT INTO Query3 (
	SELECT * FROM COUNTRYWIN, NEIGHBORWIN ORDER BY c1name ASC
);

DROP VIEW IF EXISTS CODETONAME CASCADE;
DROP VIEW IF EXISTS landLocked CASCADE;
DROP VIEW IF EXISTS landNeighbor CASCADE;
DROP VIEW IF EXISTS winners CASCADE;
DROP VIEW IF EXISTS winnerview CASCADE;
DROP VIEW IF EXISTS COUNTRYWIN CASCADE;
DROP VIEW IF EXISTS NEIGHBORWIN CASCADE;

-- Query 4 statements
CREATE VIEW directAccess as SELECT cid, oid FROM oceanAccess GROUP BY cid, oid;
CREATE VIEW inDirectAccess as 
						SELECT * 
						FROM neighbour, directAccess
						WHERE directAccess.cid = neighbor;
CREATE VIEW directAndIndirect as SELECT * FROM directAccess UNION SELECT country, oid FROM inDirectAccess;
CREATE VIEW CIDSwithOceanNames as SELECT * FROM directAndIndirect natural join ocean;
INSERT INTO Query4 (
	SELECT cname, oname FROM cidswithoceannames natural join country ORDER BY cname ASC, oname DESC
);

DROP VIEW IF EXISTS directAccess CASCADE;
DROP VIEW IF EXISTS inDirectAccess CASCADE;
DROP VIEW IF EXISTS directAndIndirect CASCADE;
DROP VIEW IF EXISTS CIDSwithOceanNames CASCADE;

-- Query 5 statements
CREATE VIEW filteredYears as SELECT * FROM hdi WHERE year >= 2009 and year <= 2013;
INSERT INTO Query5 (
	SELECT avg(hdi_score) FROM filteredYears GROUP BY cid ORDER BY avg(hdi_score) ASC LIMIT 10
);

DROP VIEW IF EXISTS filteredYears CASCADE;

-- Query 6 statements
CREATE VIEW CODETONAME as SELECT cid, cname FROM country;

CREATE VIEW inc2009 as SELECT * FROM hdi WHERE year=2009;
ALTER TABLE inc2009 RENAME COLUMN year TO incyear;
ALTER TABLE inc2009 RENAME COLUMN hdi_score TO inchdi_score;

CREATE VIEW inc2010 as SELECT cid, year, hdi_score FROM hdi natural join inc2009 WHERE year=2010 and incyear=2009 and hdi_score > inchdi_score;
ALTER TABLE inc2010 RENAME COLUMN year TO incyear;
ALTER TABLE inc2010 RENAME COLUMN hdi_score TO inchdi_score;

CREATE VIEW inc2011 as SELECT cid, year, hdi_score FROM hdi natural join inc2010 WHERE year=2011 and incyear=2010 and hdi_score > inchdi_score;
ALTER TABLE inc2011 RENAME COLUMN year TO incyear;
ALTER TABLE inc2011 RENAME COLUMN hdi_score TO inchdi_score;

CREATE VIEW inc2012 as SELECT cid, year, hdi_score FROM hdi natural join inc2011 WHERE year=2012 and incyear=2011 and hdi_score > inchdi_score;
ALTER TABLE inc2012 RENAME COLUMN year TO incyear;
ALTER TABLE inc2012 RENAME COLUMN hdi_score TO inchdi_score;

CREATE VIEW inc2013 as SELECT cid, year, hdi_score FROM hdi natural join inc2012 WHERE year=2013 and incyear=2012 and hdi_score > inchdi_score;
INSERT INTO Query6 (
	SELECT cid, cname FROM CODETONAME natural join inc2013 ORDER BY cname ASC
);

DROP VIEW IF EXISTS CODETONAME CASCADE;
DROP VIEW IF EXISTS inc2009 CASCADE;
DROP VIEW IF EXISTS inc2010 CASCADE;
DROP VIEW IF EXISTS inc2011 CASCADE;
DROP VIEW IF EXISTS inc2012 CASCADE;
DROP VIEW IF EXISTS inc2013 CASCADE;


-- Query 7 statements
INSERT INTO Query7 (
 	SELECT rname, sum(religion.rpercentage * country.population) as followers 
 	FROM religion natural join country 
 	GROUP BY rname 
 	ORDER BY followers DESC
);


-- Query 8 statements
CREATE VIEW countrylisting as 
				SELECT cid, cname, lname, lpercentage, population, (lpercentage * population) as numspeakers 
				from language natural join country ;
CREATE VIEW mostpopularlang as SELECT cname, max(numspeakers) as numspeakers FROM countrylisting GROUP BY cname;
CREATE VIEW countryandlang as SELECT cname, cid, lname FROM countrylisting natural join mostpopularlang;
CREATE VIEW neighborlang as SELECT * FROM countryandlang;


ALTER TABLE neighborlang RENAME COLUMN cname to nname;
ALTER TABLE neighborlang RENAME COLUMN cid to neighbor;
ALTER TABLE neighborlang RENAME COLUMN lname to nlname;

ALTER TABLE countryandlang RENAME COLUMN cid to country;
CREATE VIEW countrylangxneighbour as SELECT * FROM countryandlang natural join neighbour;
CREATE VIEW answertable as SELECT * FROM countrylangxneighbour natural join neighborlang;
INSERT INTO Query8  (
	SELECT country as c1id, cname as c1name, neighbor as c2id, nname as c2name 
	FROM answertable 
	WHERE lname = nlname 
	ORDER BY c1name ASC
);


DROP VIEW IF EXISTS countrylisting CASCADE;
DROP VIEW IF EXISTS mostpopularlang CASCADE;
DROP VIEW IF EXISTS countryandlang CASCADE;
DROP VIEW IF EXISTS neighborlang CASCADE;
DROP VIEW IF EXISTS countrylangxneighbour CASCADE;
DROP VIEW IF EXISTS answertable CASCADE;


-- Query 9 statements
CREATE VIEW R1 as SELECT * FROM oceanAccess natural join ocean;
CREATE VIEW hasOceanAcc as SELECT cid, sum(depth + height)/2 as height FROM R1 natural join country GROUP BY cid;

CREATE VIEW hasNoOcean as SELECT cid, height FROM country WHERE cid NOT IN (SELECT cid FROM oceanAccess);

CREATE VIEW allCountries as (SELECT cid, sum(depth + height) as height FROM R1 natural join country GROUP BY cid) 
 								UNION 
 							 (SELECT cid, height FROM country WHERE cid NOT IN (SELECT cid FROM oceanAccess))

CREATE VIEW ANSWER as SELECT * FROM allCountries ORDER BY height DESC LIMIT 1;
ALTER TABLE ANSWER RENAME COLUMN height TO totalspan;

INSERT INTO Query9 (
	SELECT cname, totalspan FROM country natural join ANSWER
);


DROP VIEW IF EXISTS R1 CASCADE;
DROP VIEW IF EXISTS hasNoOcean CASCADE;
DROP VIEW IF EXISTS hasOceanAcc CASCADE;
DROP VIEW IF EXISTS allCountries CASCADE;
DROP VIEW IF EXISTS ANSWER CASCADE;


-- Query 10 statements
CREATE VIEW longestBorder as SELECT country, sum(length) FROM neighbour GROUP BY country;
ALTER TABLE longestBorder RENAME COLUMN country TO cid;
INSERT INTO Query10 (
	SELECT cname, sum as borderslength 
	FROM longestBorder natural join country 
	ORDER BY borderslength DESC LIMIT 1
);

DROP VIEW IF EXISTS longestBorder CASCADE;

