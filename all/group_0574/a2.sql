-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW COUNTRY_AND_NEI AS 
SELECT cid AS countryid, cname AS countryname, neighbor
FROM country INNER JOIN neighbour
ON cid = country;

CREATE VIEW COUNTRY_NEI_HEIGHT AS
SELECT countryid, countryname, cid AS ncid, cname AS neiname, height
FROM COUNTRY_AND_NEI INNER JOIN COUNTRY
ON neighbor = cid;

INSERT INTO Query1
(
SELECT CNH.countryid AS c1id, CNH.countryname AS c1name, ncid AS c2id, neiname AS c2name
FROM COUNTRY_NEI_HEIGHT CNH 
INNER JOIN (
	SELECT countryid, MAX(height) AS maxheight
	FROM COUNTRY_NEI_HEIGHT
	GROUP BY countryid
	) CNH2 ON CNH2.maxheight = CNH.height AND CNH2.countryid = CNH.countryid
ORDER BY c1name ASC
);

DROP VIEW COUNTRY_NEI_HEIGHT;
DROP VIEW COUNTRY_AND_NEI;



-- Query 2 statements

INSERT INTO Query2
(
SELECT country.cid, country.cname
FROM country
EXCEPT
SELECT DISTINCT country.cid, country.cname
FROM country
NATURAL JOIN
oceanAccess
ORDER BY cname ASC);



-- Query 3 statements

CREATE VIEW landlocked_country AS
SELECT country.cid, country.cname
FROM country
EXCEPT
SELECT DISTINCT country.cid, country.cname
FROM country
NATURAL JOIN
oceanAccess
ORDER BY cname ASC;


CREATE VIEW landlocked_country_with_neighbour AS
SELECT cid AS c1id, cname AS c1name, neighbor AS c2id, ccname AS c2name
FROM
(landlocked_country INNER JOIN neighbour ON country = cid) t
INNER JOIN (SELECT cid AS ccid, cname AS ccname
FROM country) renamed_table
ON neighbor = renamed_table.ccid;

INSERT INTO Query3
(
SELECT *
FROM
(SELECT c1id
FROM landlocked_country_with_neighbour
GROUP BY c1id HAVING COUNT(c2id) < 2) landlocked_country_with_one_neighbour
NATURAL JOIN landlocked_country_with_neighbour
ORDER BY c1name ASC);

DROP VIEW landlocked_country_with_neighbour;
DROP VIEW landlocked_country;



-- Query 4 statements
CREATE VIEW INDIRECT_ACCESS AS
SELECT cname, oname
FROM country INNER JOIN 
	(SELECT cid, oname
	FROM ocean INNER JOIN
		(SELECT N.neighbor AS cid, O.oid AS oid
		FROM oceanAccess O INNER JOIN neighbour N
		ON O.cid = N.country) A ON ocean.oid = A.oid) B 
	ON country.cid = B.cid;

CREATE VIEW DIRECT_ACCESS AS
SELECT cname, oname
FROM ocean INNER JOIN 
	(SELECT C.cname AS cname, O.oid AS oid
	FROM oceanAccess O INNER JOIN country C ON O.cid = C.cid) A
	ON ocean.oid = A.oid;

INSERT INTO Query4
(
SELECT cname, oname FROM INDIRECT_ACCESS
UNION
SELECT cname, oname FROM DIRECT_ACCESS
ORDER BY cname ASC, oname DESC);

DROP VIEW DIRECT_ACCESS;
DROP VIEW INDIRECT_ACCESS;



-- Query 5 statements

CREATE VIEW CID_AVG AS
SELECT A.cid AS cid, avg(A.hdi_score) AS AVG
FROM (SELECT cid, hdi_score
	  FROM hdi
	  WHERE year >= 2009 AND year <= 2013) A
GROUP BY A.cid
ORDER BY AVG DESC
LIMIT 5;
INSERT INTO Query5
(
SELECT CID_AVG.cid AS cid, cname, avg AS avghid
FROM CID_AVG INNER JOIN country ON country.cid = CID_AVG.cid
);

DROP VIEW CID_AVG;


-- Query 6 statements

CREATE VIEW HDI_2009 AS
SELECT cid, hdi_score
FROM hdi
WHERE year = 2009;

CREATE VIEW HDI_2010 AS
SELECT cid, hdi_score
FROM hdi
WHERE year = 2010;

CREATE VIEW HDI_2011 AS
SELECT cid, hdi_score
FROM hdi
WHERE year = 2011;


CREATE VIEW HDI_2012 AS
SELECT cid, hdi_score
FROM hdi
WHERE year = 2012;

CREATE VIEW HDI_2013 AS
SELECT cid, hdi_score
FROM hdi
WHERE year = 2013;

CREATE VIEW POS_CID AS
SELECT HDI_2013.cid AS cid
FROM (SELECT HDI_2012.cid AS cid, HDI_2012.hdi_score AS hdi_score
	FROM (SELECT HDI_2011.cid AS cid, HDI_2011.hdi_score AS hdi_score 
		FROM (SELECT HDI_2010.cid AS cid, HDI_2010.hdi_score AS hdi_score
	 		FROM HDI_2009 INNER JOIN HDI_2010
	 		ON HDI_2009.cid = HDI_2010.cid
	 		WHERE HDI_2009.hdi_score < HDI_2010.hdi_score) HDI_2010_POS
		INNER JOIN HDI_2011 ON HDI_2010_POS.cid = HDI_2011.cid
		WHERE HDI_2011.hdi_score > HDI_2010_POS.hdi_score) HDI_2011_POS
	INNER JOIN HDI_2012 ON HDI_2012.cid = HDI_2011_POS.cid
	WHERE HDI_2012.hdi_score > HDI_2011_POS.hdi_score) HDI_2012_POS
INNER JOIN HDI_2013 ON HDI_2013.cid > HDI_2012_POS.cid
WHERE HDI_2013.hdi_score > HDI_2012_POS.hdi_score;

CREATE VIEW CNAME_CID AS
SELECT POS_CID.cid AS cid, country.cname AS cname
FROM POS_CID INNER JOIN country ON POS_CID.cid = country.cid;

INSERT INTO Query6
(
SELECT A.cid AS cid, country.cname AS cname
FROM	(SELECT cid
	FROM CNAME_CID
	GROUP BY cid) A INNER JOIN country
ON A.cid = country.cid
ORDER BY cname ASC);

DROP VIEW CNAME_CID;
DROP VIEW POS_CID;
DROP VIEW HDI_2013;
DROP VIEW HDI_2012;
DROP VIEW HDI_2011;
DROP VIEW HDI_2010;
DROP VIEW HDI_2009;

-- Query 7 statements

CREATE VIEW c_religion_pop AS
SELECT religion.rid AS rid, country.population * religion.rpercentage AS pop_rel
FROM country INNER JOIN religion ON country.cid = religion.cid;

INSERT INTO Query7(
SELECT A.rid AS rid, C.rname AS rname, A.followers AS followers
FROM (SELECT rid, SUM(pop_rel) AS followers
	 FROM c_religion_pop
	 GROUP BY rid) A INNER JOIN 
	(SELECT B.rid AS rid, B.rname AS rname
	FROM (SELECT rid, rname FROM religion) B
	GROUP BY B.rid, B.rname) C
ON A.rid = C.rid
ORDER BY followers DESC);

DROP VIEW c_religion_pop;


-- Query 8 statements
CREATE VIEW COU_LANPER AS
SELECT cid, lpercentage
FROM language;

CREATE VIEW COU_MPOPLAN AS
SELECT L1.cid AS cid, language.lname AS lname 
FROM (SELECT cid, MAX(lpercentage) AS max_per
	  FROM COU_LANPER
	  GROUP BY cid) L1 INNER JOIN language
ON L1.cid = language.cid
WHERE L1.max_per = language.lpercentage;

CREATE VIEW CID_MLAN AS
SELECT C1.cid AS c1id, C2.cid AS c2id, C1.lname AS lname
FROM COU_MPOPLAN C1 INNER JOIN neighbour N 
ON C1.cid = N.country 
INNER JOIN COU_MPOPLAN C2
ON C2.cid = N.neighbor
WHERE C1.cid != C2.cid AND C1.lname = C2.lname;

INSERT INTO Query8
(
SELECT C.cname AS c1name, C2.cname AS c2name, C1.lname AS lname
FROM CID_MLAN C1 INNER JOIN country C
ON C1.c1id = C.cid
INNER JOIN country C2
ON C1.c2id = C2.cid
ORDER BY lname ASC, c1name DESC);

DROP VIEW CID_MLAN;
DROP VIEW COU_MPOPLAN;
DROP VIEW COU_LANPER;

-- Query 9 statements
CREATE VIEW COU_OCE_DEP AS
SELECT C.cid AS cid, O.depth AS depth, C.height AS height
FROM country C INNER JOIN oceanAccess OA
ON C.cid = OA.cid
INNER JOIN ocean O ON OA.oid = O.oid;


CREATE VIEW COU_MAXDEP AS
SELECT B.cid AS cid, MAX(B.depth) AS max_dep
FROM (SELECT A.cid, A.depth
	 FROM COU_OCE_DEP A) B
GROUP BY B.cid;

CREATE VIEW COU_MAXHEI AS
SELECT B.cid AS cid, MAX(B.height) AS max_hei
FROM (SELECT A.cid, A.height
	 FROM COU_OCE_DEP A) B
GROUP BY B.cid;

CREATE VIEW COU_MAX AS
SELECT H.cid AS cid, (H.max_hei + D.max_dep) AS totalspan
FROM COU_MAXHEI H INNER JOIN COU_MAXDEP D
ON H.cid = D.cid;

CREATE VIEW MAX AS
SELECT B.cid AS cid, A.totalspan AS totalspan
FROM (SELECT MAX(totalspan) AS totalspan
	FROM COU_MAX) A
INNER JOIN COU_MAX B 
ON A.totalspan = B.totalspan;

INSERT INTO Query9
(
SELECT country.cname AS cname, A.totalspan AS totalspan
FROM MAX A INNER JOIN country ON country.cid = A.cid);


DROP VIEW MAX;
DROP VIEW COU_MAX;
DROP VIEW COU_MAXHEI;
DROP VIEW COU_MAXDEP;
DROP VIEW COU_OCE_DEP;


-- Query 10 statements

CREATE VIEW CID_LENGTH AS
SELECT A.country AS cid, SUM(length) AS borderslength
FROM (SELECT country, length
	  FROM neighbour) A
GROUP BY A.country;

CREATE VIEW MAX_LENGTH AS
SELECT CID_LENGTH.cid AS cid, A.borderslength AS borderslength
FROM (SELECT MAX(borderslength) AS borderslength
	FROM CID_LENGTH) A INNER JOIN CID_LENGTH
ON A.borderslength = CID_LENGTH.borderslength;

INSERT INTO Query10
(
SELECT country.cname AS cname, A.borderslength AS borderslength
FROM MAX_LENGTH A
INNER JOIN country ON A.cid = country.cid);

DROP VIEW MAX_LENGTH;
DROP VIEW CID_LENGTH;








