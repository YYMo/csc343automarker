-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

SET search_path TO A2;



-- Query 1 statements
DROP VIEW IF EXISTS nboursheight CASCADE;

CREATE VIEW nboursheight as
select C.cid as c1id, C.cname as c1name, H.cid as c2id, H.cname as c2name, H.height as c2Height
from country C cross join country H
where exists (select country, neighbor from neighbour where country = C.cid and neighbor = H.cid)
;

-- HERE IS THE INSERT 1 --
INSERT INTO Query1 (c1id, c1name, c2id, c2name)  (

SELECT A.c1id, A.c1name, A.c2id, A.c2name
FROM nboursheight A
WHERE  A.c2height >= ALL(SELECT  c2height
                     FROM nboursheight B
                     WHERE B.c1id = A.c1id)
ORDER BY A.c1name ASC
);


DROP VIEW IF EXISTS nboursheight CASCADE;


-- Query 2 statements
DROP VIEW IF EXISTS LandLockedCountries CASCADE;

CREATE VIEW LandLockedCountries as
SELECT distinct cid
FROM ( (SELECT cid FROM country)
                EXCEPT
       (SELECT cid FROM oceanAccess)) as TEMP;

-- HERE IS THE INSERT 2 --
INSERT INTO Query2 (cid, cname) (

SELECT  country.cid as cid, country.cname as cname
FROM LandLockedCountries JOIN country on LandLockedCountries.cid = country.cid
ORDER BY country.cname ASC
);


DROP VIEW IF EXISTS LandLockedCountries CASCADE;


-- Query 3 statements
DROP VIEW IF EXISTS LandLockedCountries2 CASCADE;
DROP VIEW IF EXISTS OneNeighbour CASCADE;
DROP VIEW IF EXISTS LLandON CASCADE;
DROP VIEW IF EXISTS CountryPlusNeighbour CASCADE;


CREATE VIEW LandLockedCountries2 as
SELECT distinct cid
FROM ( (SELECT cid FROM country)
                EXCEPT
       (SELECT cid FROM oceanAccess)) as TEMP;



CREATE VIEW OneNeighbour as
SELECT distinct country as cid
FROM neighbour
GROUP BY country  HAVING (count(neighbor) = 1);



CREATE VIEW LLandON as
SELECT cid
FROM  ((SELECT * FROM LandLockedCountries2) INTERSECT (SELECT * FROM OneNeighbour)) AS TEMP2;

CREATE VIEW CountryPlusNeighbour as
SELECT neighbour.country, neighbour.neighbor
FROM LLandON JOIN neighbour ON LLandON.cid = neighbour.country;


-- HERE IS THE INSERT 3 --
INSERT INTO Query3 (c1id, c1name, c2id, c2name) (
SELECT B.cid as c1id, B.cname as c1name, C.cid as c2id, C.cname as c2name
FROM CountryPlusNeighbour A JOIN country B ON A.country = B.cid
                            JOIN country C ON A.neighbor = C.cid
ORDER BY B.cname ASC
);

DROP VIEW IF EXISTS LandLockedCountries2 CASCADE;
DROP VIEW IF EXISTS OneNeighbour CASCADE;
DROP VIEW IF EXISTS LLandON CASCADE;
DROP VIEW IF EXISTS CountryPlusNeighbour CASCADE;



-- Query 4 statements
DROP VIEW IF EXISTS DirectAccess CASCADE;
DROP VIEW IF EXISTS IndirectAccess CASCADE;
DROP VIEW IF EXISTS Partial CASCADE;

CREATE VIEW DirectAccess as
SELECT A.cid as cid, B.oname as oname
FROM oceanAccess A JOIN ocean B ON A.oid = B.oid;

CREATE VIEW IndirectAccess as
SELECT A.country as cid, C.oname as oname
FROM neighbour A JOIN oceanAccess B ON A.neighbor = B.cid
                 JOIN ocean C ON B.oid = C.oid
WHERE B.cid IN (SELECT cid FROM oceanAccess) ORDER BY A.country;

CREATE VIEW Partial as
SELECT Partial.cid as cid, Partial.oname as oname
FROM ((SELECT * FROM DirectAccess) UNION (SELECT * FROM IndirectAccess)) as PARTIAL;



-- HERE IS THE INSERT 4 --
INSERT INTO Query4 (cname, oname) (
SELECT B.cname as cname, A.oname as oname
FROM Partial A JOIN country B ON A.cid = B.cid
ORDER BY B.cname ASC, A.oname DESC
);


DROP VIEW IF EXISTS DirectAccess CASCADE;
DROP VIEW IF EXISTS IndirectAccess CASCADE;
DROP VIEW IF EXISTS Partial CASCADE;



-- Query 5 statements
DROP VIEW IF EXISTS HDIPeriod CASCADE;
DROP VIEW IF EXISTS Rank10 CASCADE;

CREATE VIEW HDIPeriod as
SELECT *
FROM hdi A
WHERE A.year >= 2009 AND A.year <=2013;

CREATE VIEW Rank10 as
SELECT cid, avg(hdi_score) as avghdi
FROM HDIPeriod
GROUP BY cid
ORDER BY avg(hdi_score) DESC
LIMIT 10;


-- HERE IS THE INSERT 5 --
INSERT INTO Query5 (cid, cname, avghdi) (
SELECT A.cid as cid, B.cname as cname, A.avghdi as avghdi
FROM Rank10 A JOIN country B ON A.cid = B.cid
ORDER BY A.avghdi DESC
);


DROP VIEW IF EXISTS HDIPeriod CASCADE;
DROP VIEW IF EXISTS Rank10 CASCADE;





-- Query 6 statements
DROP VIEW IF EXISTS OnlyPeriod CASCADE;
DROP VIEW IF EXISTS Decrease CASCADE;
DROP VIEW IF EXISTS Temp3 CASCADE;


CREATE VIEW OnlyPeriod AS
SELECT *
FROM hdi A
WHERE A.year >= 2009 and A.year <= 2013
ORDER BY A.cid, A.year DESC;


CREATE VIEW Decrease AS
SELECT *
FROM OnlyPeriod A
WHERE A.hdi_score <=  ANY(SELECT B.hdi_score FROM OnlyPeriod B WHERE B.cid = A.cid AND  B.year < A.year);

CREATE VIEW Temp3 AS
SELECT TEMP.cid as cid
FROM ( (SELECT A.cid FROM OnlyPeriod A) EXCEPT (SELECT B.cid FROM Decrease B) ) TEMP;




-- HERE IS THE INSERT 6 --
INSERT INTO Query6 (cid, cname) (
SELECT B.cid as cid, B.cname as cname
FROM Temp3 A JOIN country B ON A.cid = B.cid
ORDER BY B.cname ASC
);

DROP VIEW IF EXISTS OnlyPeriod CASCADE;
DROP VIEW IF EXISTS Decrease CASCADE;
DROP VIEW IF EXISTS Temp3 CASCADE;




-- Query 7 statements
--HERE IS THE INSERT 7 --
INSERT INTO Query7(rid, rname, followers)(

SELECT A.rid as rid, A.rname as rname, sum(A.rpercentage*B.population) as followers

FROM religion A JOIN country B on A.cid = B.cid
GROUP BY A.rid, A.rname
ORDER BY sum(A.rpercentage*B.population) DESC
);


-- Query 8 statements
DROP VIEW IF EXISTS MostPopLang CASCADE;
DROP VIEW IF EXISTS Partial2 CASCADE;


CREATE VIEW MostPopLang as
SELECT A.cid, A.lid, A.lname
FROM language A
WHERE A.lpercentage >= ALL (SELECT B.lpercentage FROM language B WHERE B.cid = A.cid);


CREATE VIEW Partial2 as
SELECT A.cid as c1id , B.cid as c2id , A.lname as lname
FROM MostPopLang A JOIN MostPopLang B ON A.lid = B.lid
WHERE EXISTS (SELECT C.country, C.neighbor from neighbour C where C.country = A.cid and C.neighbor = B.cid) ;



-- HERE IS THE INSER 8 --
INSERT INTO Query8 (c1name, c2name, lname)(
SELECT B.cname as c1name, C.cname as c2name, A.lname as lname
FROM Partial2 A JOIN country B ON A.c1id = B.cid
                JOIN country C ON A.c2id = C.cid
ORDER BY A.lname ASC, B.cname DESC

);


DROP VIEW IF EXISTS MostPopLang CASCADE;
DROP VIEW IF EXISTS Partial2 CASCADE;




-- Query 9 statements
DROP VIEW IF EXISTS AllGlue CASCADE;
DROP VIEW IF EXISTS Largest CASCADE;


CREATE VIEW AllGlue AS
SELECT A.cid as cid, A.cname as cname, A.height as height, B.oid as oid, C.oname as oname, C.depth as depth
FROM country A JOIN oceanAccess B ON A.cid = B.cid
               JOIN ocean C ON B.oid = C.oid
;

CREATE VIEW Largest AS
SELECT A.cid as cid, A.cname as cname, A.height as height, A.oid as oid, A.oname as oname, A.depth as depth
FROM AllGlue A
WHERE  ABS(A.depth) >= ALL  (SELECT ABS(B.depth) FROM AllGlue B WHERE B.cid = A.cid);



-- HERE IS THE INSERT 9 --
INSERT INTO Query9 (cname, totalspan)(
SELECT A.cname as cname, ABS(A.height)+ABS(A.depth) as totalspan
FROM Largest A
WHERE ABS(A.height) + ABS(A.depth) >= ALL (SELECT ABS(B.height) + ABS(B.depth) as total FROM Largest B)
);

DROP VIEW IF EXISTS AllGlue CASCADE;
DROP VIEW IF EXISTS Largest CASCADE;




-- Query 10 statements

DROP VIEW IF EXISTS Length CASCADE;

CREATE VIEW Length AS
SELECT A.country as cid, sum(A.length) as tamanho
FROM neighbour A
GROUP BY A.country;



-- HERE IS THE INSERT 10
INSERT INTO Query10 (cname, borderslength)(
SELECT B.cname as cname, A.tamanho as borderslength
FROM Length A JOIN country B ON A.cid = B.cid
WHERE A.tamanho >= ALL (SELECT C.tamanho FROM Length C)
);


DROP VIEW IF EXISTS Length CASCADE;




