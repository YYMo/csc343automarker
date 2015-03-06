-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW heightneighbours AS
SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name, c1.height AS c1height, c2.height AS c2height
FROM country c1, country c2, neighbour
WHERE c1.cid = neighbour.country AND c2.cid = neighbour.neighbor;

INSERT INTO Query1(SELECT h1.c1name, h1.c2name
FROM heightNeighbours h1
WHERE h1.c2height = (
	SELECT MAX(c2height)
	FROM heightNeighbours
	WHERE c1name = h1.c1name));




-- Query 2 statements

CREATE VIEW landlocked AS
SELECT cid, cname
FROm Country
WHERE country.cid NOT IN(
	SELECT cid
	FROM oceanAccess
	ORDER BY cname ASC);

INSERT INTO Query2(SELECT * FROM landlocked);





-- Query 3 statements

INSERT INTO Query3(

SELECT old.c1id, old.c1name, old.c2id, old.c2name
FROM heightNeighbours AS old
WHERE(SELECT COUNT(c1id)
	FROM heightNeighbours
	WHERE c1id = old.c1id) = 1
AND
old.c1ID IN(SELECT cid FROM landlocked)
ORDER BY old.c1name ASC);

DROP VIEW heightNeighbours;
DROP VIEW landlocked;



-- Query 4 statements

CREATE VIEW directAccess AS
SELECT country.cname, ocean.oname
FROM country, ocean, oceanAccess
WHERE country.cid = oceanAccess.cid
AND ocean.oid = oceanAccess.oid;

INSERT INTO Query4(SELECT * FROM directAccess);

DROP VIEW directAccess;

-- Query 5 statements

CREATE VIEW hdis AS
SELECT cid, AVG(hdi_score) AS avghdi
FROM hdi
WHERE year > 2008 AND year < 2014
GROUP BY hdi.cid;


INSERT INTO Query5(SELECT DISTINCT hdi.cid, cname, avghdi
FROM hdis, country, hdi
WHERE country.cid = hdi.cid AND hdi.cid = hdis.cid
ORDER BY avghdi DESC);

DROP VIEW hdis;

-- Query 6 statements



-- Query 7 statements



-- Query 8 statements



-- Query 9 statements



-- Query 10 statements

CREATE VIEW summedup AS       
SELECT COUNTRY, SUM(length)
FROM neighbour
GROUP BY country;

CREATE VIEW almost AS
SELECT country, sum
FROM summedup
WHERE sum = (SELECT MAX(sum) FROM summedup);


INSERT INTO Query10 (

	

	SELECT country.cname, sum
	FROM country, almost
	WHERE country.cid = almost.country

);

DROP VIEW almost;
DROP VIEW summedup;



