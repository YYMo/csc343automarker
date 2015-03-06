-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW NeighborHeightInfo AS
	SELECT neighbour.country as country, neighbour.neighbor as neighbor, country.height as height, country.cname as neighborname
	FROM country 
		INNER JOIN neighbour
			ON country.cid = neighbour.neighbor;

CREATE VIEW completeInfo as 
	SELECT NeighborHeightInfo.country as c1id, country.cname as c1name, NeighborHeightInfo.neighbor as c2id, NeighborHeightInfo.neighborname as c2name, NeighborHeightInfo.height as height
	FROM NeighborHeightInfo 
		INNER JOIN country
			ON country.cid = NeighborHeightInfo.country;

-- Single entry by country, neighbor and neighbor's max height
CREATE VIEW CountryNeighborMaxHeight AS
	SELECT completeInfo.c1id as c1id, completeInfo.c1name as c1name, completeInfo.c2id as c2id, completeInfo.c2name as c2name--, foo.mheight as height
	FROM completeInfo INNER JOIN
		(SELECT c1id, MAX(height) as mheight
			FROM completeInfo GROUP BY c1id) as foo
		ON completeInfo.c1id = foo.c1id and completeInfo.height = foo.mheight
	Order By c1name ASC;

INSERT INTO Query1 (SELECT * FROM CountryNeighborMaxHeight);

DROP VIEW NeighborHeightInfo CASCADE;

-- Query 2 statements
CREATE VIEW RESULT as
	select cid, cname
	From country
	where cid not in (
		select cid
		From oceanAccess
	)
	order by cname;

INSERT INTO Query2 (SELECT * FROM RESULT);
DROP VIEW RESULT cascade;



-- Query 3 statements

CREATE VIEW Landlocked as
	select  cid, cname
	from country
	where cid not in 
	(select cid From oceanAccess);

CREATE VIEW neighborList as

	select cid, cname, count(neighbor) as num
	from country inner join neighbour on (country.cid = neighbour.country)
	GROUP By cid;

CREATE VIEW one as
	select cid as country, cname, num
	From neighborList
	where num=1;

CREATE VIEW findC2id as
	select country, cname as c1name, neighbor
	from one natural join neighbour;

CREATE VIEW RESULT as
	select country, c1name, neighbor, cname
	from findC2id inner join country on findC2id.neighbor = country.cid;

INSERT INTO Query3 (SELECT * FROM RESULT);


DROP VIEW Landlocked CASCADE;
DROP VIEW neighborList CASCADE;
DROP VIEW one CASCADE;
DROP VIEW findC2id CASCADE;
DROP VIEW RESULT CASCADE;

--  4 statements

CREATE VIEW Landlocked as
	select  cid, cname
	from country
	where cid not in 
	(select cid From oceanAccess);

CREATE VIEW Direct as
	select cname, oname, cid
	From country natural join (oceanAccess natural join ocean) as foo
	ORDER BY cname ASC, oname DESC;

CREATE VIEW IndirectCID as
	select cid, neighbor
	From Landlocked inner join neighbour on Landlocked.cid = neighbour.country;

CREATE VIEW IndirectAccess as
	select cname, oname
	From IndirectCID inner join Direct on IndirectCID.neighbor = Direct.cid;

CREATE VIEW RESULT as
	select cname, oname
	From (select cname, oname from Direct union all select * from IndirectAccess) as foo
	ORDER BY cname ASC, oname DESC;


INSERT INTO Query4 (SELECT * FROM RESULT);


DROP VIEW Landlocked CASCADE;
DROP VIEW Direct CASCADE;
DROP VIEW IndirectCID CASCADE;
DROP VIEW IndirectAccess CASCADE;
DROP VIEW RESULT CASCADE;






/*
CREATE VIEW v1 AS
SELECT oceanAccess.cid as cid, oceanaccess.oid as coid, neighbour.neighbor as nid
	FROM oceanAccess INNER JOIN  neighbour
		ON oceanAccess.cid = neighbour.country;

CREATE VIEW v2 AS
	SELECT v1.cid, v1.coid, v1.nid, oceanAccess.oid as noid
	FROM v1 INNER JOIN oceanAccess
		ON v1.nid = oceanAccess.cid;

CREATE VIEW va AS
	SELECT cid, coid from v2;

CREATE VIEW vb AS
	SELECT cid, noid as coid from v2;

CREATE VIEW semiResult AS
SELECT * from (select * from va) as foo union (select * from vb);

CREATE VIEW qResult AS
select cname, coid from semiResult NATURAL JOIN country;

CREATE VIEW Result AS
SELECT cname, oname from qResult INNER JOIN ocean ON coid = oid order by cname ASC, oname DESC;

INSERT INTO Query4 (Select * from Result);

DROP VIEW v1 cascade;

*/
-- Query 5 statements

create view RESULT as
	SELECT cid, cname, AVG(hdi_score) as avghdi
	FROM country NATURAL JOIN (
		select cid, hdi_score
		from hdi
		where year >= 2009 and year <= 2013) as foo
	GROUP BY cid
	ORDER BY AVG(hdi_score) DESC
	Limit 10;

INSERT INTO Query5 (SELECT * FROM Result);

DROP VIEW Result cascade;


-- Query 6 statements

CREATE VIEW hdi1 AS
	SELECT * FROM hdi WHERE year = '2009';

CREATE VIEW hdi2 AS
	SELECT * FROM hdi WHERE year  = '2010';

CREATE VIEW hdi3 AS
	SELECT * FROM hdi WHERE year = '2011';

CREATE VIEW hdi4 AS
	SELECT * FROM hdi WHERE year = '2012';

CREATE VIEW hdi5 AS
	SELECT * FROM hdi WHERE year = '2013';

CREATE VIEW combined AS

	SELECT hdi1.cid as cid, hdi1.hdi_score as scoreOne, hdi2.hdi_score as scoreTwo, hdi3.hdi_score as scoreThree, hdi4.hdi_score as scoreFour, hdi5.hdi_score as scoreFive FROM hdi1 
	INNER JOIN hdi2 
		ON hdi1.cid = hdi2.cid
	INNER JOIN hdi3
		ON hdi1.cid = hdi3.cid
	INNER JOIN hdi4
		ON hdi1.cid = hdi4.cid
	INNER JOIN hdi5
		ON hdi1.cid = hdi5.cid;

CREATE VIEW Result AS
SELECT cid, cname FROM country NATURAL JOIN
	(SELECT cid
	FROM combined
	WHERE scoreOne < scoreTwo
		and scoreTwo < scoreThree
		and scoreThree < scoreFour
		and scoreFour < scoreFive) as foo
	ORDER BY cname ASC;

INSERT INTO Query6 (SELECT * FROM Result);

DROP VIEW hdi1 CASCADE;
DROP VIEW hdi2 CASCADE;
DROP VIEW hdi3 CASCADE;
DROP VIEW hdi4 CASCADE;
DROP VIEW hdi5 CASCADE;

-- Query 7 statements
CREATE VIEW RESULT AS
	select rid, rname, ceil(population * rpercentage / 100) as followers
	from religion natural join country
	order by ceil(population * rpercentage / 100) DESC;

INSERT INTO Query7 (SELECT * FROM Result);

DROP VIEW RESULT cascade;

-- Query 8 statements

CREATE VIEW f1 AS
	SELECT cid, max(lpercentage) as maxP
	FROM language
	GROUP BY cid;

CREATE VIEW co AS
	SELECT * FROM f1 
		NATURAL JOIN language 
	WHERE maxP = lpercentage;

CREATE VIEW f2 AS
	SELECT coO.cid as cid, coO.lname as clname, coT.cid as nid, coT.lname as nlname 
	FROM co as coO, co as coT
	WHERE coO.lname = coT.lname
		and coO.cid != coT.cid;

CREATE VIEW combined AS
SELECT cid, nid, clname as lname
FROM f2 INNER JOIN neighbour
	ON f2.cid = neighbour.country
WHERE f2.cid = neighbour.country and f2.nid = neighbour.neighbor;

CREATE VIEW Result AS
SELECT cOne.cname as c1name, cTwo.cname as c2name, lname
FROM combined 
	INNER JOIN country as cOne
		ON combined.cid = cOne.cid
	INNER JOIN country as cTwo
		ON combined.nid = cTwo.cid
ORDER BY lname ASC, c1name DESC;

INSERT INTO Query8 (SELECT * FROM Result);

DROP VIEW f1 cascade;

-- Query 9 statements
CREATE VIEW cheight AS
SELECT cid, cname, height FROM country;

CREATE VIEW oceanDepth AS
SELECT cid, max(depth) as depth
FROM (SELECT * from oceanaccess NATURAL JOIN ocean) as foo
GROUP BY cid;

CREATE VIEW combined AS
SELECT cheight.cid, cname, height, depth
FROM oceanDepth RIGHT JOIN cheight ON oceanDepth.cid = cheight.cid;

CREATE VIEW sumed AS
SELECT cname, combined.depth + combined.height as totalspan from combined;

CREATE VIEW Result AS
SELECT cname, max(totalspan) as totalspan
FROM
	(SELECT * FROM sumed
	UNION ALL
	SELECT cname, height as totalspan FROM cheight) as foo
GROUP BY cname;

INSERT INTO Query9 (SELECT * FROM Result);

DROP VIEW cheight CASCADE;
DROP VIEW oceanDepth CASCADE;

-- Query 10 statements
CREATE VIEW Result AS
	SELECT cname, tlength as borderslength
	FROM country NATURAL JOIN
		(SELECT country as cid, SUM(length) as tlength
		FROM neighbour
		GROUP BY country) as foo
	ORDER BY borderslength DESC
	LIMIT 1;

INSERT INTO Query10 (SELECT * FROM Result);

DROP VIEW Result;