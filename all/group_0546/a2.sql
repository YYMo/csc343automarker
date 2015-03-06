-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW HighestN (cid, maxhighet) AS
SELECT
neighbour.country, max(country.height)
FROM country JOIN neighbour on (country.cid = neighbour.neighbor)
GROUP BY neighbour.country;

INSERT INTO Query1 (SELECT DISTINCT C1.cid as c1id , C1.cname as c1name , C2.cid as c2id, C2.cname as c2name
FROM country C1, country C2, HighestN, neighbour N
WHERE C1.cid = HighestN.cid AND HighestN.Maxhighet = C2.height AND N.country = HighestN.cid AND N.neighbor = C2.cid
ORDER BY C1.cname ASC);

DROP VIEW HighestN CASCADE;


-- Query 2 statements

INSERT INTO Query2 (SELECT country.cid, country.cname
FROM country
WHERE country.cid NOT IN (SELECT oceanAccess.cid FROM oceanAccess)
ORDER BY cname ASC);



-- Query 3 statements

CREATE VIEW landlocked (cid, cname) AS
SELECT country.cid, country.cname
FROM country
WHERE country.cid NOT IN (SELECT oceanAccess.cid FROM oceanAccess);

CREATE VIEW oneNeighbour(cid1, cid2) AS
SELECT neighbour.country cid1, neighbour.neighbor cid2
FROM neighbour
WHERE neighbour.country NOT IN (
SELECT N1.country
FROM neighbour N1, neighbour N2
WHERE N1.country = N2.country AND N1.neighbor > N2.neighbor);

INSERT INTO Query3 (SELECT landlocked.cid as c1id, landlocked.cname as c1name, country.cid as c2id, country.cname as c2name
FROM landlocked, oneNeighbour, country
WHERE landlocked.cid = oneNeighbour.cid1 AND oneNeighbour.cid2 = country.cid 
ORDER BY landlocked.cname);

DROP VIEW landlocked CASCADE;
DROP VIEW oneNeighbour CASCADE;


 
-- Query 4 statements

INSERT INTO Query4 (SELECT country.cname, ocean.oname

FROM
oceanAccess

JOIN ocean ON (oceanAccess.oid = ocean.oid)

JOIN neighbour ON (oceanAccess.cid = neighbour.country)

JOIN country ON (country.cid = oceanAccess.cid)

UNION

SELECT country.cname, ocean.oname

FROM
oceanAccess

JOIN ocean ON (oceanAccess.oid = ocean.oid)

JOIN neighbour ON (oceanAccess.cid = neighbour.country)

JOIN country ON (country.cid = neighbour.neighbor));


-- Query 5 statements

CREATE VIEW five_hdi (cid, year, hdi_score) AS
SELECT hdi.cid, hdi.year, hdi_score
FROM hdi
WHERE hdi.year BETWEEN 2009 AND 2013;

INSERT INTO Query5 (SELECT country.cid, country.cname, AVG(five_hdi.hdi_score) AS avghdi
FROM five_hdi JOIN country ON (five_hdi.cid = country.cid)
GROUP BY country.cid ORDER BY AVG(five_hdi.hdi_score) DESC LIMIT 10);


-- Query 6 statements

INSERT INTO Query6 (SELECT country.cid, country.cname
FROM country, five_hdi F1, five_hdi F2
WHERE country.cid = F1.cid AND F1.cid = F2.cid AND F1.year > F2.year AND F1.hdi_score > F2.hdi_score
ORDER BY country.cname ASC);

DROP VIEW five_hdi CASCADE;


-- Query 7 statements

INSERT INTO Query7( 
SELECT religion.rid, religion.rname, sum(religion.rpercentage * country.population) AS followers
FROM country JOIN religion ON (country.cid = religion.cid)
GROUP BY religion.rid, religion.rname
ORDER BY followers DESC);

-- Query 8 statements

CREATE VIEW Popular (c1name, c2name, lname, numspeakers) as
SELECT DISTINCT 
       country.cname AS c1name,
       nInfo.cname AS c2name, 
       language.lname AS lname, 
       (lpercentage * country.population) AS numspeakers

FROM language

JOIN country ON (country.cid = language.cid) 

JOIN neighbour ON (country.cid = neighbour.country)

-- join neighbour to country table to retrieve neighbour cname
JOIN country nInfo ON (nInfo.cid = neighbour.neighbor AND neighbour.neighbor IN (SELECT cid FROM language));

CREATE VIEW Max (c1name, numspeakers) as
SELECT Popular.c1name, max(numspeakers)
FROM Popular
GROUP BY Popular.c1name;

INSERT INTO Query8 (SELECT p2.c1name, p2.c2name, p2.lname
FROM Popular p1, Popular p2, Max
WHERE p1.c1name = Max.c1name AND p1.c1name = p2.c1name AND p1.lname = p2.lname AND p1.c2name = p2.c2name AND p2.numspeakers = Max.numspeakers
ORDER BY p1.lname ASC, p1.c1name DESC);

DROP VIEW Max CASCADE;
DROP VIEW Popular CASCADE;

 
-- Query 9 statements

CREATE VIEW Span (cid, totalspan) AS

SELECT country.cid, abs(country.height - max(ocean.depth)) AS totalspan
FROM country, oceanAccess, ocean
WHERE country.cid = oceanAccess.cid AND oceanAccess.oid = ocean.oid
GROUP BY country.cid

UNION

SELECT country.cid, country.height AS totalspan
FROM country
WHERE cid NOT IN (SELECT cid FROM oceanAccess);

INSERT INTO Query9 (SELECT country.cname, Span.totalspan
FROM Span JOIN country ON (Span.cid = country.cid AND Span.totalspan = (SELECT max(totalspan) FROM Span)));

DROP VIEW Span CASCADE;


-- Query 10 statements

INSERT INTO Query10 (
SELECT country.cname,  sum(neighbour.length) AS borderslength
FROM country, neighbour
WHERE country.cid = neighbour.country
GROUP BY country.cname
ORDER BY borderslength DESC LIMIT 1);