-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

SELECT N.country AS c1id, C.cname AS c1name, N.neighbor AS c2id, C.cname AS c2name
FROM neighbour N, country C 
WHERE N.height >= ALL(SELECT N.height FROM neighbour N)
ORDER BY c1name ASC; 

-- Query 2 statements

SELECT C.cid, C.cname
FROM country C
ORDER BY C.cname ASC 

-- Query 3 statements

SELECT N.country AS c1id, C.cname AS c1name, N.neighbor AS c2id, C.cname AS c2name
FROM neighbour N, country C 
WHERE C.height >= ALL(SELECT C.height FROM country C)
ORDER BY c1name ASC;


-- Query 4 statements

SELECT C.cname, O.oname 
FROM country C, oceanAccess OA, ocean O
WHERE C.cid = OA.cid and O.oid = OA.oid
ORDER BY cname ASC, oname DESC  


-- Query 5 statements

SELECT C.cid, C.cname, avg(H.hdi_scorce) AS avghdi 
FROM country C, hid H
WHERE avg(H.hdi_scorce) >= ALL(SELECT avg(H.hdi_scorce) FROM hid H) and H.year = 2009-2013
GROUP BY C.cid
ORDER BY avghdi LIMIT 10

-- Query 6 statements

SELECT C.cid, C.cname
FROM country C, hid H
WHERE H.hdi_scorce++ and H.year = 2009-2013
GROUP BY C.cid
ORDER BY cname ASC

-- Query 7 statements

SELECT R.rid, R.rname, sum((R.rpercentage / 100) * C.population) AS followers
FROM religion R, country C
GROUP BY R.rid
ORDER BY followers DESC;


-- Query 8 statements

SELECT C.cname AS c1name FROM neighbour N and country C WHERE N.country = C.cid, C.cname AS c2name FROM neighbour N and country C WHERE N.neighbor = C.id, L.lname
FROM neighbour N, country C, language L 
WHERE c1name.cid = L.cid = c2name.cid
ORDER BY lname ASC, c1name DESC; 


-- Query 9 statements

SELECT C.cname, sum(C.height >= ALL(SELECT C.height FROM country C) - O.depth >= ALL((SELECT O.depth FROM ocean O)) AS totalspan  
FROM country C, ocean O


-- Query 10 statements

SELECT N.country, sum(N.length >= ALL(SELECT N.length FROM neighbour N) AS borderslength  
FROM neighbour N
