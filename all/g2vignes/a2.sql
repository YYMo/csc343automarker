-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

SELECT CO.cid, CO.cname, CO.height, NB.country, NB.neighbor, (SELECT CO.height FROM country CO CROSS JOIN neighbor NB WHERE NB.neighbor=CO.cid) as neighborHeight INTO Temp1 FROM country CO JOIN neighbor NB ON CO.cid=NB.country; 

SELECT country as c1id, cname as c1name, neighbor as c2id, (SELECT CO.cname FROM country CO CROSS JOIN neighbor NB WHERE NB.neighbor=CO.cid) as c2name, MAX(height) as elevation INTO Temp2 FROM Temp1 GROUP BY c1id ORDER BY c1name ASC;

INSERT INTO QUERY1(SELECT c1id, c1name, c2id, c2name FROM Temp2); 

DROP TABLE IF EXISTS Temp1;
DROP TABLE IF EXISTS Temp2;

-- Query 2 statements

INSERT INTO QUERY2(SELECT CO.cid, CO.cname FROM country CO JOIN oceanAccess OA ON OA.cid=CO.cid); 

-- Query 3 statements

SELECT CO.cid, CO.cname INTO LL FROM country CO JOIN oceanAccess OA ON OA.cid=CO.cid;  

SELECT LL.cid as c1id, LL.cname as c1name, NB.neighbor as c2id, (SELECT DISTINCT CO.cname FROM neighbor NB CROSS JOIN country CO WHERE NB.neighbor=CO.cid) as c2name INTO Temp2 FROM LL JOIN neighbor NB ON LL.cid=NB.country;

SELECT c1id, c1name, c2id, c2name, COUNT(neighbor) as numofneighbors INTO Temp3 FROM Temp2 GROUP BY c1id HAVING COUNT(neighbor)=1;

INSERT INTO QUERY3(SELECT c1id, c1name, c2id, c2name FROM Temp3);

DROP TABLE IF EXISTS LL;
DROP TABLE IF EXISTS Temp2;
DROP TABLE IF EXISTS Temp3;

-- Query 4 statements



-- Query 5 statements



-- Query 6 statements



-- Query 7 statements



-- Query 8 statements



-- Query 9 statements



-- Query 10 statements


