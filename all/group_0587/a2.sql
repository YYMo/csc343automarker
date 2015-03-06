-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

SET SEARCH_PATH TO A2;

-- Query 1 statements

DELETE FROM Query1;
CREATE OR REPLACE VIEW neig AS (select country , neighbor AS cid, length from neighbour);
CREATE OR REPLACE VIEW combine AS (select * from neig NATURAL JOIN country);

CREATE OR REPLACE VIEW temp AS ( 
select N.country AS country, N.cid AS neighbor from neig N where N.cid IN (select cid from combine where height IN (select max(height) from combine where N.country = combine.country))
UNION
SELECT a.cid as country, NULL AS neighbor FROM country a LEFT OUTER JOIN neighbour b ON a.cid=b.country
WHERE b.neighbor IS NULL
);

INSERT INTO Query1 (
SELECT T.country AS c1id, C1.cname AS c1name, T.neighbor AS c2id, C2.cname AS c2name 
FROM temp T LEFT OUTER JOIN country C1 ON (T.country=C1.cid) LEFT OUTER JOIN country C2 ON (T.neighbor=C2.cid )
ORDER BY c1name ); 


DROP VIEW temp;
DROP VIEW combine;
DROP VIEW neig;


-- Query 2 statements
DELETE FROM Query2;
INSERT INTO Query2 (
SELECT cid, cname 
FROM country c 
WHERE c.cid NOT IN(SELECT cid FROM oceanAccess) 
ORDER BY cname); 


-- Query 3 statements
DELETE FROM Query3;
CREATE OR REPLACE VIEW SUCCESSFUL_COUNTRY AS (
SELECT cid as country from country c where c.cid NOT IN(SELECT cid from oceanAccess)
INTERSECT
SELECT country FROM neighbour GROUP BY country HAVING count(neighbor) =1);



CREATE OR REPLACE VIEW RESULT_COUNTRY AS (SELECT country, neighbor FROM neighbour WHERE country IN (SELECT * FROM SUCCESSFUL_COUNTRY));

INSERT INTO Query3 (SELECT T.country AS c1id, C1.cname AS c1name, T.neighbor AS c2id, C2.cname AS c2name from RESULT_COUNTRY T, country C1, country C2 where T.country=C1.cid and T.neighbor=C2.cid order by c1name ); 


DROP VIEW RESULT_COUNTRY;
DROP VIEW SUCCESSFUL_COUNTRY;


-- Query 4 statements
DELETE FROM Query4;

CREATE OR REPLACE VIEW SUCCESSFUL_COUNTRY AS (
SELECT DISTINCT a.cid AS country, b.oname FROM oceanaccess a, ocean b WHERE a.oid=b.oid
UNION
SELECT a.country, b.oname FROM neighbour a, ocean b, oceanaccess c WHERE c.oid=b.oid AND a.neighbor=c.cid);

INSERT INTO Query4 (SELECT cname AS cname, a.oname FROM SUCCESSFUL_COUNTRY a, country b WHERE a.country = b.cid ORDER BY cname, oname DESC); 


DROP VIEW SUCCESSFUL_COUNTRY;


-- Query 5 statements
DELETE FROM Query5;
CREATE OR REPLACE VIEW AVG_HDI_SCORE AS (
SELECT cid, avg(hdi_score) as avg_hdi_score 
FROM hdi
WHERE year BETWEEN 2009 AND 2013
GROUP BY cid
);


INSERT INTO Query5 (SELECT a.cid, b.cname, a.avg_hdi_score 
FROM AVG_HDI_SCORE a, country b 
WHERE a.cid=b.cid
ORDER BY a.avg_hdi_score DESC LIMIT 10);

DROP VIEW AVG_HDI_SCORE;



-- Query 6 statements
DELETE FROM Query6;
CREATE OR REPLACE VIEW temp AS (select cid, year, hdi_score from hdi where year>=2009 and year <=2013 group by cid, year, hdi_score order by year);
CREATE OR REPLACE VIEW temp2 AS (select T1.cid AS cid from temp T1 Join temp T2 ON T1.cid=T2.cid and T1.year-T2.year=1 and T1.hdi_score-T2.hdi_score<=0);
CREATE OR REPLACE VIEW temp3 AS (
select distinct cid from temp
EXCEPT 
select distinct cid from temp2
);
 
INSERT INTO Query6 (SELECT cid, cname  from temp3 T NATURAL JOIN country C ORDER BY C.cname);  

DROP VIEW temp3;
DROP VIEW temp2;
DROP VIEW temp;


-- Query 7 statements
DELETE FROM Query7;
CREATE OR REPLACE VIEW GET_RAMOUNT AS(
SELECT a.*, (a.rpercentage* b.population) as amount
FROM religion a, country b
WHERE a.cid=b.cid);

INSERT INTO Query7 (SELECT rid, rname, SUM(amount) as followers FROM GET_RAMOUNT GROUP BY rid,rname ORDER BY SUM(amount) DESC);

DROP VIEW GET_RAMOUNT;



-- Query 8 statements
DELETE FROM Query8;
CREATE OR REPLACE VIEW MOST_POPULAR_L AS(
SELECT a.cid as cid, a.lname
FROM language a, (SELECT cid,MAX(lpercentage) AS POPULAR_L FROM language GROUP BY cid) b
WHERE a.cid = b.cid
AND   a.lpercentage = b.POPULAR_L);

CREATE OR REPLACE VIEW RESULT AS(
SELECT a.cid AS cid1, b.cid AS cid2, a.lname FROM MOST_POPULAR_L a, MOST_POPULAR_L b 
WHERE a.lname = b.lname
AND   a.cid != b.cid);

INSERT INTO Query8 (
SELECT C1.cname AS c1name, C2.cname AS c2name, T.lname as lname 
FROM RESULT T, country C1, country C2 
WHERE T.cid1=C1.cid 
AND T.cid2=C2.cid 
ORDER BY lname, c1name DESC);


DROP VIEW RESULT;
DROP VIEW MOST_POPULAR_L;



-- Query 9 statements
DELETE FROM Query9;

CREATE OR REPLACE VIEW CALCULATION AS (
SELECT R.cname, (R.height+R.depth) AS totalspan FROM
(
SELECT a.cname, a.height, 
CASE WHEN c.depth IS NULL THEN 0 
ELSE c.depth 
END 
FROM country a LEFT OUTER JOIN oceanaccess b ON a.cid=b.cid 
LEFT OUTER JOIN ocean c ON c.oid=b.oid
) R);

 
INSERT INTO Query9 (SELECT * from CALCULATION WHERE totalspan = (SELECT MAX(totalspan) FROM CALCULATION));  


DROP VIEW CALCULATION;








-- Query 10 statements
DELETE FROM Query10;
CREATE OR REPLACE VIEW TOT_B AS (SELECT country, sum(length) as TOT_LENGTH from neighbour group by country);


INSERT INTO Query10 (SELECT b.cname, a.tot_length as bordeslength FROM TOT_B a, country b WHERE a.tot_length = (SELECT MAX(tot_length) FROM TOT_B) AND a.country=b.cid);

DROP VIEW TOT_B;


