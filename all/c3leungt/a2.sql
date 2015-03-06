-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW countrylist AS SELECT cid AS c1id, cname AS c1name FROM country;
CREATE VIEW countryneighbours AS SELECT countrylist.c1id AS c1id, countrylist.c1name AS c1name, neighbour.neighbor AS c2id FROM countrylist JOIN neighbour ON countrylist.c1id=neighbour.country;
INSERT INTO Query1 (SELECT c1id, c1name, c2id, c2name FROM(SELECT c1id, c1name, c2id, country.cname AS c2name, MAX(country.height) FROM countryneighbours JOIN country ON countryneighbours.c2id=country.cid GROUP BY c1id, c1name, c2id, c2name ORDER BY c1name ASC) as highestneighbour);


-- Query 2 statements
INSERT INTO Query2 (SELECT countryoceans.cid AS cid, countryoceans.cname AS cname FROM (SELECT oceanAccess.cid AS cid, oceanAccess.oid AS oid, country.cname AS cname FROM oceanAccess LEFT JOIN country ON oceanAccess.cid=country.cid) AS countryoceans WHERE oid IS NULL ORDER BY cname ASC);

-- Query 3 statements
CREATE VIEW numneighbours AS SELECT Query2.cid AS cid, COUNT(neighbour.neighbor) AS numofneighbours FROM (Query2 JOIN neighbour ON Query2.cid=neighbour.country) GROUP BY cid;
CREATE VIEW numneighbours1 AS SELECT numneighbours.cid AS cid FROM numneighbours WHERE numofneighbours=1;
CREATE VIEW singleneighbour AS SELECT numneighbours1.cid AS c1id, neighbour.neighbor AS c2id FROM numneighbours1 JOIN neighbour ON numneighbours1.cid=neighbour.country;
CREATE VIEW singleneighbour1 AS SELECT singleneighbour.c1id AS c1id, country.cname AS c1name, singleneighbour.c2id AS c2id FROM (singleneighbour JOIN country ON singleneighbour.c1id=country.cid);
INSERT INTO Query3 (SELECT singleneighbour1.c1id AS c1id, singleneighbour1.c1name AS c1name, singleneighbour1.c2id AS c2id, country.cname AS c2name FROM singleneighbour1 JOIN country ON singleneighbour1.c2id=country.cid ORDER BY c1name ASC);


-- Query 4 statements
CREATE VIEW oceanicaccess AS SELECT oceanAccess.cid AS cid, ocean.oname AS oname FROM oceanAccess JOIN ocean ON oceanAccess.oid=ocean.oid;
CREATE VIEW oceanicaccess2 AS SELECT oceanicaccess.cid AS cid, oceanicaccess.oname AS oname, neighbour.neighbor AS nid FROM (oceanicaccess JOIN neighbour ON oceanicaccess.cid=neighbour.country);
CREATE VIEW oceanicaccess3 AS SELECT oceanicaccess2.cid AS cid, oceanicaccess2.oname AS oname FROM oceanicaccess2;
CREATE VIEW oceanicaccess4 AS SELECT oceanicaccess2.nid AS cid, oceanicaccess2.oname AS oname FROM oceanicaccess2;
CREATE TRIGGER oceaninsert INSTEAD OF INSERT ON oceanicaccess3 BEGIN IF (NOT EXISTS (SELECT cid FROM oceanicaccess, inserted I WHERE oceanicaccess.cid=I.cid)) INSERT INTO oceanicaccess3 (SELECT * FROM oceanicaccess4) END;
INSERT INTO Query4(SELECT country.cname AS cname, oceanicaccess3.oname AS oname FROM (oceanicaccess3 JOIN country ON oceanicaccess3.cid=country.cid) ORDER BY cname ASC, oname DESC);


-- Query 5 statements
CREATE VIEW hdipastfive AS SELECT hdi.cid AS cid, hdi.year AS year, hdi.hdi_score AS hdi_score FROM hdi WHERE hdi.year=2009 OR hdi.year=2010 OR hdi.year=2011 OR hdi.year=2012 OR hdi.year=2013;
CREATE VIEW hdihighest10 AS SELECT hdipastfive.cid AS cid, AVG(hdipastfive.hdi_score) AS avghdi FROM hdipastfive GROUP BY hdipastfive.cid LIMIT 10;
INSERT INTO Query5 (SELECT hdihighest10.cid AS cid, country.cname AS cname, hdihighest10.avghdi AS avghdi FROM (hdihighest10 JOIN country ON hdihighest10.cid=country.cid) ORDER BY avghdi DESC);



-- Query 6 statements
CREATE VIEW hdi0910 AS SELECT h1.cid AS cid, h2.hdi_score AS score10 FROM hdi h1, hdi h2 WHERE (h1.cid=h2.cid AND h1.year=2009 AND h2.year=2010 AND h1.hdi_score < h2.hdi_score);
CREATE VIEW hdi0911 AS SELECT h1.cid AS cid, h2.hdi_score AS score11 FROM hdi0910 h1, hdi h2 WHERE (h1.cid=h2.cid AND h2.year=2011 AND h1.score10 < h2.hdi_score);
CREATE VIEW hdi0912 AS SELECT h1.cid AS cid, h2.hdi_score AS score12 FROM hdi0911 h1, hdi h2 WHERE (h1.cid=h2.cid AND h2.year=2012 AND h1.score11 < h2.hdi_score);
CREATE VIEW hdi0913 AS SELECT h1.cid AS cid FROM hdi0912 h1, hdi h2 WHERE (h1.cid=h2.cid AND h2.year=2013 AND h1.score12 < h2.hdi_score);
INSERT INTO Query6 (SELECT hdi0913.cid AS cid, country.cname AS cname FROM hdi0913 JOIN country ON hdi0913.cid=country.cid ORDER BY cname ASC);

-- Query 7 statements
CREATE VIEW religionfollowers AS SELECT religion.rid AS rid, religion.rname AS rname, religion.cid AS cid, religion.rpercentage * country.population AS countryfollowers FROM religion JOIN country ON religion.cid=country.cid;
INSERT INTO Query7 (SELECT religionfollowers.rid AS rid, religionfollowers.rname AS rname, SUM(countryfollowers) AS followers FROM religionfollowers GROUP BY rid, rname ORDER BY followers DESC);


-- Query 8 statements
CREATE VIEW pl AS SELECT language.lid AS lid, language.lname AS lname, language.cid AS cid, MAX(language.lpercentage) FROM language GROUP BY lid, lname, cid;
CREATE VIEW pl2 AS SELECT pl.lid AS lid, pl.lname AS lname, pl.cid AS cid, c.cname AS cname FROM pl JOIN country c ON pl.cid=c.cid;
CREATE VIEW pl3 AS SELECT neighbour.country AS cid, neighbour.neighbor AS nid, pl2.lid AS lid, pl2.lname AS lname, pl2.cname AS cname FROM neighbour JOIN pl2 ON neighbour.country=pl2.cid;
INSERT INTO Query8 (SELECT pl3.cname AS c1name, pl2.cname AS c2name, pl3.lname AS lname FROM pl3 JOIN pl2 ON pl3.nid=pl2.cid WHERE pl3.lid=pl2.lid ORDER BY lname ASC, c1name DESC);

-- Query 9 statements



-- Query 10 statements


