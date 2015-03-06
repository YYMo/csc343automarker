-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW country_neighbour AS 
SELECT c.cid, c.cname, n.neighbor FROM  country c, neighbour n 
WHERE c.cid = n.country;

CREATE VIEW country_neighbour_height AS
SELECT n.cid AS cid, n.cname AS cname, n.neighbor AS nid, c.cname AS nname, c.height AS nheight 
FROM country_neighbour n, country c WHERE n.neighbor = c.cid;

CREATE VIEW max_height AS 
SELECT n.country, max(c.height) AS max_height 
FROM neighbour n, country c 
WHERE n.neighbor = c.cid 
GROUP BY n.country;

INSERT INTO Query1
SELECT c.cid AS c1id, c.cname AS c1name, c.nid AS c2id, c.nname AS c2name 
FROM country_neighbour_height c, max_height m 
WHERE c.cid = m.country AND c.nheight = m.max_height 
ORDER BY c1name ASC;

DROP VIEW max_height;
DROP VIEW country_neighbour_height;
DROP VIEW country_neighbour;


-- Query 2 statements
INSERT INTO Query2
SELECT country.cid, country.cname FROM country, oceanAccess WHERE country.cid != oceanAccess.cid GROUP BY 1,2 ORDER BY country.cname ASC;

-- Query 3 statements
CREATE VIEW country_neighbour_id AS 
SELECT c.cid, c.cname , n.neighbor 
FROM  country c, neighbour n 
WHERE c.cid = n.country;

CREATE VIEW one_neighbour_country AS
SELECT cid FROM country_neighbour_id GROUP BY cid HAVING count(neighbor) = 1;

INSERT INTO Query3
SELECT c1.cid AS c1id, c1.cname AS c1name, c1.neighbor AS c2id, c.cname AS c2name 
FROM country_neighbour_id c1, one_neighbour_country c2, country c 
WHERE c1.cid = c2.cid AND c.cid = c1.neighbor;

DROP VIEW one_neighbour_country;
DROP VIEW country_neighbour_id;


-- Query 4 statements
CREATE VIEW CID_OID AS
(SELECT neighbour.country AS COUNTRY_NAME, oceanAccess.oid AS OCEAN_NAME FROM neighbour, oceanAccess WHERE neighbour.neighbor = oceanAccess.cid)
UNION
(SELECT cid AS COUNTRY_NAME, oid AS OCEAN_NAME FROM oceanAccess);

INSERT INTO Query4
SELECT cname, oname FROM country, ocean, CID_OID WHERE country.cid = CID_OID.COUNTRY_NAME AND ocean.oid = CID_OID.OCEAN_NAME GROUP BY 1, 2 ORDER BY cname ASC, oname DESC;

DROP VIEW CID_OID;

-- Query 5 statements
CREATE VIEW top_hdi_country AS
SELECT cid, AVG(hdi_score) AS avghdi 
FROM hdi 
WHERE year >=2009 AND year <= 2013 
GROUP BY cid 
ORDER BY  avghdi DESC LIMIT 10;

INSERT INTO Query5
SELECT t.cid AS cid, c.cname AS cname, t.avghdi AS avghdi 
FROM top_hdi_country t, country c 
WHERE t.cid = c.cid;

DROP VIEW top_hdi_country;


-- Query 6 statements
-- CREATE VIEW ALL_YEAR AS
-- SELECT * FROM hdi WHERE year=2013 AND year=2012 AND year=2011 AND year=2010 AND year=2009;
CREATE VIEW THIRTEEN_INDEX AS
SELECT cid, hdi_score FROM hdi WHERE year=2013;
CREATE VIEW TWELVE_INDEX AS
SELECT cid, hdi_score FROM hdi WHERE year=2012;
CREATE VIEW ELEVAN_INDEX AS
SELECT cid, hdi_score FROM hdi WHERE year=2011;
CREATE VIEW TEN_INDEX AS
SELECT cid, hdi_score FROM hdi WHERE year=2010;
CREATE VIEW NINE_INDEX AS
SELECT cid, hdi_score FROM hdi WHERE year=2009;
CREATE VIEW GROWING AS
SELECT THIRTEEN_INDEX.cid FROM THIRTEEN_INDEX, TWELVE_INDEX, ELEVAN_INDEX, TEN_INDEX, NINE_INDEX WHERE
THIRTEEN_INDEX.cid = TWELVE_INDEX.cid AND
TWELVE_INDEX.cid = ELEVAN_INDEX.cid AND
ELEVAN_INDEX.cid = TEN_INDEX.cid AND
TEN_INDEX.cid = NINE_INDEX.cid AND
THIRTEEN_INDEX.hdi_score > TWELVE_INDEX.hdi_score AND
TWELVE_INDEX.hdi_score > ELEVAN_INDEX.hdi_score AND
ELEVAN_INDEX.hdi_score > TEN_INDEX.hdi_score AND
TEN_INDEX.hdi_score > NINE_INDEX.hdi_score;

INSERT INTO Query6 (SELECT country.cid, country.cname FROM country, GROWING WHERE country.cid = GROWING.cid GROUP BY 1,2 ORDER BY country.cname ASC);

DROP VIEW GROWING;
DROP VIEW NINE_INDEX;
DROP VIEW TEN_INDEX;
DROP VIEW ELEVAN_INDEX;
DROP VIEW TWELVE_INDEX;
DROP VIEW THIRTEEN_INDEX;


-- Query 7 statements
INSERT INTO Query7
SELECT r.rid AS rid, r.rname AS rname, SUM(r.rpercentage*c.population) AS followers 
FROM religion r, country c 
WHERE r.cid = c.cid 
GROUP BY r.rid, r.rname ORDER BY followers DESC;

-- Query 8 statements
CREATE VIEW COUNTRY_POPULAR AS
SELECT cid, MAX(lpercentage) AS popular FROM language GROUP BY cid;
CREATE VIEW COUNTRY_POPULAR_LANGUAGE AS
SELECT COUNTRY_POPULAR.cid, language.lname AS popular_language, popular FROM COUNTRY_POPULAR, language WHERE COUNTRY_POPULAR.cid = language.cid AND COUNTRY_POPULAR.popular = language.lpercentage;
CREATE VIEW NEIGHBOUR_POPULAR_LANGUAGE AS
SELECT * FROM COUNTRY_POPULAR_LANGUAGE;
CREATE VIEW COUNTRY_NEIGHBOUR_ID AS
SELECT country, neighbor, COUNTRY_POPULAR_LANGUAGE.popular_language FROM neighbour, COUNTRY_POPULAR_LANGUAGE, NEIGHBOUR_POPULAR_LANGUAGE WHERE neighbour.country = COUNTRY_POPULAR_LANGUAGE.cid AND neighbour.neighbor = NEIGHBOUR_POPULAR_LANGUAGE.cid AND COUNTRY_POPULAR_LANGUAGE.popular_language = NEIGHBOUR_POPULAR_LANGUAGE.popular_language; 
CREATE VIEW COUNTRY_NAME AS
SELECT country, neighbor, COUNTRY_NEIGHBOUR_ID.popular_language, country.cname AS country_name FROM country JOIN COUNTRY_NEIGHBOUR_ID ON country.cid = COUNTRY_NEIGHBOUR_ID.country;
CREATE VIEW COUNTRY_NEIGHBOUR_NAME AS
SELECT country, neighbor, COUNTRY_NAME.popular_language, country_name, country.cname AS neighbour_name FROM country JOIN COUNTRY_NAME on country.cid = COUNTRY_NAME.neighbor;

INSERT INTO Query8
SELECT country_name AS c1name, neighbour_name AS c2name, popular_language AS lname FROM COUNTRY_NEIGHBOUR_NAME ORDER BY lname ASC, c1name DESC;

DROP VIEW COUNTRY_NEIGHBOUR_NAME;
DROP VIEW COUNTRY_NAME;
DROP VIEW COUNTRY_NEIGHBOUR_ID;
DROP VIEW NEIGHBOUR_POPULAR_LANGUAGE;
DROP VIEW COUNTRY_POPULAR_LANGUAGE;
DROP VIEW COUNTRY_POPULAR;

-- Query 9 statements
CREATE VIEW country_depth AS
SELECT c.cname AS cname, c.height AS height,
CASE WHEN o.depth IS NULL THEN 0 ELSE o.depth END AS depth
FROM country c LEFT JOIN oceanAccess oa ON c.cid = oa.cid
LEFT JOIN ocean o ON oa.oid = o.oid;

INSERT INTO Query9
SELECT d.cname AS cname, d.height+d.depth AS totalspan
FROM country_depth d
WHERE d.height+d.depth IN (
SELECT MAX(d2.height+d2.depth)
FROM country_depth d2);

DROP VIEW country_depth;

-- Query 10 statements
CREATE VIEW BORDER_LENGTH AS
SELECT country, SUM(length) AS total_length FROM neighbour GROUP BY country;
CREATE VIEW BORDER_MAX AS
SELECT country, total_length AS max_length FROM BORDER_LENGTH WHERE total_length = (SELECT MAX(total_length) FROM BORDER_LENGTH);

INSERT INTO Query10
SELECT cname, max_length AS borderslength FROM country JOIN BORDER_MAX ON country.cid = BORDER_MAX.country;

DROP VIEW BORDER_MAX;
DROP VIEW BORDER_LENGTH;





