-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW neighboring_countries AS
SELECT country AS c1id, c1.cname AS c1name, neighbor AS c2id, c2.cname AS c2name, c2.height as height
FROM neighbour join country c1 on neighbour.country = c1.cid join country c2 on neighbour.neighbor = c2.cid; 

CREATE VIEW neighbor_highest_ele AS
SELECT c1id, c1name, c2id, c2name
FROM neighboring_countries nc1
WHERE height >= ALL (
SELECT height
FROM neighboring_countries nc2
WHERE nc2.c1id=nc1.c1id
);

INSERT INTO Query1 (SELECT * FROM neighbor_highest_ele ORDER BY c1name ASC);

DROP VIEW neighbor_highest_ele;
DROP VIEW neighboring_countries;


-- Query 2 statements
CREATE VIEW landlocked_countries AS
SELECT cid
FROM country
WHERE cid <> ALL (
SELECT cid
FROM oceanAccess
);

INSERT INTO Query2 (
SELECT lc.cid AS cid, cname 
FROM landlocked_countries lc join country on lc.cid=country.cid
ORDER BY cname ASC);

DROP VIEW landlocked_countries;

-- Query 3 statements
CREATE VIEW landlocked_countries AS
SELECT cid
FROM country
WHERE cid <> ALL (
SELECT cid
FROM oceanAccess
);

CREATE VIEW surrounded_by_one_country AS
SELECT cid
FROM landlocked_countries
WHERE 1=(
SELECT count(neighbor)
FROM neighbour
GROUP BY neighbour.country
HAVING cid=neighbour.country
);

CREATE VIEW neighbor_of_each_country AS
SELECT cid AS c1id, neighbor AS c2id
FROM surrounded_by_one_country join neighbour on cid=country;

INSERT INTO Query3 (
SELECT c1id, c1.cname AS c1name, c2id, c2.cname AS c2name
FROM neighbor_of_each_country join country c1 on c1id=c1.cid join country c2 on c2id=c2.cid
ORDER BY c1name ASC);

DROP VIEW neighbor_of_each_country;
DROP VIEW surrounded_by_one_country;
DROP VIEW landlocked_countries;

-- Query 4 statements
CREATE VIEW countries_with_coastlines AS
SELECT cid, oname
FROM oceanAccess join ocean on oceanAccess.oid=ocean.oid;

CREATE VIEW countries_neighboring_oceans AS
SELECT country AS cid, cwo.oname AS oname
FROM countries_with_coastlines cwo join neighbour on cid = neighbor;

CREATE VIEW accessible_oceans AS
(SELECT * FROM countries_with_coastlines) 
UNION
(SELECT * FROM countries_neighboring_oceans);

INSERT INTO Query4 (
SELECT cname AS cname, oname
FROM accessible_oceans ao join country on ao.cid=country.cid
ORDER BY cname ASC, oname DESC);

DROP VIEW accessible_oceans;
DROP VIEW countries_neighboring_oceans;
DROP VIEW countries_with_coastlines;

-- Query 5 statements
CREATE VIEW hdi_2009_2013 AS
SELECT cid, hdi_score
FROM hdi
WHERE year>=2009 AND year<=2013;

CREATE VIEW highest_average_hdi AS
SELECT cid, avg(hdi_score) AS avghdi
FROM hdi_2009_2013
GROUP BY cid
ORDER BY avg(hdi_score);

INSERT INTO Query5 (
SELECT hah.cid AS cid, cname, avghdi
FROM highest_average_hdi hah join country on hah.cid=country.cid
ORDER BY avghdi DESC
LIMIT 10);

DROP VIEW highest_average_hdi;
DROP VIEW hdi_2009_2013;

-- Query 6 statements
CREATE VIEW hdi_2009_2013 AS
SELECT cid, year, hdi_score
FROM hdi
WHERE year>=2009 AND year<=2013;

CREATE VIEW not_increasing AS
SELECT cid
FROM hdi_2009_2013 hdi1
WHERE NOT hdi_score < (
SELECT hdi_score
FROM hdi_2009_2013
WHERE year=(hdi1.year+1) and hdi1.cid=cid
);

CREATE VIEW increasing AS
(SELECT cid FROM hdi_2009_2013) 
EXCEPT
(SELECT * FROM not_increasing);

INSERT INTO Query6 (
SELECT increasing.cid, cname
FROM increasing join country on increasing.cid=country.cid
ORDER BY cname ASC);

DROP VIEW increasing;
DROP VIEW not_increasing;
DROP VIEW hdi_2009_2013;

-- Query 7 statements
CREATE VIEW followers_per_country AS
SELECT religion.cid, rid, rname, rpercentage*population AS rpopulation
FROM religion join country on religion.cid=country.cid;

CREATE VIEW total_followers AS
SELECT rid, rname, sum(rpopulation) as followers
FROM followers_per_country fpc
GROUP BY rid, rname;

INSERT INTO Query7 (
SELECT * 
FROM total_followers
ORDER BY followers DESC);

DROP VIEW total_followers;
DROP VIEW followers_per_country;

-- Query 8 statements
CREATE VIEW language_population AS
SELECT language.cid, lid, lname, lpercentage*population AS lpopulation
FROM language join country on
language.cid=country.cid;

CREATE VIEW most_popular_language AS
SELECT cid, lid, lname
FROM language_population lp1
WHERE lpopulation >= ALL (
SELECT lpopulation
FROM language_population
WHERE lp1.cid=cid
);

CREATE VIEW same_popular_language AS
SELECT mpl1.cid AS c1id, mpl2.cid AS c2id, mpl1.lid, mpl1.lname
FROM most_popular_language mpl1, most_popular_language mpl2
WHERE mpl1.cid <> mpl2.cid AND mpl1.lid=mpl2.lid;

INSERT INTO Query8 (
SELECT c1.cname AS c1name, c2.cname AS c2name, lname
FROM same_popular_language spl join country c1 on c1.cid=c1id join country c2 on c2.cid=c2id
ORDER BY lname ASC, c1name DESC);

DROP VIEW same_popular_language;
DROP VIEW most_popular_language;
DROP VIEW language_population;

-- Query 9 statements
CREATE VIEW ocean_depths AS
SELECT cid, depth
FROM oceanAccess join ocean on oceanAccess.oid=ocean.oid right join country using(cid);

CREATE VIEW deepest_ocean AS
SELECT cid, depth
FROM ocean_depths od1
WHERE depth >= ALL(
SELECT depth
FROM ocean_depths od2
WHERE od2.cid=od1.cid
);

CREATE VIEW no_oceans AS
(SELECT cid
FROM country)
EXCEPT
(SELECT cid
FROM ocean_depths);

CREATE VIEW total_span_with_heights AS
SELECT cname, @(depth-height) AS totalspan
FROM deepest_ocean join country on deepest_ocean.cid=country.cid;

CREATE VIEW total_span_without_heights AS
SELECT cname, height AS totalspan
FROM no_oceans join country on no_oceans.cid=country.cid;

INSERT INTO Query9(
(SELECT * FROM total_span_with_heights)
UNION
(SELECT * FROM total_span_without_heights));

DROP VIEW total_span_without_heights;
DROP VIEW total_span_with_heights;
DROP VIEW no_oceans;
DROP VIEW deepest_ocean;
DROP VIEW ocean_depths;

-- Query 10 statements
CREATE VIEW neighbors_border_length AS
SELECT country AS cid, sum(length) as borderslength
FROM neighbour
GROUP BY country;

CREATE VIEW country_no_neighbor AS
(SELECT cid FROM country)
EXCEPT
(SELECT neighbor AS cid FROM neighbour);

CREATE VIEW neighborless_border_length AS
SELECT cid, 0 AS borderslength
FROM country_no_neighbor;

CREATE VIEW total_border_length AS
(SELECT * FROM neighbors_border_length)
UNION
(SELECT * FROM neighborless_border_length);

INSERT INTO Query10 (
SELECT cname, borderslength
FROM total_border_length join country on total_border_length.cid=country.cid);

DROP VIEW total_border_length;
DROP VIEW neighborless_border_length;
DROP VIEW country_no_neighbor;
DROP VIEW neighbors_border_length;
