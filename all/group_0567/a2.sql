-- Query 1 statements

CREATE VIEW first_countries AS
SELECT cid AS c1id, cname AS c1name, neighbor FROM country NATURAL JOIN neighbour WHERE cid=country; 

CREATE VIEW first_and_neighbours AS
SELECT c1id, c1name, cid AS c2id, cname as c2name, height FROM first_countries NATURAL JOIN country WHERE neighbor=country.cid;

CREATE VIEW maxes AS 
SELECT c1id, max(height), c1name FROM first_and_neighbours GROUP BY c1id, c1name ORDER BY c1id;

CREATE VIEW answer AS
select c1id, c1name, c2id, c2name FROM first_and_neighbours NATURAL JOIN maxes WHERE max=height ORDER BY c1id;

INSERT INTO Query1(SELECT * FROM answer);
DROP VIEW first_countries CASCADE;


-- Query 2 statements

CREATE VIEW landlocked_countries AS
SELECT cid, cname FROM country WHERE cid NOT IN (SELECT cid FROM oceanaccess) ORDER BY cname;

INSERT INTO Query2(SELECT * FROM landlocked_countries);
DROP VIEW landlocked_countries;


-- Query 3 statements

CREATE VIEW landlocked_countries AS 
SELECT cid, cname FROM country WHERE cid NOT IN (SELECT cid FROM oceanaccess) ORDER BY cname;

CREATE VIEW surrounded_by_1 AS 
SELECT country FROM neighbour GROUP BY country HAVING count(country)=1;

CREATE VIEW locked_and_surrounded AS
SELECT cid FROM ((SELECT cid FROM landlocked_countries) INTERSECT (SELECT country FROM surrounded_by_1))intersected;

CREATE VIEW first_countries AS
SELECT cid AS c1id, cname AS c1name from locked_and_surrounded NATURAL JOIN country ORDER BY cid;

CREATE VIEW second_countries AS
SELECT neighbor FROM first_countries, neighbour WHERE c1id=country ORDER BY neighbor;

CREATE VIEW second_countries_complete AS
SELECT neighbor AS c2id, cname AS c2name FROM second_countries, country WHERE neighbor=cid ORDER BY neighbor;

CREATE VIEW answer AS
SELECT DISTINCT * FROM first_countries, second_countries_complete ORDER BY c1name;

INSERT INTO Query3 (SELECT * FROM answer);
DROP VIEW landlocked_countries CASCADE;
DROP VIEW surrounded_by_1 CASCADE;


-- Query 4 statements --

CREATE VIEW indirect_access AS
SELECT country, neighbor, oid FROM oceanaccess NATURAL JOIN neighbour WHERE neighbor=cid;

CREATE VIEW all_access AS
SELECT country AS cid, oid FROM ((SELECT country, oid FROM indirect_access) UNION (SELECT * FROM oceanaccess)) all_access;

CREATE VIEW answer AS
SELECT cname, oname FROM (SELECT cid, oname FROM all_access NATURAL JOIN ocean)ocean_names NATURAL JOIN country ORDER BY cname ASC, oname DESC;

INSERT INTO Query4 (SELECT * FROM answer);
DROP VIEW indirect_access CASCADE;


-- Query 5 statements

CREATE VIEW in_years AS
SELECT * FROM hdi WHERE year >=2009 AND year <=2013;

CREATE VIEW averages AS
SELECT cid, avg(hdi_score) AS avghdi FROM in_years GROUP BY cid;

CREATE VIEW answer AS 
SELECT averages.cid AS cid, cname, avghdi FROM averages, country WHERE averages.cid=country.cid 
ORDER BY avghdi DESC LIMIT 10;

INSERT INTO Query5 (SELECT * FROM answer);
DROP VIEW in_years CASCADE;


-- Query 6 statements

CREATE VIEW nine AS
SELECT cid, hdi_score FROM hdi WHERE year='2009';

CREATE VIEW ten AS
SELECT cid, hdi_score FROM hdi WHERE year='2010';

CREATE VIEW eleven AS
SELECT cid, hdi_score FROM hdi WHERE year='2011';

CREATE VIEW twelve AS
SELECT cid, hdi_score FROM hdi WHERE year='2012';

CREATE VIEW thirteen AS
SELECT cid, hdi_score FROM hdi WHERE year='2013';

CREATE VIEW first_increase AS
SELECT ten.cid, ten.hdi_score FROM nine,ten WHERE ten.hdi_score>nine.hdi_score AND nine.cid=ten.cid;

CREATE VIEW second_increase AS
SELECT eleven.cid, eleven.hdi_score FROM first_increase, eleven 
WHERE eleven.hdi_score>first_increase.hdi_score AND first_increase.cid=eleven.cid;

CREATE VIEW third_increase AS
SELECT twelve.cid, twelve.hdi_score FROM second_increase, twelve 
WHERE twelve.hdi_score>second_increase.hdi_score AND second_increase.cid=twelve.cid;

CREATE VIEW fourth_increase AS
SELECT thirteen.cid FROM third_increase, thirteen 
WHERE thirteen.hdi_score>third_increase.hdi_score AND third_increase.cid=thirteen.cid;

CREATE VIEW answer AS
SELECT cid, cname FROM fourth_increase NATURAL JOIN country;

INSERT INTO Query6(SELECT * FROM answer);

DROP VIEW nine CASCADE;
DROP VIEW ten CASCADE;
DROP VIEW eleven CASCADE;
DROP VIEW twelve CASCADE;
DROP VIEW thirteen CASCADE;


-- Query 7 statements

CREATE VIEW  religion_and_num AS
SELECT (population * rpercentage) AS num, * FROM country NATURAL JOIN religion;

CREATE VIEW answer AS
SELECT rid, rname,  sum (num) AS followers FROM religion_and_num 
GROUP BY religion_and_num.rid, religion_and_num.rname ORDER BY followers DESC;

DROP VIEW religion_and_num CASCADE;


-- Query 8 statements -- 
CREATE VIEW max AS
SELECT cid, max(lpercentage) AS lpercentage FROM language GROUP BY cid;

CREATE VIEW most_pop AS
SELECT cid, lname
FROM max NATURAL JOIN language
WHERE max.cid = language.cid AND max.lpercentage = language.lpercentage;

CREATE VIEW first_pops AS
SELECT country, lname AS c1lname, neighbor FROM most_pop NATURAL JOIN neighbour WHERE country=cid;

CREATE VIEW combined_pops AS
SELECT country AS c1id, c1lname, neighbor AS c2id, lname AS c2lname FROM first_pops NATURAL JOIN most_pop WHERE neighbor=cid;

CREATE VIEW pairs AS
SELECT c1id, c2id, c1lname AS lname FROM combined_pops WHERE c1lname = c2lname;

CREATE VIEW first_rename AS
SELECT cname AS c1name, c2id, lname FROM pairs NATURAL JOIN country WHERE c1id = cid;

CREATE VIEW answer AS
SELECT c1name, cname AS c2name, lname FROM first_rename NATURAL JOIN country WHERE c2id = cid;

INSERT INTO Query8 (SELECT * FROM answer);
DROP VIEW max CASCADE;


-- Query 9 statements
CREATE VIEW oceans_and_depths AS
SELECT cname, height, depth FROM country NATURAL JOIN oceanaccess NATURAL JOIN ocean;

CREATE VIEW ocean_span AS
SELECT cname, max(depth-height) AS total_span FROM oceans_and_depths GROUP BY cname;

CREATE VIEW max_diff_ocean AS
SELECT cname, total_span FROM ocean_span WHERE total_span = (SELECT max(total_span) from ocean_span);

CREATE VIEW landlocked_countries AS
SELECT cname, height FROM country WHERE cid NOT IN (SELECT cid FROM oceanaccess) ORDER BY cname;

CREATE VIEW landlocked_span AS
SELECT cname, height AS total_span FROM landlocked_countries WHERE height = (SELECT max(height) from landlocked_countries);

CREATE VIEW all_country_spans AS
SELECT * FROM ((SELECT * from max_diff_ocean) UNION (select * from  landlocked_span))all_countries;

CREATE VIEW answer AS
SELECT cname, total_span AS totalspan FROM all_country_spans WHERE total_span = (SELECT max(total_span) 
FROM all_country_spans);

INSERT INTO Query9 (SELECT * FROM answer);
DROP VIEW oceans_and_depths CASCADE;
DROP VIEW landlocked_countries CASCADE;


-- Query 10 statements

CREATE VIEW border_lengths AS
SELECT country, sum(length) AS borderslength FROM neighbour GROUP BY country;

CREATE VIEW cid_border AS
SELECT country, borderslength
FROM border_lengths
WHERE borderslength = (SELECT max(borderslength) FROM border_lengths);

CREATE VIEW answer AS
SELECT cname, borderslength FROM cid_border, country WHERE country = cid;

INSERT INTO Query10 (SELECT * FROM answer);
DROP VIEW border_lengths CASCADE;
