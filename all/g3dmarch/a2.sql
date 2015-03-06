-- Query 1 statements

CREATE VIEW countries_new AS
SELECT cid AS c1id, cname AS c1name, neighbor FROM country NATURAL JOIN neighbour WHERE cid=country; 

CREATE VIEW neighbours_new AS
SELECT c1id, c1name, cid AS c2id, cname as c2name, height FROM countries_new NATURAL JOIN country WHERE neighbor=country.cid;

CREATE VIEW highest_elevation AS 
SELECT c1id, max(height), c1name FROM neighbours_new GROUP BY c1id, c1name ORDER BY c1id;

CREATE VIEW answer AS
select c1id, c1name, c2id, c2name FROM neighbours_new NATURAL JOIN highest_elevation WHERE max=height ORDER BY c1id;

INSERT INTO Query1(SELECT * FROM answer);
DROP VIEW countries_new CASCADE;


-- Query 2 statements

CREATE VIEW landlocked AS
SELECT cid, cname FROM country WHERE cid NOT IN (SELECT cid FROM oceanaccess) ORDER BY cname;

INSERT INTO Query2(SELECT * FROM landlocked);
DROP VIEW landlocked;


-- Query 3 statements

CREATE VIEW landlocked AS 
SELECT cid, cname FROM country WHERE cid NOT IN (SELECT cid FROM oceanaccess) ORDER BY cname;

CREATE VIEW exactly_one AS 
SELECT country FROM neighbour GROUP BY country HAVING count(country)=1;

CREATE VIEW locked_exactlyone AS
SELECT cid FROM ((SELECT cid FROM landlocked) INTERSECT (SELECT country FROM exactly_one))joined;

CREATE VIEW first AS
SELECT cid AS c1id, cname AS c1name from locked_exactlyone NATURAL JOIN country ORDER BY cid;

CREATE VIEW second AS
SELECT neighbor FROM first, neighbour WHERE c1id=country ORDER BY neighbor;

CREATE VIEW second_complete AS
SELECT neighbor AS c2id, cname AS c2name FROM second, country WHERE neighbor=cid ORDER BY neighbor;

CREATE VIEW answer AS
SELECT DISTINCT * FROM first, second_complete ORDER BY c1name;

INSERT INTO Query3 (SELECT * FROM answer);
DROP VIEW landlocked CASCADE;
DROP VIEW exactly_one CASCADE;


-- Query 4 statements --

CREATE VIEW indirect AS
SELECT country, neighbor, oid FROM oceanaccess NATURAL JOIN neighbour WHERE neighbor=cid;

CREATE VIEW joined_oceanaccess AS
SELECT country AS cid, oid FROM ((SELECT country, oid FROM indirect) UNION (SELECT * FROM oceanaccess)) joined_oceanaccess;

CREATE VIEW answer AS
SELECT cname, oname FROM (SELECT cid, oname FROM joined_oceanaccess NATURAL JOIN ocean)ocean_names NATURAL JOIN country ORDER BY cname ASC, oname DESC;

INSERT INTO Query4 (SELECT * FROM answer);
DROP VIEW indirect CASCADE;


-- Query 5 statements

CREATE VIEW relevant_years AS
SELECT * FROM hdi WHERE year >=2009 AND year <=2013;

CREATE VIEW hdi_average AS
SELECT cid, avg(hdi_score) AS avghdi FROM relevant_years GROUP BY cid;

CREATE VIEW answer AS 
SELECT hdi_average.cid AS cid, cname, avghdi FROM hdi_average, country WHERE hdi_average.cid=country.cid 
ORDER BY avghdi DESC LIMIT 10;

INSERT INTO Query5 (SELECT * FROM answer);
DROP VIEW relevant_years CASCADE;


-- Query 6 statements

CREATE VIEW year_nine AS
SELECT cid, hdi_score FROM hdi WHERE year='2009';

CREATE VIEW year_ten AS
SELECT cid, hdi_score FROM hdi WHERE year='2010';

CREATE VIEW year_eleven AS
SELECT cid, hdi_score FROM hdi WHERE year='2011';

CREATE VIEW year_twelve AS
SELECT cid, hdi_score FROM hdi WHERE year='2012';

CREATE VIEW year_thirteen AS
SELECT cid, hdi_score FROM hdi WHERE year='2013';

CREATE VIEW first_increase AS
SELECT year_ten.cid, year_ten.hdi_score FROM year_nine,year_ten WHERE year_ten.hdi_score>year_nine.hdi_score AND year_nine.cid=year_ten.cid;

CREATE VIEW second_increase AS
SELECT year_eleven.cid, year_eleven.hdi_score FROM first_increase,year_eleven WHERE year_eleven.hdi_score>first_increase.hdi_score AND first_increase.cid=year_eleven.cid;

CREATE VIEW third_increase AS
SELECT year_twelve.cid, year_twelve.hdi_score FROM second_increase,year_twelve WHERE year_twelve.hdi_score>second_increase.hdi_score AND second_increase.cid=year_twelve.cid;

CREATE VIEW fourth_increase AS
SELECT year_thirteen.cid FROM year_twelve,year_thirteen WHERE year_thirteen.hdi_score>third_increase.hdi_score AND third_increase.cid=year_thirteen.cid;

CREATE VIEW answer AS
SELECT cid, cname FROM fourth_increase NATURAL JOIN country;

INSERT INTO Query6(SELECT * FROM answer);

DROP VIEW year_nine CASCADE;
DROP VIEW year_ten CASCADE;
DROP VIEW year_eleven CASCADE;
DROP VIEW year_twelve CASCADE;
DROP VIEW year_thirteen CASCADE;


-- Query 7 statements

CREATE VIEW  relg_percent AS
SELECT (population * rpercentage) AS num, * FROM country NATURAL JOIN religion;

CREATE VIEW answer AS
SELECT rid, rname,  sum (num) AS followers FROM relg_percent 
GROUP BY relg_percent.rid, relg_percent.rname ORDER BY followers DESC;

DROP VIEW relg_percent CASCADE;


-- Query 8 statements -- 
CREATE VIEW max AS
SELECT cid, max(lpercentage) AS lpercentage FROM language GROUP BY cid;

CREATE VIEW popular_lang AS
SELECT cid, lname
FROM max NATURAL JOIN language
WHERE max.cid = language.cid AND max.lpercentage = language.lpercentage;

CREATE VIEW country_popular AS
SELECT country, lname AS c1lname, neighbor FROM popular_lang NATURAL JOIN neighbour WHERE country=cid;

CREATE VIEW pair_and_popular AS
SELECT country AS c1id, c1lname, neighbor AS c2id, lname AS c2lname FROM country_popular NATURAL JOIN popular_lang WHERE neighbor=cid;

CREATE VIEW find_pairs AS
SELECT c1id, c2id, c1lname AS lname FROM pair_and_popular WHERE c1lname = c2lname;

CREATE VIEW change_c1name AS
SELECT cname AS c1name, c2id, lname FROM find_pairs NATURAL JOIN country WHERE c1id = cid;

CREATE VIEW answer AS
SELECT c1name, cname AS c2name, lname FROM change_c1name NATURAL JOIN country WHERE c2id = cid;

INSERT INTO Query8 (SELECT * FROM answer);
DROP VIEW max CASCADE;


-- Query 9 statements
CREATE VIEW oceans_new AS
SELECT cname, height, depth FROM country NATURAL JOIN oceanaccess NATURAL JOIN ocean;

CREATE VIEW ocean_span AS
SELECT cname, max(depth-height) AS total_span FROM oceans_new GROUP BY cname;

CREATE VIEW totalspan_ocean AS
SELECT cname, total_span FROM ocean_span WHERE total_span = (SELECT max(total_span) from ocean_span);

CREATE VIEW landlocked AS
SELECT cname, height FROM country WHERE cid NOT IN (SELECT cid FROM oceanaccess) ORDER BY cname;

CREATE VIEW landlocked_span AS
SELECT cname, height AS total_span FROM landlocked WHERE height = (SELECT max(height) from landlocked);

CREATE VIEW combined_spans AS
SELECT * FROM ((SELECT * from totalspan_ocean) UNION (select * from  landlocked_span))all_countries;

CREATE VIEW answer AS
SELECT cname, total_span AS totalspan FROM combined_spans WHERE total_span = (SELECT max(total_span) 
FROM combined_spans);

INSERT INTO Query9 (SELECT * FROM answer);
DROP VIEW oceans_new CASCADE;
DROP VIEW landlocked CASCADE;


-- Query 10 statements

CREATE VIEW lengths AS
SELECT country, sum(length) AS borderslength FROM neighbour GROUP BY country;

CREATE VIEW max_border AS
SELECT country, borderslength
FROM lengths
WHERE borderslength = (SELECT max(borderslength) FROM lengths);

CREATE VIEW answer AS
SELECT cname, borderslength FROM max_border, country WHERE country = cid;

INSERT INTO Query10 (SELECT * FROM answer);
DROP VIEW lengths CASCADE;