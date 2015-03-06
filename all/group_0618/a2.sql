-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.



-- Query 1 statements
CREATE VIEW ADJ_LIST AS select neighbour.country o_cid, neighbour.neighbor n_cid, country.cname n_cname, country.height n_height from neighbour, country where neighbour.neighbor = country.cid;

CREATE VIEW ALMOST_THERE AS select c1.cid c1id, c1.cname c1name, c2.cid c2id, c2.cname c2name from country c1, country c2, (select o_cid,  max(n_height) n_max_height from ADJ_LIST group by o_cid) as adj where c1.cid = o_cid and c2.height = n_max_height order by c1.cname;

insert into Query1 (select * from ALMOST_THERE);

DROP VIEW IF EXISTS ALMOST_THERE;
DROP VIEW IF EXISTS ADJ_LIST;


-- Query 2 statements
INSERT INTO query2 (select cid, cname from country where NOT EXISTS (select * from oceanaccess where oceanaccess.cid = country.cid) ORDER BY cname);


-- Query 3 statements
CREATE VIEW landlocked AS select cid, cname from country where NOT EXISTS (select * from oceanaccess where oceanaccess.cid = country.cid);

CREATE VIEW only_one AS select cid, count(*) from landlocked, neighbour where neighbour.neighbor = landlocked.cid group by cid having count(*) = 1;

CREATE VIEW result AS select country_o.cid c1id, country_o.cname c1name, country_n.cid c2id, country_n.cname c2name from only_one, country country_o, country country_n, neighbour where only_one.cid = country_o.cid and neighbour.country = country_o.cid and neighbour.neighbor = country_n.cid;

insert into query3 (select c1id, c1name, c2id, c2name from result order by c1name);

DROP VIEW IF EXISTS result;
DROP VIEW IF EXISTS only_one;
DROP VIEW IF EXISTS landlocked;


-- Query 4 statements
CREATE VIEW countries_w_ocean AS select cid, oid FROM oceanaccess;
CREATE VIEW countries_wo_ocean AS select cid FROM country where cid NOT IN (select cid from countries_w_ocean);
CREATE VIEW adj_countries_wo_ocean AS select cid from countries_wo_ocean, neighbour where countries_wo_ocean.cid = neighbour.country AND EXISTS (select cid from countries_w_ocean where neighbour.neighbor = cid); --<<<get the name and other info

CREATE VIEW neighbours_w_ocean AS select adj_countries_wo_ocean.cid as cid_no_ocean, countries_w_ocean.cid cid_neighbour_ocean, countries_w_ocean.oid from adj_countries_wo_ocean, countries_w_ocean, neighbour where neighbour.country = adj_countries_wo_ocean.cid and neighbour.neighbor = countries_w_ocean.cid;

CREATE VIEW accessible_ocean AS (select cid, oid from countries_w_ocean) UNION (select cid_no_ocean cid, oid from neighbours_w_ocean oid);

INSERT INTO query4 (select country.cname, ocean.oname from accessible_ocean, country, ocean where accessible_ocean.cid = country.cid and accessible_ocean.oid = ocean.oid order by ocean.oid desc, ocean.oname desc);

DROP VIEW IF EXISTS accessible_ocean;
DROP VIEW IF EXISTS neighbours_w_ocean;
DROP VIEW IF EXISTS adj_countries_wo_ocean;
DROP VIEW IF EXISTS countries_wo_ocean;
DROP VIEW IF EXISTS countries_w_ocean;


-- Query 5 statements
--to improve data-->>
CREATE VIEW avg_hdi AS (SELECT cid, avg(hdi_score) avg_hdi_score from hdi where year >= 2009 and year <= 2013  GROUP BY cid);
CREATE VIEW answer_q5 AS select country.cid, cname, avg_hdi_score from avg_hdi, country where avg_hdi.cid = country.cid order by avg_hdi_score desc;
insert into query5 (select * from answer_q5);

DROP VIEW IF EXISTS answer_q5;
DROP VIEW IF EXISTS avg_hdi;


-- Query 6 statements
--to improve data-->>
CREATE VIEW ascending_country AS select h1.cid from hdi h1, hdi h2, hdi h3, hdi h4, hdi h5 where h1.cid = h2.cid and h2.cid = h3.cid and h3.cid = h4.cid and h4.cid = h5.cid and h5.hdi_score > h4.hdi_score and h4.hdi_score > h3.hdi_score and h3.hdi_score > h2.hdi_score and h2.hdi_score > h1.hdi_score and h1.year = 2009 and h2.year = 2010 and h3.year = 2011 and h4.year = 2012 and h5.year = 2013;
CREATE VIEW answer_q6 AS select country.cid, country.cname from ascending_country JOIN country on ascending_country.cid = country.cid order by cname asc;

INSERT INTO query6 (select * from answer_q6);

DROP VIEW IF EXISTS answer_q6;
DROP VIEW IF EXISTS ascending_country;


-- Query 7 statements
-->> PQ PRECISEI USAR DISTINCT?
CREATE VIEW religion_total AS select religion.cid, religion.rid, (religion.rpercentage*country.population/100) rel_people from religion, country where religion.cid = country.cid;

CREATE VIEW religious_people_world AS select sum(rel_people) followers, rid from religion_total group by rid;

CREATE VIEW answer_q7 AS select DISTINCT religious_people_world.rid, religion.rname, followers from religious_people_world, religion where religious_people_world.rid = religion.rid order by followers desc;

insert into query7 (select * from answer_q7);

DROP VIEW IF EXISTS answer_q7;
DROP VIEW IF EXISTS religious_people_world;
DROP VIEW IF EXISTS religion_total;


-- Query 8 statements
CREATE VIEW top_language AS 
select cid, lid from language l1 where l1.lpercentage >= ALL (select lpercentage from language l2 where l1.lid <> l2.lid and l1.cid = l2.cid);

CREATE VIEW neighbour_popular_langage AS
select tl_1.cid cid_1, tl_2.cid cid_2, tl_2.lid lid_1 from top_language tl_1, top_language tl_2, neighbour where neighbour.country = tl_1.cid and neighbour.neighbor = tl_2.cid and tl_1.cid <> tl_2.cid and tl_1.lid = tl_2.lid; 

CREATE VIEW answer_q8 AS
select distinct c1.cname c1name, c2.cname c2name, lname from neighbour_popular_langage nl, country c1, country c2, language where c1.cid = nl.cid_1 and c2.cid = nl.cid_2 and nl.lid_1 = language.lid;

INSERT INTO query8 (select * from answer_q8 order by lname asc, c1name desc);

DROP VIEW IF EXISTS answer_q8;
DROP VIEW IF EXISTS neighbour_popular_langage;
DROP VIEW IF EXISTS top_language;


-- Query 9 statements
CREATE VIEW countries_w_ocean AS select cid, oid FROM oceanaccess;
CREATE VIEW countries_wo_ocean AS select cid FROM country where cid NOT IN (select cid from countries_w_ocean);

CREATE VIEW delta_ocean AS
select country.cid, country.cname, (country.height+ocean.depth) total_distance from countries_w_ocean, country, ocean where country.cid = countries_w_ocean.cid and ocean.oid = countries_w_ocean.oid;

CREATE VIEW delta_no_ocean AS
select country.cid, country.cname, country.height as total_distance from country, countries_wo_ocean where country.cid = countries_wo_ocean.cid;

CREATE VIEW delta_union AS 
select * from ((select * from delta_ocean) UNION (select * from delta_no_ocean)) AS delta_union;

CREATE VIEW answer_q9 AS 
select cname, total_distance as totalspan from delta_union where total_distance = (select max(delta_union.total_distance) from ((select * from delta_ocean) UNION (select * from delta_no_ocean)) AS delta_union);

INSERT INTO query9 (select * from answer_q9);

DROP VIEW IF EXISTS answer_q9;
DROP VIEW IF EXISTS delta_union;
DROP VIEW IF EXISTS delta_no_ocean;
DROP VIEW IF EXISTS delta_ocean;
DROP VIEW IF EXISTS countries_wo_ocean;
DROP VIEW IF EXISTS countries_w_ocean;


-- Query 10 statements
CREATE VIEW largest_border AS
select neighbour.country as cid, sum(length) as borderslength from neighbour group by neighbour.country limit 1;

CREATE VIEW answer_q10 AS
select country.cname, borderslength from largest_border, country where largest_border.cid = country.cid;

INSERT INTO query10 (select * from answer_q10);

DROP VIEW IF EXISTS answer_q10;
DROP VIEW IF EXISTS largest_border;
