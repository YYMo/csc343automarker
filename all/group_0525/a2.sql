-- Add below your SQL statements.
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result ta$
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW country_and_neighbour AS
        SELECT N.country as c1id, N.neighbor as c2id, C.height as height
        FROM Country C JOIN Neighbour N ON C.cid=N.neighbor;

CREATE VIEW max_elevation AS
        SELECT c1id, max(height) as height
        FROM country_and_neighbour
        GROUP BY c1id;

CREATE VIEW max_country_and_neighbour AS
        SELECT CN.c1id as c1id, CN.c2id as c2id
        FROM max_elevation E join country_and_neighbour CN ON E.c1id=CN.c1id
        and E.height=CN.height;

CREATE VIEW country_name AS
        SELECT C.cid as c1id, C.cname as c1name, M.c2id as c2id
        FROM max_country_and_neighbour M join Country C on M.c1id=C.cid;


CREATE VIEW neighbour_name AS
        SELECT C.cid as c2id, C.cname as c2name, M.c1id as c1id
        FROM max_country_and_neighbour M join Country C on M.c2id=C.cid;

INSERT INTO Query1(
        SELECT c1id, c1name, c2id, c2name
        FROM country_name natural join max_country_and_neighbour natural join neighbour_name
        ORDER BY c1name);

DROP VIEW country_and_neighbour CASCADE;


-- Query 2 statements
INSERT INTO Query2(
        SELECT cid, cname
        FROM Country
        WHERE cid NOT IN (Select cid from OceanAccess)
        ORDER BY cname
);


-- Query 3 statements
CREATE VIEW LandLocked AS
        SELECT cid, cname
        FROM Country
        WHERE cid NOT IN (Select cid from OceanAccess)
        ORDER BY cname;

CREATE VIEW ExactlyOne AS
        SELECT cid
        FROM Landlocked join Neighbour on Landlocked.cid=Neighbour.country
        GROUP BY cid
        HAVING COUNT(DISTINCT neighbor) = 1;

CREATE VIEW exact_one_country_neighbour AS
        SELECT N.country as c1id,N.neighbor as c2id
        FROM ExactlyOne join Neighbour N on ExactlyOne.cid=N.country;

CREATE VIEW country_info AS
        SELECT C.cid as c1id, C.cname as c1name, E.c2id as c2id
        FROM exact_one_country_neighbour E join Country C on C.cid=E.c1id;

CREATE VIEW neighbour_info AS
        SELECT C.cid as c2id, C.cname as c2name, E.c1id as c1id
        FROM exact_one_country_neighbour E join Country C on C.cid=E.c2id;


INSERT INTO Query3(
        SELECT C.c1id AS cid, C.c1name AS c1name, C.c2id AS c2i, N.c2name AS c2name
        FROM country_info C join neighbour_info N ON C.c1id=N.c1id and C.c2id=N.c2id
        ORDER BY C.c1name
);

DROP VIEW LandLocked CASCADE;


-- Query 4 statements
CREATE VIEW direct_access AS
        SELECT cid, oid
        FROM oceanAccess;

CREATE VIEW neighbouring AS
        SELECT N.country as cid, oid
        FROM neighbour N join oceanAccess A ON N.neighbor = A.cid;

CREATE VIEW together AS
        SELECT C.cname as cname, O.oname as oname
        FROM direct_access D natural join country C natural join ocean O
        UNION
        SELECT C.cname as cname, O.oname as oname
        FROM neighbouring N natural join country C natural join ocean O;

INSERT INTO Query4(
        SELECT cname, oname
        FROM together
        ORDER BY cname ASC, oname DESC

);
DROP VIEW direct_access CASCADE;
DROP VIEW neighbouring CASCADE;

-- Query 5 statements
CREATE VIEW FiveYear AS
        SELECT *
        FROM hdi
        WHERE year >= 2009 and year <= 2013;

CREATE VIEW Highest AS
        SELECT cid, avg(hdi_score) as avghdi
        FROM FiveYear
        GROUP BY cid
        ORDER BY avg(hdi_score) desc
        LIMIT 10;

INSERT INTO Query5(
        SELECT cid, cname, avghdi
        FROM Highest natural join Country
	ORDER BY avghdi DESC
);
DROP VIEW FiveYear CASCADE;


-- Query 6 statements
CREATE VIEW hdi_nine AS
        SELECT cid, cname, hdi_score
        FROM country natural join hdi
        WHERE year = 2009;

CREATE VIEW hdi_ten AS
        SELECT country.cid, cname, hdi_score
        FROM country natural join hdi
        WHERE year = 2010;


CREATE VIEW hdi_eleven AS
        SELECT country.cid, cname, hdi_score
        FROM country natural join hdi
        WHERE year = 2011;


CREATE VIEW hdi_twelve AS
        SELECT country.cid, cname, hdi_score
        FROM country natural join hdi
        WHERE year = 2012;


CREATE VIEW hdi_thirteen AS
        SELECT country.cid, cname, hdi_score
        FROM country natural join hdi
        WHERE year = 2013;

CREATE VIEW const_inc_ten AS
        SELECT hdi_nine.cid, hdi_nine.cname, hdi_ten.hdi_score
        FROM hdi_nine join hdi_ten on hdi_nine.cid = hdi_ten.cid
        WHERE hdi_nine.hdi_score < hdi_ten.hdi_score;


CREATE VIEW const_inc_eleven AS
        SELECT const_inc_ten.cid, const_inc_ten.cname, hdi_eleven.hdi_score
        FROM hdi_eleven join const_inc_ten on hdi_eleven.cid = const_inc_ten.cid
        WHERE const_inc_ten.hdi_score < hdi_eleven.hdi_score;

CREATE VIEW const_inc_twelve AS
        SELECT const_inc_eleven.cid, const_inc_eleven.cname, hdi_twelve.hdi_score
        FROM hdi_twelve join const_inc_eleven on hdi_twelve.cid = const_inc_eleven.cid
        WHERE const_inc_eleven.hdi_score < hdi_twelve.hdi_score;

CREATE VIEW const_inc_thirteen AS
        SELECT const_inc_twelve.cid, const_inc_twelve.cname, hdi_thirteen.hdi_score
        FROM hdi_thirteen join const_inc_twelve on hdi_thirteen.cid = const_inc_twelve.cid
        WHERE const_inc_twelve.hdi_score < hdi_thirteen.hdi_score;

INSERT INTO Query6(
        SELECT cid, cname
        FROM const_inc_thirteen
        ORDER BY cname ASC);

DROP VIEW const_inc_thirteen CASCADE;

-- Query 7 statements
CREATE VIEW country_religion AS
        SELECT cid, rid, rname, rpercentage, population
        FROM country natural join religion;

CREATE VIEW country_rel_pop AS
        SELECT cid, rid, rname, (rpercentage * population) as rel_pop
        FROM country_religion;

INSERT INTO Query7(
        SELECT rid, rname, sum(rel_pop) as followers
        FROM country_rel_pop
        GROUP BY rid, rname
	ORDER BY followers DESC
);

DROP VIEW country_rel_pop CASCADE;

-- Query 8 statements
CREATE VIEW country_languages AS
        SELECT cid, cname, lid, lname, lpercentage
        FROM country natural join language;

CREATE VIEW max_language AS
        SELECT cid, MAX(lpercentage) as mpercentage
        FROM country_languages
        GROUP BY cid;

CREATE VIEW max_tuple AS
        SELECT country_languages.cid, country_languages.cname,
        country_languages.lid, country_languages.lname
        FROM country_languages inner join max_language
        ON country_languages.cid = max_language.cid
        AND country_languages.lpercentage = max_language.mpercentage;

INSERT INTO Query8(
        SELECT c1.cname AS c1name, c2.cname AS c2name, c1.lname AS lname
        FROM max_tuple as c1, max_tuple as c2
        WHERE c1.cid != c2.cid AND c1.lid = c2.lid
        AND (c1.cid, c2.cid)
        IN (SELECT country, neighbor FROM neighbour)
	ORDER BY lname ASC, c1name DESC);

DROP VIEW max_tuple CASCADE;

-- Query 9 statements

CREATE VIEW access_depth AS
        SELECT cid, oid, depth
        FROM ocean natural join oceanAccess;

CREATE VIEW deepest_depth AS
        SELECT access_depth.cid, access_depth.depth
        FROM access_depth JOIN
        (SELECT cid, MAX(depth) as mdepth
        FROM access_depth
        GROUP BY cid) d1
        ON access_depth.cid = d1.cid
        AND access_depth.depth = d1.mdepth;

CREATE VIEW no_depth AS
        SELECt cname, height AS totalspan
        FROM country
        WHERE cid NOT IN
        (SELECT cid FROM oceanAccess);


CREATE VIEW with_depth AS
        SELECT cname, (height + depth) as totalspan
        FROM country natural join deepest_depth
        WHERE (height + depth) IN
        (SELECT MAX(height + depth)
        FROM deepest_depth natural join country);

CREATE VIEW all_depth AS
        SELECT cname, totalspan
        FROM no_depth
        UNION
        SELECT cname, totalspan
        FROM with_depth;

INSERT INTO Query9(
        SELECT cname, totalspan
        FROM all_depth
        WHERE totalspan IN
        (SELECT MAX(totalspan)
        FROM all_depth));

DROP VIEW all_depth CASCADE;
 

-- Query 10 statements
CREATE VIEW country_border AS
        SELECT country, SUM(length) as borderlength
        FROM neighbour
        GROUP BY country;

INSERT INTO Query10(
        SELECT cname, borderlength
        FROM country join country_border
        ON country.cid = country_border.country
        WHERE borderlength IN
        (SELECT MAX(borderlength)
        FROM country_border));

DROP VIEW country_border;

