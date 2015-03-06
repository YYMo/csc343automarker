-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW neighbourheight AS
    (
    SELECT country, neighbor, height
    FROM   country, neighbour
    WHERE  neighbor=cid
    );

CREATE VIEW nothighest AS (
    SELECT DISTINCT a.country, a.neighbor
    FROM            neighbourheight a, neighbourheight b
    WHERE           a.country = b.country AND a.height < b.height );

CREATE VIEW highest AS
    (
        (
        SELECT country, neighbor
        FROM neighbourheight
        )
    EXCEPT
        (
        SELECT *
        FROM nothighest
        )
    );

INSERT INTO query1 (
    SELECT   country as c1id, a.cname as c1name, neighbor as c2id, b.cname as c2name
    FROM     highest, country a, country b
    WHERE    country=a.cid AND neighbor=b.cid
    ORDER BY c1name ASC
    );

DROP VIEW neighbourheight;
DROP VIEW nothighest;
DROP VIEW highest;

-- Query 2 statements

CREATE VIEW llocked AS
    (
    SELECT cid, cname
    FROM   country
    WHERE  cid NOT IN
        (
        SELECT cid
        FROM   oceanaccess
        )
    ORDER BY cname ASC
    );

INSERT INTO query2 SELECT * from llocked;

DROP VIEW llocked;

-- Query 3 statements


CREATE VIEW llocked AS
    (
    SELECT cid, cname
    FROM   country
    WHERE  cid NOT IN
        (
        SELECT cid
        FROM   oceanaccess
        )
    ORDER BY cname ASC
    );


CREATE VIEW onlyone AS
(
    SELECT DISTINCT country as c1id, cname as c1name
    FROM neighbour, country
    WHERE country NOT IN
    ( 
        SELECT a.country
        FROM neighbour a, neighbour b
        WHERE a.country=b.country AND a.neighbor<b.neighbor
    )
);

INSERT INTO query3
(
    SELECT c1id, c1name, neighbor as c2id, c.cname as c2name
    FROM   onlyone, country c, neighbour, llocked l
    WHERE  c1id=country AND c.cid=neighbor AND c1id=l.cid
    ORDER BY c1name ASC
);

DROP VIEW onlyone;
DROP VIEW llocked;


-- Query 4 statements

CREATE VIEW accessible AS
(
    SELECT country as cid, oid
    FROM neighbour, oceanaccess
    WHERE country=cid OR neighbor=cid
);

INSERT INTO query4
(
    SELECT cname, oname
    FROM accessible a, country c, ocean o
    WHERE o.oid=a.oid AND c.cid=a.cid
    ORDER BY cname ASC, oname DESC
);

DROP VIEW accessible;

-- Query 5 statements
CREATE VIEW averagehdi AS
(
    SELECT cid, AVG(hdi_score) as avghdi
    FROM hdi
    WHERE year>2008 AND year<2014
    GROUP BY cid
    ORDER by avghdi DESC
    LIMIT 10
);

INSERT INTO query5
(
    SELECT a.cid, a.cname, b.avghdi
    FROM country a, averagehdi b
    WHERE a.cid=b.cid
    ORDER BY avghdi DESC
);

DROP VIEW averagehdi;

-- Query 6 statements

CREATE VIEW hdi09 AS ( SELECT cid, hdi_score as hdi_09
                       FROM hdi
                       WHERE year=2009);

CREATE VIEW hdi10 AS ( SELECT cid, hdi_score as hdi_10
                       FROM hdi
                       WHERE year=2010);

CREATE VIEW hdi11 AS ( SELECT cid, hdi_score as hdi_11
                       FROM hdi
                       WHERE year=2011);

CREATE VIEW hdi12 AS ( SELECT cid, hdi_score as hdi_12
                       FROM hdi
                       WHERE year=2012);

CREATE VIEW hdi13 AS ( SELECT cid, hdi_score as hdi_13
                       FROM hdi
                       WHERE year=2013);

CREATE VIEW increasinghdi AS
(
    SELECT cid
    FROM hdi09 NATURAL JOIN hdi10 NATURAL JOIN hdi11 NATURAL JOIN hdi12 NATURAL JOIN hdi13
    WHERE hdi_09<hdi_10 AND hdi_10<hdi_11 AND hdi_11<hdi_12 AND hdi_12<hdi_13
);

INSERT INTO query6
(
    SELECT cid, cname
    FROM increasinghdi NATURAL JOIN country
    ORDER BY cname ASC
);

DROP VIEW increasinghdi;
DROP VIEW hdi09;
DROP VIEW hdi10;
DROP VIEW hdi11;
DROP VIEW hdi12;
DROP VIEW hdi13;

-- Query 7 statements

CREATE VIEW followersbycountry AS
(
    SELECT country.cid, rid, rname, round(population * rpercentage / 100) as follow
    FROM religion, country
    WHERE religion.cid = country.cid
);

CREATE VIEW totalfollowers AS
(
    SELECT rid, SUM(follow) as followers
    FROM followersbycountry
    GROUP BY rid
    ORDER BY followers DESC
);

INSERT INTO query7
(
    SELECT religion.rid, rname, followers
    FROM totalfollowers, religion
    WHERE totalfollowers.rid=religion.rid
    ORDER BY followers DESC
);

DROP VIEW totalfollowers;
DROP VIEW followersbycountry;

-- Query 8 statements

CREATE VIEW notpopular AS (
    SELECT a.cid, a.lid
    FROM language a, language b
    WHERE a.cid=b.cid AND a.lpercentage<b.lpercentage
);

CREATE VIEW popularid AS (
    SELECT cid, lid
    FROM language
    EXCEPT (
        SELECT * FROM notpopular
    )
);

CREATE VIEW popular AS (
    SELECT cname, lname
    FROM popularid p, country c, language l
    WHERE p.cid=c.cid AND l.lid=p.lid
);

INSERT INTO query8 (
    SELECT a.cname as c1name, b.cname as c2name, a.lname
    FROM popular a, popular b
    WHERE a.lname=b.lname AND a.cname<b.cname
    ORDER BY c1name ASC
);

DROP VIEW popular;
DROP VIEW popularid;
DROP VIEW notpopular;

-- Query 9 statements

CREATE VIEW oceanwithdepth AS (
    SELECT cid, a.oid, depth
    FROM oceanaccess a, ocean b
    WHERE a.oid=b.oid
);

CREATE VIEW notdeepest AS (
    SELECT a.cid, a.oid
    FROM oceanwithdepth a, oceanwithdepth b
    WHERE a.cid=b.cid AND a.depth<b.depth
);

CREATE VIEW deepest AS (
    SELECT * FROM oceanaccess EXCEPT (
        SELECT * from notdeepest
    )
);

CREATE VIEW llocked AS
(
    SELECT cid, 0 AS depth
    FROM   country
    WHERE  cid NOT IN
    (
        SELECT cid
        FROM   oceanaccess
    )
    ORDER BY cname ASC
);


CREATE VIEW actualdepth AS (
    SELECT a.cid, b.depth
    FROM deepest a, ocean b
    WHERE a.oid=b.oid
);


CREATE VIEW alldepth AS (
    (
        SELECT * FROM llocked
    ) UNION (
        SELECT * FROM actualdepth
    )
);

CREATE VIEW span AS (
    SELECT a.cname, b.depth + a.height AS totalspan
    FROM alldepth b, country a
    WHERE b.cid=a.cid
);

CREATE VIEW notmaxspan AS (
    SELECT a.cname, a.totalspan
    FROM span a, span b
    WHERE a.totalspan < b.totalspan
);

INSERT INTO query9 (
    SELECT * FROM span EXCEPT (
        SELECT * from notmaxspan
    )
);

DROP VIEW notmaxspan;
DROP VIEW span;
DROP VIEW alldepth;
DROP VIEW actualdepth;
DROP VIEW llocked;
DROP VIEW deepest;
DROP VIEW notdeepest;
DROP VIEW oceanwithdepth;


-- Query 10 statements

CREATE VIEW border AS (
    SELECT cname, SUM(length) as borderslength
    FROM country, neighbour
    WHERE cid=country
    GROUP BY cname
);

CREATE VIEW notlongest AS (
    SELECT a.cname, a.borderslength
    FROM border a, border b
    WHERE a.borderslength<b.borderslength
);

INSERT INTO query10 (
    SELECT * FROM border EXCEPT (
        SELECT * FROM notlongest
    )
);

DROP VIEW notlongest;
DROP VIEW border;


