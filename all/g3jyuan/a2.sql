-- Add below your SQL statements.
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW neighbour_data AS
SELECT *
FROM neighbour JOIN country ON neighbor = cid;

INSERT INTO Query1 (
    SELECT
        c1.cid   AS c1id,
        c1.cname AS c1name,
        c2.cid   AS c2id,
        c2.cname AS c2name
    FROM country c1 JOIN neighbour_data c2 ON c1.cid = c2.country
    WHERE c2.height = (
            SELECT MAX(height)
            FROM neighbour_data n
            WHERE n.country = c1.cid
        )
    ORDER BY c1name
);

DROP VIEW neighbour_data;

-- Query 2 statements
INSERT INTO Query2 (
    SELECT cid, cname
    FROM country c
    WHERE NOT EXISTS (
        SELECT *
        FROM oceanAccess o
        WHERE o.cid = c.cid
    )
    ORDER BY cname
);

-- Query 3 statements
CREATE VIEW landlocked AS
SELECT cid, cname
FROM country c
WHERE NOT EXISTS (
    SELECT *
    FROM oceanAccess o
    WHERE o.cid = c.cid
);

INSERT INTO Query3 (
    SELECT
        c1.cid   AS c1id,
        c1.cname AS c1name,
        c2.cid   AS c2id,
        c2.cname AS c2name
    FROM
        landlocked c1
        JOIN neighbour n ON c1.cid = n.country
        JOIN country c2  ON n.neighbor = c2.cid
    WHERE 1 = (
        SELECT COUNT(*)
        FROM neighbour n2
        WHERE n2.country = c1.cid
    )
    ORDER BY c1name
);

DROP VIEW landlocked;

-- Query 4 statements
CREATE VIEW direct_access AS
SELECT cid, cname, oname
FROM
    country c
    JOIN oceanAccess USING (cid)
    JOIN ocean       USING (oid)
;

CREATE VIEW indirect_access AS
SELECT
    c.cid   AS cid,
    c.cname AS cname,
    d.oname AS oname
FROM
    country c
    JOIN neighbour n     ON c.cid = n.country
    JOIN direct_access d ON n.neighbor = d.cid
;

INSERT INTO Query4 (
    (SELECT cname, oname
     FROM direct_access)
    UNION
    (SELECT cname, oname
     FROM indirect_access)
    ORDER BY
        cname,
        oname DESC
);

DROP VIEW indirect_access;
DROP VIEW direct_access;

-- Query 5 statements
CREATE VIEW top_ten AS
SELECT
    cid,
    AVG(hdi_score) AS avghdi
FROM hdi
WHERE
    year >= 2009
    AND
    year <= 2013
GROUP BY cid
ORDER BY avghdi DESC
LIMIT 10;

INSERT INTO Query5 (
    SELECT
        c1.cid   AS cid,
        c1.cname AS cname,
        avghdi
    FROM country c1 JOIN top_ten USING (cid)
    ORDER BY avghdi DESC
);

DROP VIEW top_ten;

-- Query 6 statements
INSERT INTO Query6 (
    SELECT cid, cname
    FROM
        country
        JOIN hdi h1 USING (cid)
        JOIN hdi h2 USING (cid)
        JOIN hdi h3 USING (cid)
        JOIN hdi h4 USING (cid)
        JOIN hdi h5 USING (cid)
    WHERE
        h1.year = 2009
        AND
        h2.year = 2010
        AND
        h3.year = 2011
        AND
        h4.year = 2012
        AND
        h5.year = 2013
        AND
        h1.hdi_score < h2.hdi_score
        AND
        h2.hdi_score < h3.hdi_score
        AND
        h3.hdi_score < h4.hdi_score
        AND
        h4.hdi_score < h5.hdi_score
    ORDER BY cname
);

-- Query 7 statements
CREATE VIEW religion_pop AS
SELECT
    rid,
    SUM(rpercentage * population) AS followers
FROM religion JOIN country USING (cid)
GROUP BY rid;

INSERT INTO Query7 (
    SELECT DISTINCT rid, rname, followers
    FROM religion JOIN religion_pop USING (rid)
    ORDER BY followers DESC
);

DROP VIEW religion_pop;

-- Query 8 statements
CREATE VIEW country_language_pop AS
SELECT cid, lid, lname
FROM language JOIN country c USING (cid)
WHERE lpercentage = (
    SELECT MAX(lpercentage)
    FROM language l2
    WHERE l2.cid = c.cid
);

INSERT INTO Query8 (
    SELECT
        c1.cname AS c1name,
        c2.cname AS c2name,
        p1.lname AS lname
    FROM
        country c1
        JOIN neighbour               ON c1.cid = neighbour.country
        JOIN country c2              ON c2.cid = neighbour.neighbor
        JOIN country_language_pop p1 ON p1.cid = c1.cid
        JOIN country_language_pop p2 ON p2.cid = c2.cid
    WHERE p1.lid = p2.lid
    ORDER BY
        lname,
        c1name DESC
);

DROP VIEW country_language_pop;

-- Query 9 statements
CREATE VIEW landlocked AS
SELECT *
FROM country c
WHERE NOT EXISTS (
    SELECT *
    FROM oceanAccess o
    WHERE o.cid = c.cid
);

CREATE VIEW ocean_span AS
SELECT
    cname,
    height + depth AS totalspan
FROM
    country
    JOIN oceanAccess USING (cid)
    JOIN ocean       USING (oid)
;

CREATE VIEW country_span AS
(SELECT
    cname,
    height AS totalspan
 FROM landlocked)
UNION
(SELECT
    cname,
    MAX(totalspan)
 FROM ocean_span
 GROUP BY cname);

INSERT INTO Query9 (
    SELECT *
    FROM country_span
    WHERE totalspan >= ALL (
        SELECT totalspan
        FROM country_span
    )
);

DROP VIEW country_span;
DROP VIEW ocean_span;
DROP VIEW landlocked;

-- Query 10 statements
CREATE VIEW country_border_length AS
SELECT
    cname,
    SUM(length) AS borderslength
FROM country JOIN neighbour ON cid = country
GROUP BY cname;

INSERT INTO Query10 (
    SELECT *
    FROM country_border_length
    WHERE borderslength >= ALL (
        SELECT borderslength
        FROM country_border_length
    )
);

DROP VIEW country_border_length;
