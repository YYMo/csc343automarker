-- Add below your SQL statements.
-- You can CREATE VIEWrmediate views (AS needed). Remember to DROP VIEWe views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW q1_cn_pair AS
	-- Query to find all the country, neighbors and neighbor's height
	SELECT n.country, n.neighbor, c.height
		FROM neighbour AS n, country AS c
		WHERE n.neighbor = c.cid
		ORDER BY (n.country, c.height) DESC;

CREATE VIEW q1_maxhts AS
	-- Query to find the highest neighbor's height among all the
	--   country-neighbor pairs.
	SELECT country, max(height) AS max_ht
		FROM q1_cn_pair
		GROUP BY country;

CREATE VIEW q1_maxng AS
	-- Query to get the neighbor
	SELECT a.country, a.neighbor
		FROM q1_cn_pair AS a
		WHERE a.height = (
			SELECT max_ht
				FROM q1_maxhts
				WHERE country = a.country
		);

INSERT INTO Query1 (
	-- Final Query! get country names and cid's
	SELECT a.cid, a.cname, b.cid, b.cname
		FROM country AS a, country AS b
		WHERE (a.cid, b.cid) = (
			SELECT * FROM q1_maxng
				WHERE country = a.cid AND
					neighbor = b.cid
		)
		ORDER BY a.cname ASC
);
DROP VIEW q1_maxng;
DROP VIEW q1_maxhts;
DROP VIEW q1_cn_pair;


-- Query 2 statements
CREATE VIEW q2_cwo AS
	-- Query to find country with oceans beside it
	SELECT cid
		FROM oceanAccess
		GROUP BY cid;

INSERT INTO Query2 (
	-- Query to find which countries don't exists in q2_cwo
	SELECT c.cid, c.cname
		FROM country AS c
		WHERE NOT EXISTS (
			SELECT *
				FROM   q2_cwo AS q
				WHERE  c.cid = q.cid
		)
		ORDER BY c.cname ASC
);
DROP VIEW q2_cwo;


-- Query 3 statements
CREATE VIEW query3_1negh AS
	-- Query to find countries with one neighbor.
	SELECT country
		FROM neighbour
		GROUP BY country
		HAVING COUNT(neighbor) = 1;

INSERT INTO Query3 (
	-- Final Query. Find the cid and cname of the results of the view above.
	SELECT a.cid, a.cname, b.cid, b.cname
		FROM country AS a, country AS b
		WHERE
			a.cid IN (
				SELECT country
					FROM query3_1negh
			) AND b.cid IN (
				SELECT neighbor
					FROM neighbour
					WHERE b.cid = neighbor AND a.cid = country)
		ORDER BY a.cid ASC
);
DROP VIEW query3_1negh;


-- Query 4 statements
CREATE VIEW q4_indirect AS
	-- Query to find countries with indirect access.
	SELECT N.country, O.oid
		FROM neighbour AS N, oceanAccess AS O
		WHERE (N.neighbor, O.oid) = (
			SELECT *
				FROM oceanAccess
				WHERE cid = N.neighbor AND oid = O.oid
		)
		GROUP BY N.country, O.oid;

CREATE VIEW q4_allAccess AS
	-- Query to combine countries with direct and indirect access.
	SELECT * FROM q4_indirect
	UNION
	SELECT * FROM oceanAccess;

INSERT INTO Query4 (
	-- Query to find the ocean names and country names.
	SELECT c.cname, o.oname
		FROM ocean AS o, country AS c
		WHERE (c.cid, o.oid) IN (
			SELECT * FROM q4_allAccess
		)
		GROUP BY c.cname, o.oname
		ORDER BY c.cname ASC, o.oname DESC
);
DROP VIEW q4_allAccess;
DROP VIEW q4_indirect;


-- Query 5 statements
INSERT INTO Query5 (
	SELECT h.cid, c.cname, AVG(h.hdi_score) AS avghdi
		FROM hdi AS h, country AS c
		WHERE
			h.year > 2008 AND
			h.year < 2014 AND
			c.cid = h.cid
		GROUP BY h.cid, c.cname
		ORDER BY avghdi DESC
);


-- Query 6 statements
CREATE VIEW q6_y1 AS
	-- Query for coloumns with first year positive
	SELECT b.cid, b.hdi_score
		FROM (
			SELECT cid, hdi_score
				FROM hdi
				WHERE year = 2009
		) AS a, (
			SELECT cid, hdi_score
				FROM hdi
				WHERE year = 2010
		) AS b
		WHERE
			a.cid = b.cid AND
			a.hdi_score < b.hdi_score;

CREATE VIEW q6_y2 AS
	-- Query for coloumns with second year positive
	SELECT b.cid, b.hdi_score
		FROM q6_y1 AS a, (
			SELECT cid, hdi_score
				FROM hdi
				WHERE year = 2011
		) AS b
		WHERE
			a.cid = b.cid AND
			a.hdi_score < b.hdi_score;

CREATE VIEW q6_y3 AS
	-- Query for coloumns with third year positive
	SELECT b.cid, b.hdi_score
		FROM q6_y2 AS a, (
			SELECT cid, hdi_score
				FROM hdi
				WHERE year = 2012
		) AS b
		WHERE
			a.cid = b.cid AND
			a.hdi_score < b.hdi_score;

CREATE VIEW q6_y4 AS
	-- Query for coloumns with third year positive
	SELECT b.cid, b.hdi_score
		FROM q6_y3 AS a, (
		  SELECT cid, hdi_score
			FROM hdi
			WHERE year = 2013
		) AS b
		WHERE
			a.cid = b.cid AND
			a.hdi_score < b.hdi_score;

-- Final Query with ordered elements
INSERT INTO Query6 (
	SELECT country.cid, cname
		FROM q6_y4, country
		WHERE country.cid = q6_y4.cid
		ORDER BY cname ASC
);


-- Query 7 statements
CREATE VIEW q7_follow AS
	-- Query to find the number of people following a religion
	SELECT rid, sum(population * rpercentage) AS pop
		FROM country AS c, religion AS r
		WHERE c.cid = r.cid
		GROUP BY rid;

-- Well.. yeah. same thing.
INSERT INTO Query7 (
	SELECT DISTINCT a.rid, rname, pop AS followers
		FROM religion AS a, q7_follow AS b
		WHERE a.rid = b.rid
		ORDER BY followers DESC
);
DROP VIEW q7_follow;

-- Query 8 statements
CREATE VIEW q8_mspoken AS
  -- Query to find the most spoken language
	SELECT a.cid, a.lname
	FROM language AS a
	WHERE a.lpercentage >= ALL (
		SELECT lpercentage
			FROM language
			WHERE a.cid = cid
	);

CREATE VIEW q8_mspokenpair AS
	-- Query to CREATE VIEWtry pairs
	SELECT a.cid AS c1id, b.cid AS c2id, a.lname
		FROM q8_mspoken AS a, q8_mspoken AS b
		WHERE a.lname = b.lname AND
			a.cid <> b.cid;

CREATE VIEW q8_lneigh AS
	-- Query to filter pair which are neighboring countries only.
	SELECT c1id, c2id, lname
		FROM q8_mspokenpair, neighbour
		WHERE c1id = country AND
		c2id = neighbor;

DROP VIEW q8_lneigh;
DROP VIEW q8_mspokenpair;
DROP VIEW q8_mspoken;


-- Query 9 statements
CREATE VIEW q9_deepest AS
	-- Query to find the deepest points of ocean with countries that have
	-- access.
	SELECT cid, max(depth) AS deep
	FROM ocean AS a, oceanAccess AS b
	WHERE a.oid = b.oid
	GROUP BY cid;

CREATE VIEW q9_dist AS
	-- Query to add the total span instead of just depth..
	SELECT cname, (height + deep) AS totalspan
		FROM q9_deepest AS a, country AS b
		WHERE (height + deep) >= ALL (
			SELECT (height + deep)
				FROM q9_deepest AS c, country AS d
				WHERE c.cid = d.cid
		) AND a.cid = b.cid;

CREATE VIEW q9_highest AS
	-- Query to find the highest span.
	SELECT cname, height AS totalspan
		FROM country
		WHERE height >= ALL (
			SELECT height
				FROM country
		);

INSERT INTO Query9 (
	SELECT *
		FROM q9_dist
		WHERE totalspan >= ALL (
			SELECT totalspan
				FROM q9_highest
		)
);

INSERT INTO Query9 (
	SELECT *
		FROM q9_highest
		WHERE totalspan >= ALL (
			SELECT totalspan
				FROM q9_dist
		)
);
DROP VIEW q9_highest;
DROP VIEW q9_dist;
DROP VIEW q9_deepest;


-- Query 10 statements
CREATE VIEW q10_sumlen AS
	-- Query to find the sum of border lengths
	SELECT country, sum(length) AS borderslength
		FROM neighbour
		GROUP BY country;

CREATE VIEW q10_maxlen AS
	-- Query to find the max ...
	SELECT country, borderslength
		FROM q10_sumlen
		WHERE borderslength >= ALL(
			SELECT borderslength FROM q10_sumlen
		);

INSERT INTO Query10(
	SELECT cname, borderslength
		FROM q10_maxlen, country
		WHERE country = cid
);
DROP VIEW q10_maxlen;
DROP VIEW q10_sumlen;