-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

SET search_path TO a2;

-- Query 1 statements
CREATE VIEW highestNeighbour as
	SELECT
		c1.cid as c1id, c1.cname as c1name,
		c2.cid as c2id, c2.cname as c2name
	from country as c1, country as c2, neighbour as n,	
		(select c1.cid, max(c2.height) as maxheight
			from
				country as c1, country as c2, neighbour as n
			where
				(c1.cid = n.country) AND
				(c2.cid = n.neighbor)
			group by
			c1.cid) as c1max
	where
		(c1.cid = n.country) AND
		(c2.cid = n.neighbor) AND
		(c1.cid = c1max.cid) AND
		(c2.height = c1max.maxheight)
	order by c1.cname ASC;

DELETE FROM QUERY1 *;
INSERT INTO QUERY1(SELECT * from highestNeighbour);
DROP VIEW IF EXISTS highestNeighbour CASCADE;

-- Query 2 statements
CREATE VIEW notLandLocked AS 
	SELECT cid
		FROM
			country as c1
		WHERE c1.cid IN (SELECT cid FROM oceanAccess);

CREATE VIEW landLocked AS
	SELECT cid
		FROM
			country 
		EXCEPT (SELECT * from notLandLocked);

DELETE FROM Query2 *;
INSERT INTO Query2(
	SELECT country.cid as cid, cname
		FROM
			landLocked JOIN country
		ON landlocked.cid = country.cid);
DROP VIEW IF EXISTS landLocked;
DROP VIEW IF EXISTS notLandLocked;

-- Query 3 statements
CREATE VIEW notLandLocked AS 
	SELECT cid
		FROM
			country as c1
		WHERE c1.cid IN (SELECT cid FROM oceanAccess);

CREATE VIEW landLocked AS
	SELECT cid
		FROM
			country 
		EXCEPT (SELECT * from notLandLocked);



CREATE VIEW landLockedAlone AS 
	SELECT cid, count(neighbor) as ncount
		FROM
			landLocked as ll, neighbour as n
		WHERE
			ll.cid = n.country
		GROUP BY
			ll.cid
		HAVING count(neighbor)= 1;

CREATE VIEW landLockedAloneNeighbours AS
	SELECT
		c1.cid as c1id, c1.cname as c1name,
		c2.cid as c2id, c2.cname as c2name
	FROM
		country as c1, country as c2, neighbour as n,
		landLockedAlone as lla
	WHERE
		(lla.cid = c1.cid) AND
		(c1.cid = n.country) AND
		(c2.cid = n.neighbor)
	ORDER BY
		c1.cname ASC;



DELETE FROM Query3 *;

INSERT INTO Query3 (
	SELECT * FROM landLockedAloneNeighbours
);
DROP VIEW IF EXISTS landLockedAloneNeighbours;
DROP VIEW IF EXISTS landLockedAlone; 
DROP VIEW IF EXISTS landLocked;
DROP VIEW IF EXISTS notLandLocked;

-- Query 4 statements

CREATE VIEW oceanPortOneStep AS
	SELECT
		oa.cid as cid, n.neighbor as ncid, oa.oid
	FROM
		oceanAccess as oa, neighbour as n
	WHERE
		(oa.cid = n.country);

CREATE VIEW opos1 AS
	SELECT cid, oid
	FROM oceanPortOneStep;

CREATE VIEW opos2 AS
	SELECT ncid as cid, oid
	FROM oceanPortOneStep;

CREATE VIEW opos0 AS
		( SELECT * FROM opos1) UNION
		( SELECT * FROM opos2);



CREATE VIEW OceanPortOneStepPP AS
	SELECT
		c.cname as cname, o.oname as oname
	FROM
		opos0 as op, country as c, ocean as o
	WHERE
		(op.cid = c.cid) AND
		(o.oid = op.oid)
	ORDER BY
		cname ASC,
		oname DESC;

DELETE FROM Query4 *;

INSERT INTO Query4 (
	SELECT * FROM OceanPortOneStepPP
);

DROP VIEW IF EXISTS oceanPortOneStepPP;
DROP VIEW IF EXISTS opos0;
DROP VIEW IF EXISTS opos1;
DROP VIEW IF EXISTS opos2;
DROP VIEW IF EXISTS oceanPortOneStep;

-- Query 5 statements
CREATE VIEW countryAvgHDI AS
	SELECT
		c.cid as cid, AVG(h.hdi_score) as avghdi
	FROM
		country as c, hdi as h
	WHERE
		(c.cid = h.cid)
	GROUP BY
		c.cid;

CREATE VIEW countryAvgHDIPP AS
	SELECT
		c.cid as cid, c.cname as cname, chdi.avghdi as avghdi 
	FROM
		country as c, countryAvgHDI as chdi
	WHERE
		(c.cid = chdi.cid)
	ORDER BY chdi.avghdi DESC
	LIMIT 10;

DELETE FROM Query5 *;
INSERT INTO QUERY5(SELECT * FROM countryAvgHDIPP);
DROP VIEW IF EXISTS countryAvgHDIPP;
DROP VIEW IF EXISTS countryAvgHDI;

-- Query 6 statements
CREATE VIEW hdispan AS
	SELECT
		c.cid as cid, c.cname as cname
	FROM
		country as c,
		hdi as h0, hdi as h1, hdi as h2,
		hdi as h3, hdi as h4
	WHERE
		(c.cid = h0.cid) AND
		(c.cid = h1.cid) AND
		(c.cid = h2.cid) AND
		(c.cid = h3.cid) AND
		(c.cid = h4.cid) AND
		(h0.year = 2009) AND
		(h1.year = 2010) AND
		(h2.year = 2011) AND
		(h3.year = 2012) AND
		(h4.year = 2013) AND
		(h4.hdi_score > h3.hdi_score) AND
		(h3.hdi_score > h2.hdi_score) AND
		(h2.hdi_score > h1.hdi_score) AND
		(h1.hdi_score > h0.hdi_score)
	ORDER BY
		c.cname DESC;

DELETE FROM Query6 *;
INSERT INTO Query6 ( SELECT * from hdispan );
DROP VIEW IF EXISTS hdispan;
-- Query 7 statements

CREATE VIEW religionByPop AS
	SELECT
		r.rid as rid,
		SUM(c.population * r.rpercentage) as followers
	FROM
		country as c, religion as r
	WHERE
		(c.cid = r.cid)
	GROUP BY
		r.rid;

CREATE VIEW religionByPopPP AS
	SELECT DISTINCT
		rp.rid as rid, r.rname as rname,
		rp.followers as followers
	FROM
		religionByPop as rp, religion as r
	WHERE
		(rp.rid = r.rid)
	ORDER BY
		rp.followers DESC;

DELETE FROM Query7 *;
INSERT INTO Query7(SELECT * from religionByPopPP);
DROP VIEW IF EXISTS religionByPopPP;
DROP VIEW IF EXISTS religionByPop;

-- Query 8 statements
CREATE VIEW cMostPopLangPercent AS
	SELECT
		c.cid as cid, MAX(l.lpercentage) as maxp
	FROM
		country as c, language as l
	WHERE
		(c.cid = l.cid)
	GROUP BY
		c.cid;

CREATE VIEW cMostPopLang AS
	SELECT DISTINCT
		c.cid as cid, l.lid as lid
	FROM
		country as c, 
		cMostPopLangPercent as cp,
		language as l
	WHERE
		(c.cid = cp.cid) AND
		(c.cid = l.cid) AND
		(cp.cid = l.cid) AND
		(cp.maxp = l.lpercentage);

CREATE VIEW sharedPopLang AS
	SELECT
		n.country as cid,
		n.neighbor as nid,
		cp0.lid as lid
	FROM
		neighbour as n,
		cMostPopLang as cp0, 
		cMostPopLang as cp1
	WHERE
		(cp0.cid = n.country) AND
		(cp1.cid = n.neighbor) AND
		(cp0.lid = cp1.lid);

CREATE VIEW sharedPopLangPP AS
	SELECT DISTINCT
		c0.cname as c1name,
		c1.cname as c2name,
		l.lname as lname
	FROM
		sharedPopLang as spl,
		country as c0,
		country as c1,
		language as l
	WHERE
		(c0.cid = spl.cid) AND
		(c1.cid = spl.nid) AND
		(l.lid = spl.lid)
	ORDER BY
		l.lname ASC,
		c0.cname DESC;
		
		

DELETE FROM Query8 *;
INSERT INTO Query8 ( SELECT * FROM sharedPopLangPP);

DROP VIEW IF EXISTS sharedPopLangPP;
DROP VIEW IF EXISTS sharedPopLang;
DROP VIEW IF EXISTS cMostPopLang;
DROP VIEW IF EXISTS cMostPopLangPercent;

-- Query 9 statements
CREATE VIEW countryOcean AS
	SELECT
		c.cid, c.height, oa.oid
	FROM
		country as c LEFT JOIN oceanAccess as oa
	ON
		(c.cid = oa.cid);

CREATE VIEW countryOceanMtn AS
	SELECT
		co.cid as cid, 
      MAX(co.height) + MAX(COALESCE(o.depth, 0)) as totalspan
	FROM
		countryOcean as co LEFT JOIN ocean as o
	ON
		co.oid = o.oid
   GROUP BY
      co.cid;

CREATE VIEW LargestSpan AS
   SELECT
      c.cname as cname, cm.totalspan as totalspan
   FROM
      countryOceanMtn as cm, country as c
   WHERE
      (cm.cid = c.cid)
   ORDER BY
      totalspan DESC 
   LIMIT 1; 


DELETE FROM Query9 *;
INSERT INTO Query9( SELECT * FROM LargestSpan);      
DROP VIEW IF EXISTS LargestSpan;
DROP VIEW IF EXISTS countryOceanMtn;
DROP VIEW IF EXISTS countryOcean;

-- Query 10 statements

CREATE VIEW BorderLength AS
   SELECT
      n.country as cid, SUM(n.length) as borderslength
   FROM
      neighbour as n
   GROUP BY
      n.country
   ORDER BY
      borderslength DESC
   LIMIT 1;

CREATE VIEW BorderLengthPP AS
   SELECT
      c.cname as cname, blen.borderslength as borderslength
   FROM
      BorderLength as blen, country as c
   WHERE
      (blen.cid = c.cid);

DELETE FROM Query10 *;
INSERT INTO Query10 ( SELECT * FROM BorderLengthPP);
DROP VIEW IF EXISTS BorderLengthPP;
DROP VIEW IF EXISTS BorderLength;
