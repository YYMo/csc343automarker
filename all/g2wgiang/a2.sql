-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1(
SELECT country.cid as c1name,
	   country.cname as c1name,
	   neighbor.cid as c2id,
	   neighbor.cname as c2name

FROM

(SELECT Y.country,
	   Y.neighbor
FROM

(SELECT * FROM NEIGHBOUR)Y

INNER JOIN

(SELECT country, 
	   MAX(length) AS Highest 
	   FROM NEIGHBOUR 
	   GROUP BY country
	   ORDER BY country ASC)x 

ON X.country = Y.country AND X.Highest = Y.length)cid

LEFT OUTER JOIN

(SELECT cid, cname FROM COUNTRY)country ON country.cid = cid.country

LEFT OUTER JOIN

(SELECT cid, cname FROM COUNTRY)neighbor on neighbor.cid = cid.neighbor);


-- Query 2 statements
INSERT INTO Query2
(SELECT cid,
	   cname
	   FROM COUNTRY
	   WHERE cid IN (SELECT country FROM NEIGHBOUR)
	   AND cid NOT IN (SELECT cid FROM OceanAccess)
	   ORDER BY cname ASC);

-- Query 3 statements
INSERT INTO Query3
(SELECT landlock.cid as c1id,
		landlock.cname as c1name,
		n.neighbor as c2id,
		name.cname as c2name
FROM
(SELECT cid,
		cname
	   FROM COUNTRY
	   WHERE cid IN (SELECT country FROM NEIGHBOUR)
	   AND cid NOT IN (SELECT cid FROM OceanAccess)
	   AND cid IN (SELECT COUNTRY FROM
(SELECT country, count(country) 
	FROM NEIGHBOUR GROUP BY country)x WHERE count = 1))landlock

LEFT OUTER JOIN

(SELECT country,
	   neighbor
	   FROM NEIGHBOUR)n ON landlock.cid = n.country

LEFT OUTER JOIN

(SELECT cid
		,cname
		FROM COUNTRY)name ON name.cid = n.neighbor

ORDER BY c1name ASC);


-- Query 4 statements
INSERT INTO Query4(SELECT country.cname,
		ocean.oname

FROM

(SELECT cid, oid FROM OceanAccess)id

LEFT OUTER JOIN

(select cid, cname FROM country)country ON country.cid = id.cid

LEFT OUTER JOIN

(select oid, oname FROM ocean)ocean ON ocean.oid = id.oid

UNION


SELECT c.cname
		,x.oname 

FROM

(SELECT country, neighbor FROM NEIGHBOUR)n

LEFT OUTER JOIN

(SELECT cid, cname FROM COUNTRY)c on n.country = c.cid

INNER JOIN

		(SELECT country.cid,
				country.cname,
				ocean.oid,
				ocean.oname

		FROM

		(SELECT cid, oid FROM OceanAccess)id

		LEFT OUTER JOIN

		(select cid, cname FROM country)country ON country.cid = id.cid

		LEFT OUTER JOIN

		(select oid, oname FROM ocean)ocean ON ocean.oid = id.oid)x
		ON x.cid = n.neighbor

		ORDER BY cname ASC, oname DESC);


-- Query 5 statements
INSERT INTO Query5(SELECT x.cid,
	   y.cname,
	   avghdi

FROM
(SELECT cid, AVG(hdi_score) AS avghdi 
FROM hdi WHERE year >= 2009 AND year <= 2013 
GROUP BY cid ORDER BY avghdi DESC limit 10)x

LEFT OUTER JOIN

(SELECT cid, cname FROM COUNTRY)y ON x.cid = y.cid);

-- Query 6 statements
INSERT INTO Query6(SELECT core.cid,
		x.cname

FROM
(SELECT a.cid
FROM
(SELECT cid, hdi_score as "2009" FROM HDI WHERE year = '2009')a
INNER JOIN
(SELECT cid, hdi_score as "2010" FROM HDI WHERE year = '2010')b
ON a.cid = b.cid INNER JOIN 
(SELECT cid, hdi_score as "2011" FROM HDI WHERE year = '2011')c
ON a.cid = c.cid INNER JOIN 
(SELECT cid, hdi_score as "2012" FROM HDI WHERE year = '2012')d
ON a.cid = d.cid INNER JOIN 
(SELECT cid, hdi_score as "2013" FROM HDI WHERE year = '2013')e
ON a.cid = e.cid
WHERE "2013" > "2012"
	AND "2012" > "2011"
	AND "2011" > "2010"
	AND "2010" > "2009")core
LEFT OUTER JOIN

(SELECT * FROM COUNTRY)x on x.cid = core.cid
ORDER BY cname ASC);


-- Query 7 statements
INSERT INTO Query7(SELECT rid
		,rname
		,sum(followers) AS followers

		FROM 
(SELECT y.cid
		,y.rid
		,y.rname
		,population * rpercentage as "followers"

FROM

(SELECT * FROM COUNTRY)x

LEFT OUTER JOIN

(SELECT * FROM RELIGION)y

ON x.cid = y.cid)x

GROUP BY rid,rname
ORDER BY followers DESC);


-- Query 8 statements
INSERT INTO Query8(SELECT x.cname as c1name,
	   y.cname as c2name,
	   x.lname

FROM

(SELECT y.cname
	,x.lname

FROM

(SELECT x.cid
		,lname

FROM 

(SELECT cid
		,lname
		,lpercentage
FROM language)x

INNER JOIN

(SELECT cid
		,max(lpercentage) as lpercentage
FROM language
group by cid, lname)y

ON x.cid = y.cid and x.lpercentage = y.lpercentage)x

LEFT OUTER JOIN

(SELECT * FROM COUNTRY)y ON x.cid = y.cid)x

INNER JOIN

(SELECT y.cname
	,x.lname

FROM

(SELECT x.cid
		,lname

FROM 

(SELECT cid
		,lname
		,lpercentage
FROM language)x

INNER JOIN

(SELECT cid
		,max(lpercentage) as lpercentage
FROM language
group by cid, lname)y

ON x.cid = y.cid and x.lpercentage = y.lpercentage)x

LEFT OUTER JOIN

(SELECT * FROM COUNTRY)y ON x.cid = y.cid)y ON x.lname = y.lname

WHERE x.cname <> y.cname and x.lname = y.lname
ORDER BY lname ASC, c1name DESC);

-- Query 9 statements
INSERT INTO Query9(SELECT cname
		,CASE WHEN DEPTH IS NULL THEN height - 0
			  ELSE height - depth END as "totalspan"

FROM

(SELECT * FROM Country)core

LEFT OUTER JOIN


(SELECT x.cid,
		y.depth * -1 as "depth"

FROM

(SELECT x.cid, x.oid, y.depth FROM OceanAccess x

LEFT OUTER JOIN (SELECT * FROM Ocean)y ON x.oid = y.oid)x

INNER JOIN

(SELECT x.cid, max(y.depth) as depth FROM OceanAccess x

LEFT OUTER JOIN (SELECT * FROM Ocean)y ON x.oid = y.oid group by x.cid)y

ON x.cid = y.cid and x.depth = y.depth)ocean

ON ocean.cid = core.cid
ORDER BY totalspan DESC limit 1);


-- Query 10 statements
INSERT INTO QUERY10(SELECT cname
		,sum as "borderslength"

FROM
(SELECT country, sum(length) 
FROM NEIGHBOUR GROUP BY Country)x

LEFT OUTER JOIN

(SELECT * FROM COUNTRY)y
ON y.cid = x.country
ORDER BY borderslength DESC LIMIT 1);

