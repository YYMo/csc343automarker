-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.


-- Query 1 statements
INSERT INTO Query1 SELECT m.country AS c1id, cname AS c1name, neighbor AS c2id, nname AS c2name FROM 
	(SELECT country, MAX(height) AS maxH FROM
		(SELECT country, n.height FROM neighbour 
		JOIN country n ON neighbor = n.cid 
		JOIN country b ON country = b.cid) e  
	GROUP BY country) m 
JOIN 
	(SELECT country, b.cname, neighbor, n.cname AS nname, n.height FROM neighbour 
	JOIN country n ON neighbor = n.cid 
	JOIN country b ON country= b.cid
) w ON m.country = w.country AND maxh = height 
ORDER BY c1name ASC;


-- Query 2 statements
INSERT INTO Query2 SELECT cid, cname FROM country WHERE cid NOT IN 
	(SELECT cid FROM oceanaccess) 
ORDER BY cname ASC;



-- Query 3 statements
INSERT INTO Query3 SELECT cid, cname FROM country JOIN
	(SELECT country, num FROM 
		(SELECT country, COUNT(*) AS num FROM neighbour GROUP BY country) e 
	WHERE num = 1) n
ON cid = n.country WHERE (cid NOT IN (SELECT cid FROM oceanaccess)) ORDER BY cname ASC;


-- Query 4 statements

INSERT INTO Query4 SELECT DISTINCT cname, oname FROM
	(SELECT DISTINCT neighbor AS cid, oid FROM 
		oceanAccess o JOIN neighbour n ON cid = country 
		UNION ALL 
		SELECT cid, oid  FROM oceanAccess) a 
JOIN country c ON a.cid = c.cid 
JOIN ocean n ON a.oid = n.oid 
ORDER BY cname, oname ASC;

-- Query 5 statements

INSERT INTO Query5 SELECT country.cid, cname, avghdi FROM 
	(Select cid, AVG(hdi_score) AS avghdi FROM 
		(SELECT cid, hdi_score FROM hdi WHERE year >= 2009 AND year <= 2013) per 
	GROUP BY cid 
	ORDER BY avghdi DESC) avgHs 
JOIN country ON avgHs.cid = country.cid LIMIT 10;

-- Query 6 statements
INSERT INTO Query6 SELECT adv.cid, CO.cname FROM 
	(SELECT a.cid, a.hdi_score, b.hdi_score, c.hdi_score, d.hdi_score, e.hdi_score FROM 
		(SELECT cid, hdi_score FROM hdi WHERE year = 2009) a JOIN 
		(SELECT cid, hdi_score FROM hdi WHERE year = 2010) b ON a.cid = b.cid JOIN 
		(SELECT cid, hdi_score FROM hdi WHERE year = 2011) c ON a.cid = c.cid JOIN 
		(SELECT cid, hdi_score FROM hdi WHERE year = 2012) d ON a.cid = d.cid JOIN 
		(SELECT cid, hdi_score FROM hdi WHERE year = 2013) e ON a.cid = e.cid 
	WHERE a.hdi_score < b.hdi_score AND b.hdi_score < c.hdi_score AND c.hdi_score < d.hdi_score AND d.hdi_score < e.hdi_score) adv 
JOIN country CO ON adv.cid = CO.cid ORDER BY CO.cname;

-- Query 7 statements

INSERT INTO Query7 SELECT DISTINCT dr.rid, rname, followers FROM 
	(SELECT rid, SUM(numDevout) AS followers FROM
		(SELECT rid, rname, (rpercentage*population/100) AS numDevout FROM religion r JOIN country c ON r.cid = c.cid) dev
	GROUP BY rid) dr join
religion rr ON dr.rid = rr.rid ORDER BY followers DESC;

-- Query 8 statements

INSERT INTO Query8 SELECT DISTINCT c1.cname AS c1name, c2.cname AS c2name, l1.lname AS lname FROM
	(SELECT country, neighbor, x.lid AS lid1, y.lid AS lid2 FROM neighbour n JOIN
		(SELECT l.cid, lid FROM language l JOIN 
			(SELECT cid, MAX(lpercentage) AS lp FROM language GROUP BY cid) maxLP 
		ON l.cid = maxLP.cid AND lp = lpercentage) x 
	ON country = x.cid JOIN 
		(SELECT l.cid, lid FROM language l JOIN 
			(SELECT cid, MAX(lpercentage) AS lp FROM language GROUP BY cid) maxLP 
		ON l.cid = maxLP.cid AND lp = lpercentage) y 
	ON neighbor = y.cid) z
JOIN country c1 ON country = c1.cid
JOIN country c2 ON neighbor = c2.cid
JOIN language l1 ON lid1 = l1.lid
WHERE lid1 = lid2 ORDER BY lname ASC, c1name DESC;



-- Query 9 statements
INSERT INTO Query9 SELECT cname, (height + baseLV) AS totalspan FROM
	(SELECT a.cid, MAX(depth) as baseLV FROM oceanAccess a JOIN ocean o ON o.oid = a.oid GROUP BY a.cid 
	UNION ALL SELECT cid, (0) as baseLV FROM country WHERE cid NOT IN 
		(SELECT cid FROM oceanaccess)
	) ocl
JOIN country CO ON ocl.cid = CO.cid;


-- Query 10 statements
INSERT INTO Query10 SELECT cname, borderslength FROM
	(SELECT country, SUM(length) AS borderslength FROM neighbour GROUP BY country) b
JOIN country c ON c.cid = b.country;

COMMIT;

