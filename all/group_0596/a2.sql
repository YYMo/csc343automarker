-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
	INSERT INTO Query1 (
		SELECT n.country AS c1id, t1.cname AS c1name, neighbor AS c2id, t2.cname AS c2name
		FROM neighbour n
		JOIN country t1 ON t1.cid=n.country
		JOIN country t2	ON t2.cid=n.neighbor,
				(SELECT country,MAX(t.height) AS max_elevation
				 FROM neighbour n
				 JOIN country t	ON t.cid=n.neighbor
				 GROUP BY country
				 ) AS x
		WHERE t2.height=x.max_elevation AND x.country = t1.cid
		ORDER BY c1name ASC
	);

-- Query 2 statements
	INSERT INTO Query2 (
		SELECT cid, cname
		FROM country
		WHERE cid NOT IN(
			SELECT cid FROM oceanAccess)
		ORDER BY cname ASC
	);

-- Query 3 statements
	INSERT INTO Query3 (
		SELECT c1.cid AS c1id, c1.cname AS  c1name, c2.cid AS c2id, c2.cname AS c2name
		FROM (
				SELECT country, COUNT (*) AS count
				FROM neighbour n, 
						(
						 SELECT cid, cname
						 FROM country
						 WHERE cid NOT IN(
							SELECT cid FROM oceanAccess)
						 )
					 AS x
				WHERE n.country = x.cid
				GROUP BY n.country
			 ) AS r 
			JOIN country c1  ON c1.cid = r.country
			JOIN neighbour n ON n.country = r.country
			JOIN country c2  ON c2.cid = n.neighbor
		WHERE r.count = 1 
		ORDER BY c1name ASC
	);
-- Query 4 statements
	INSERT INTO Query4 (
		SELECT cname,oname
		FROM (
				SELECT c.cname AS cname, oname
				FROM neighbour n
				JOIN country c      ON c.cid = n.country
				JOIN oceanAccess oa ON n.neighbor= oa.cid
				JOIN ocean o        ON o.oid =oa.oid
			 )AS t1
			 UNION 
			 (
				SELECT cname, oname
				FROM oceanAccess 
				JOIN ocean   ON  oceanAccess.oid = ocean.oid
				JOIN country ON  oceanAccess.cid = country.cid
			 )
		ORDER BY cname ASC, oname DESC
		);
		
-- Query 5 statements
	INSERT INTO Query5 (
		SELECT c.cid AS cid, cname, a.hdi_avg AS avghdi
		FROM (
				SELECT cid, AVG(hdi_score) AS hdi_avg
				FROM hdi
				WHERE year >=2009 AND year <=2013
				GROUP BY cid
				ORDER BY AVG(hdi_score)DESC LIMIT 10
			 )a , country c
		WHERE c.cid=a.cid
		ORDER BY avghdi DESC
	);
	
-- Query 6 statements
	INSERT INTO Query6 (
		SELECT c.cid, c.cname
		FROM (
		SELECT y1.cid 
		FROM ( SELECT cid, hdi_score
			   FROM hdi
			   WHERE year=2009) y1,
			 ( SELECT cid, hdi_score
			   FROM hdi
			   WHERE year=2010) y2,
			 ( SELECT cid, hdi_score
			   FROM hdi
			   WHERE year=2011) y3,
			 ( SELECT cid, hdi_score
			   FROM hdi
			   WHERE year=2012) y4,
			 ( SELECT cid, hdi_score
			   FROM hdi
			   WHERE year=2013) y5
		WHERE   y5.hdi_score>y4.hdi_score AND
				y4.hdi_score>y3.hdi_score AND
				y3.hdi_score>y2.hdi_score AND
				y2.hdi_score>y1.hdi_score AND
				y1.cid=y2.cid AND y2.cid=y3.cid AND
				y3.cid=y4.cid AND y4.cid=y5.cid
		)x
		JOIN country c ON  c.cid = x.cid
		ORDER BY cname ASC
);
-- Query 7 statements
INSERT INTO Query7 (
		SELECT x.rid, r.rname,followers
		FROM(
			SELECT rid, SUM(religion.rpercentage* population) AS followers
			FROM religion, country
			WHERE religion.cid=country.cid
			GROUP BY rid
			)x,
			religion r
		WHERE x.rid=r.rid
		GROUP BY x.rid, r.rname,followers
		ORDER BY followers DESC
		);


-- Query 8 statements
INSERT INTO Query8 (
		SELECT c1.cname AS c1name, c2.cname AS c2name, l1.lname
		FROM neighbour n
		JOIN (
				SELECT cid,MAX (lpercentage) AS max_percentage
				FROM language
				GROUP BY cid
			 )p1 ON n.country=p1.cid
		JOIN(
				SELECT cid, MAX (lpercentage) AS max_percentage
				FROM language
				GROUP BY cid
			 )p2 ON n.neighbor=p2.cid
		JOIN country c1 ON c1.cid=n.country
		JOIN country c2 ON c2.cid=n.neighbor
		JOIN language l1 ON l1.cid=p1.cid AND l1.lpercentage=p1.max_percentage
		JOIN language l2 ON l2.cid=p2.cid AND l2.lpercentage=p2.max_percentage
		WHERE l1.lname=l2.lname 
		ORDER BY l1.lname ASC, c1name DESC
		);
			 


-- Query 9 statements
INSERT INTO Query9 (
		SELECT cname, span AS totalspan
		FROM (
				SELECT cname, (c.height+o.depth) AS span
				FROM oceanAccess oa
				JOIN country c ON c.cid = oa.cid
				JOIN ocean o ON o.oid=oa.oid
			 ) AS t1
			 UNION
			 (
				SELECT cname, height AS span
				FROM country
				WHERE cid NOT IN(
					SELECT cid FROM oceanAccess)
			 )
		ORDER BY totalspan DESC LIMIT 1
		);
			 


-- Query 10 statements
	INSERT INTO Query10 (
				SELECT cname, SUM (length) AS  borderlength
				FROM neighbour n
				JOIN country c ON n.country = c.cid
				GROUP BY c.cname
				ORDER BY borderlength DESC LIMIT 1
				);

