-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1 (
	SELECT c1id,c1name,c2id,c2name FROM (
		SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name, c2.height AS c2height
		FROM country as c1, country as c2
		WHERE EXISTS (
			SELECT * FROM neighbour WHERE c1.cid=country AND c2.cid=neighbor
		)
	) AS nHeights1
	WHERE NOT EXISTS (
		SELECT * FROM (
			SELECT c1.cid AS c1id, c1.cname AS c1name, c2.cid AS c2id, c2.cname AS c2name, c2.height AS c2height
			FROM country as c1, country as c2
			WHERE EXISTS (
				SELECT * FROM neighbour WHERE c1.cid=country AND c2.cid=neighbor
			)
		) AS nHeights2 WHERE nHeights1.c1id=nHeights2.c1id AND nHeights1.c2height < nHeights2.c2height
	) ORDER BY c1name ASC
);

-- Query 2 statements
INSERT INTO Query2 (
	SELECT cid, cname FROM country WHERE NOT EXISTS (
		SELECT cid FROM oceanAccess WHERE country.cid=oceanAccess.cid
	)
	ORDER BY cname ASC
);

-- Query 3 statements!
INSERT INTO Query3 (
	SELECT c1id,c1name,neighbor AS c2id,cname AS c2name FROM (
		SELECT cid AS c1id, cname AS c1name  FROM (
			SELECT cid, cname FROM country WHERE NOT EXISTS (
				SELECT cid FROM oceanAccess WHERE country.cid=oceanAccess.cid
			)
		) AS landlocked
		WHERE NOT EXISTS (																				
			SELECT * FROM neighbour AS n1, neighbour AS n2 WHERE landlocked.cid=n1.country AND n1.country=n2.country AND n1.neighbor<>n2.neighbor
		)
	) AS subCountries,neighbour,country WHERE c1id=country AND cid=neighbor
	ORDER BY c1name ASC
);

-- Query 4 statements
INSERT INTO Query4 (
	(SELECT cname, oname from country,oceanAccess,ocean WHERE country.cid=oceanAccess.cid AND oceanAccess.oid=ocean.oid) 
		UNION DISTINCT 
	(SELECT cname, oname from country,neighbour,oceanAccess,ocean WHERE oceanAccess.cid=neighbour.country AND oceanAccess.oid=ocean.oid AND country.cid=neighbour.neighbor) 
		ORDER BY cname,oname
);
-- Query 5 statements
INSERT INTO Query5 (
	SELECT country.cid,cname,AVG(hdi_score) AS avghdi FROM country,hdi 
	WHERE country.cid=hdi.cid AND (year>=2009 AND year<=2013) 
	GROUP BY country.cid,cname ORDER BY AVG(hdi_score) DESC LIMIT 10
);

-- Query 6 statements
INSERT INTO Query6 (
	SELECT cid,cname FROM country WHERE NOT EXISTS(
		SELECT * FROM hdi AS hdi1,hdi AS hdi2 WHERE (
			country.cid=hdi1.cid 
			AND country.cid=hdi2.cid 
			AND hdi1.cid=hdi2.cid 
			AND hdi1.year > hdi2.year 
			AND hdi1.hdi_score<=hdi2.hdi_score
		)
	)
	ORDER BY cname ASC
);

-- Query 7 statements
INSERT INTO Query7 (
	SELECT rid,rname,SUM(population*rpercentage) AS followers 
	FROM country,religion 
	WHERE country.cid=religion.cid 
	GROUP BY rid,rname 
	ORDER BY followers DESC
);

-- Query 8 statements
INSERT INTO Query8 (
	SELECT c1.cname AS c1name, c2.cname AS c2name, l1.lname AS lname FROM neighbour, country AS c1, country AS c2,
	(SELECT cid, lid, lname FROM language AS l1 WHERE NOT EXISTS(SELECT * FROM language AS l2 WHERE l1.lid<>l2.lid AND l1.cid=l2.cid AND l1.lpercentage<l2.lpercentage)) AS l1,
	(SELECT cid, lid, lname FROM language AS l1 WHERE NOT EXISTS(SELECT * FROM language AS l2 WHERE l1.lid<>l2.lid AND l1.cid=l2.cid AND l1.lpercentage<l2.lpercentage)) AS l2 
	WHERE country=l1.cid AND neighbor=l2.cid AND l1.lid=l2.lid AND country=c1.cid AND neighbor=c2.cid
	ORDER BY l1.lname ASC, c1.cname DESC
);
-- Query 9 statements
INSERT INTO Query9 (
	SELECT cname,MAX(totalspan) AS totalspan FROM (
		(SELECT country.cname AS cname,(depth+height) AS totalspan FROM ocean,oceanAccess,country WHERE ocean.oid=oceanAccess.oid AND country.cid=oceanAccess.cid)
		UNION
		(SELECT cname,height AS totalspan FROM country WHERE NOT EXISTS(SELECT * FROM oceanAccess WHERE country.cid=oceanAccess.cid))
	) AS spans GROUP BY cname ORDER BY MAX(totalspan) DESC
);
-- Query 10 statements
INSERT INTO Query10 (
	SELECT cname,SUM(length) AS borderslength FROM neighbour,country 
	WHERE cid=country GROUP BY cname ORDER BY SUM(length) DESC LIMIT 1
);

