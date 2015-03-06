-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW NeighbourHeight AS
	SELECT c1.cid c1id, c1.cname c1name, c2.cid c2id, c2.cname c2name, c2.height height
	FROM country c1, neighbour, country c2
	WHERE c1.cid=neighbour.country AND c2.cid=neighbour.neighbor;

INSERT INTO Query1 (
	SELECT maxn.c1id c1id, c1name, c2id, c2name
	FROM NeighbourHeight N, (SELECT c1id, max(height) height
							FROM NeighbourHeight GROUP BY c1id) maxn
	WHERE N.c1id=maxn.c1id AND N.height=maxn.height)
	ORDER BY 2
	);

DROP VIEW NeighbourHeight;

-- Query 2 statements
INSERT INTO Query2 (
	SELECT country.cid cid, country.cname cname
	FROM country, (SELECT cid FROM country
					WHERE cid NOT IN (SELECT cid FROM oceanAccess)) lock
	WHERE country.cid=lock.cid
	ORDER BY 2
	);

-- Query 3 statements
CREATE VIEW Landlocked AS
	SELECT country.cid, country.cname
	FROM country, (SELECT cid FROM country
					WHERE cid NOT IN (SELECT cid FROM oceanAccess)) lock
	WHERE country.cid=lock.cid;

CREATE VIEW SingleLandlocked AS
	SELECT Landlocked.cid, Landlocked.cname
	FROM Landlocked, (SELECT Landlocked.cid
						FROM Landlocked INNER JOIN neighbour ON (Landlocked.cid=neighbour.country)
						GROUP BY Landlocked.cid HAVING count(*)=1) single
	WHERE Landlocked.cid=single.cid;

INSERT INTO Query3 ( 
	SELECT SingleLandlocked.cid c1id, SingleLandlocked.cname c1name, country.cid c2id, country.cname c2name
	FROM SingleLandlocked INNER JOIN neighbour ON (SingleLandlocked.cid=neighbour.country)
							INNER JOIN country ON (neighbour.country=country.cid)
	ORDER BY 2
	);

DROP VIEW Landlocked;
DROP VIEW SingleLandlocked;

-- Query 4 statements
CREATE VIEW Accessible (cid, oid) AS
	oceanAccess UNION 
	(SELECT neighbour.neighbor, oceanAccess.oid
	 FROM oceanAccess INNER JOIN neighbour ON (oceanAccess.cid=neighbour.country)
	 );

INSERT INTO Query4 (
	SELECT country.cname cname, ocean.oname oname
	FROM Accessible, ocean, country
	WHERE Accessible.cid=country.cid AND Accessible.oid=ocean.oid
	ORDER BY cname ASC, oname DESC
);

DROP VIEW Accessible;

-- Query 5 statements
CREATE VIEW Average AS
	SELECT cid, avg(hdi_score) avghdi
	FROM hdi 
	WHERE year>=2009 AND year<=2013
	GROUP BY cid;

INSERT INTO Query5 (
	SELECT Average.cid cid, country.cname cname, Average.avghdi avghdi
	FROM Average INNER JOIN country ON (Average.cid=country.cid)
	ORDER BY avghdi DESC
	LIMIT 10
);

DROP VIEW Average;

-- Query 6 statements
CREATE VIEW FiveYear AS
	SELECT cid, year, hdi_score
	FROM hdi 
	WHERE year>=2009 AND year<=2013;

CREATE VIEW Decrease AS
	SELECT F1.cid
	FROM FiveYear F1 INNER JOIN FiveYear F2 ON (F1.cid=F2.cid)
	WHERE F1.year>F2.year AND F1.hdi_score<=F2.hdi_score;

INSERT INTO Query6 (
	SELECT cid, cname
	FROM country
	WHERE cid NOT IN (Decrease)
	ORDER BY 2
);

DROP VIEW FiveYear;
DROP VIEW Decrease;

-- Query 7 statements
CREATE VIEW Followers AS
	SELECT rid, sum(population*rpercentage) followers
	FROM country INNER JOIN religion ON (country.cid=religion.cid)
	GROUP BY rid;

INSERT INTO Query7 (
	SELECT Followers.rid rid, religion.rname rname, cast(round(Followers.followers, 0) AS Integer) followers
	FROM Followers INNER JOIN religion ON (Followers.rid=religion.rid)
	ORDER BY 3 DESC
);

DROP VIEW Followers;

-- Query 8 statements
CREATE VIEW Popular AS
	SELECT language.cid, language.lname
	FROM language INNER JOIN (SELECT cid, max(lpercentage) max
								FROM language GROUP BY cid) top 
					ON (language.cid=top.cid AND language.lpercentage=top.max);

CREATE VIEW NeighbourLang AS
	SELECT neighbour.country, neighbour.neighbor, P1.lname
	FROM Popular P1 INNER JOIN neighbour ON (P1.cid=neighbour.country)
					INNER JOIN Popular P2 ON (P2.cid=neighbour.neighbor)
	WHERE P1.lname=P2.lname AND neighbour.country>neighbour.neighbor;

INSERT INTO Query8 (
	SELECT C1.cname c1name, C2.cname c2name, lname
	FROM country C1 INNER JOIN NeighbourLang ON (C1.cid=NeighbourLang.country)
					INNER JOIN country C2 ON (C2.cid=NeighbourLang.neighbor)
	ORDER BY lname, c1name DESC
);

DROP VIEW Popular;
DROP VIEW NeighbourLang;

-- Query 9 statements
CREATE VIEW LandlockedSpan AS
	SELECT country.cname, country.height span
	FROM country, (SELECT cid FROM country
					WHERE cid NOT IN (SELECT cid FROM oceanAccess)) lock
	WHERE country.cid=lock.cid;

CREATE VIEW MaxAdjacentOcean AS
	SELECT country.cname, max(ocean.depth)+max(country.height) span
	FROM country, oceanAccess, ocean
	WHERE country.cid=oceanAccess.cid AND oceanAccess.oid=ocean.oid
	GROUP BY country.cname;

INSERT INTO Query9 (
	SELECT cname, span totalspan
	FROM LandlockedSpan
	UNION
	SELECT cname, span totalspan
	FROM MaxAdjacentOcean
);

DROP VIEW LandlockedSpan;
DROP VIEW MaxAdjacentOcean;

-- Query 10 statements
CREATE VIEW Border AS
	SELECT country.cname cname, sum(length) borderslength
	FROM neighbour INNER JOIN country ON (neighbour.country=country.cid)
	GROUP BY country.cname;

INSERT INTO Query10 (
	SELECT cname, borderslength
	FROM Border
	WHERE borderslength IN (SELECT max(borderslength)
							FROM Border)
);

DROP VIEW Border;

