-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1 (SELECT c1id, c1name, c2id, c2name 
					FROM (SELECT c1id as c1id, c2id as c2id, c1name as c1name, max(c1height) as c1height, cname as c2name
							FROM country, (SELECT country as c1id, neighbor as c2id, cname as c1name, height as c1height
											FROM neighbour, country
											WHERE neighbour.country=country.cid) c1Info
							WHERE country.cid = c1Info.c2id
							GROUP BY c1id) PairInfo
					ORDER BY c1name);



-- Query 2 statements
INSERT INTO Query2 (SELECT cid, cname
					FROM country
					WHERE cid NOT IN (SELECT cid FROM oceanAccess)
					ORDER BY cname);


-- Query 3 statements
INSERT INTO Query3 (SELECT c1id, c1name, c2id, cname as c2name
					FROM country, (SELECT c1id, c1name, neighbour.neighbor as c2id, count(neighbour.neighbor) as numNei
					FROM neighbour, (SELECT cid as c1id, cname as c1name
										FROM country
										WHERE cid NOT IN (SELECT cid FROM oceanAccess)) Landlocked
					WHERE neighbour.country = Landlocked.c1id
					GROUP BY c1id
					HAVING numNei = 1) OneNei
					WHERE c2id = cid
					ORDER BY c1name);


-- Query 4 statements
INSERT INTO Query4 (SELECT cname, oname
					FROM ocean, (SELECT cname, oid
						FROM  country, ((SELECT neighbor as cid, oid
										FROM oceanAccess, neighbour
										WHERE oceanAccess.cid = neighbour.country)
										UNION 
										(SELECT *
										FROM oceanAccess)) Ids
						WHERE Ids.cid = country.cid) Cids
					WHERE Cids.oid = ocean.oid);


-- Query 5 statements
INSERT INTO Query5 (SELECT Time.cid as cid, cname, avg(hdi_score) as avghdi
					FROM country, (SELECT cid, hdi_score
									FROM hdi
									WHERE year > 2008 AND year < 2014) Time
					WHERE country.cid = Time.cid
					GROUP BY Time.cid
					ORDER BY avghdi DESC
					LIMIT 10);


-- Query 6 statements
INSERT INTO Query6 (SELECT country.cid AS cid, cname
					FROM country, (SELECT cid
									FROM hdi
									WHERE cid NOT IN (SELECT T1.cid
												  FROM hdi T1, hdi T2
									              WHERE T1.cid = T2.cid AND T1.year > 2008 AND T1.year < 2014
									              AND T2.year > 2008 AND T2.year < 2014
									              AND T1.year > T2.year AND T1.hdi_score <= T2.hdi_score)) Ids
					WHERE country.cid = Ids.cid
					GROUP BY country.cid
					ORDER BY cname);


-- Query 7 statements
INSERT INTO Query7 (SELECT rid, rname, sum(follower) as followers
					FROM (SELECT (religion.rpercentage * country.population) as follower, rid, rname
							FROM religion, country
							WHERE religion.cid = country.cid) Partial
					GROUP BY rid
					ORDER BY followers DESC);


-- Query 8 statements
INSERT INTO Query8 (SELECT c1name, c2name, c1lname as lname
					FROM
						(SELECT country.cid as c1cid, cname as c1name, lname as c1lname, max(lpercentage) as maxp1
						FROM country NATURAL JOIN language
						GROUP BY country.cid) C1
						JOIN
						(SELECT country.cid as c2cid, cname as c2name, lname as c2lname, max(lpercentage) as maxp2
						FROM country NATURAL JOIN language
						GROUP BY country.cid) C2
						ON c1lname = c2lname AND c1cid <> c2cid
					WHERE EXISTS (
						SELECT *
						FROM neighbour
						WHERE c1cid = neighbour.country AND c2cid = neighbour.neighbor));


-- Query 9 statements
INSERT INTO Query9 (SELECT cname, max(maxSpan) AS totalspan
					FROM (SELECT cname, max(span) AS maxSpan
							FROM (SELECT country.cname, height, 
							            max(depth),
							                CASE WHEN max(depth) IS NULL THEN height
							                     ELSE max(depth)+height
							                END span
							            FROM oceanAccess JOIN ocean ON oceanAccess.oid = ocean.oid
							                 RIGHT JOIN country ON oceanAccess.cid = country.cid
							            GROUP BY country.cid) Allspan
							GROUP BY cname) Final);


-- Query 10 statements
INSERT INTO Query10 (SELECT cname, max(allBorderslength) AS borderslength
					FROM country, (SELECT sum(N1.length) AS allBorderslength, N1.country AS cid
									FROM neighbour N1
									GROUP BY N1.country) AllBorder
					WHERE country.cid = AllBorder.cid);

