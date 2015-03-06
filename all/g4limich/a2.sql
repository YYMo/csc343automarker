-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the &quot;\i a2.sql&quot; command in psql to execute the SQL commands in this file.

-- Query 1 statements


CREATE VIEW neighbourheight as 
SELECT neighbour.country, neighbour.neighbor, country.height
FROM a2.neighbour, a2.country
WHERE country.cid = neighbour.neighbor; 

CREATE VIEW tallest as 
(SELECT neighbourheight.country as c1id, neighbourheight.neighbor as c2id
FROM neighbourheight) EXCEPT
(SELECT t1.country as c1id, t1.neighbor as c2id
FROM neighbourheight t1, neighbourheight t2
WHERE t1.country = t2.country and t1.neighbor != t2.neighbor and t1.height < t2.height);

CREATE VIEW countryonename as 
SELECT tallest.c1id, country.cname as c1name, tallest.c2id
FROM tallest, country
WHERE tallest.c1id = country.cid; 

CREATE VIEW countrytwoname as 
SELECT countryonename.c1id, countryonename.c1name, countryonename.c2id, country.cname as c2name 
FROM countryonename, country
WHERE countryonename.c2id = country.cid
ORDER BY c1name ASC; 

INSERT INTO Query1(SELECT * FROM countrytwoname);


DROP VIEW countrytwoname;
DROP VIEW countryonename;
DROP VIEW tallest;
DROP VIEW neighbourheight; 

-- Query 2 statements

CREATE VIEW nocean as
(SELECT cid 
FROM country) EXCEPT
(SELECT cid
FROM oceanAccess);

CREATE VIEW landlocked as 
SELECT country.cid, country.cname as cname
FROM country, nocean
WHERE country.cid = nocean.cid
ORDER BY cname ASC;

INSERT INTO Query2(SELECT * FROM landlocked);

DROP VIEW landlocked;
DROP VIEW nocean;


-- Query 3 statements

CREATE VIEW nocean as
(SELECT cid 
FROM country) EXCEPT
(SELECT cid
FROM oceanAccess);

CREATE VIEW borders as 
SELECT nocean.cid, neighbour.neighbor
FROM nocean, neighbour
WHERE nocean.cid = neighbour.country;

CREATE VIEW onlyone as 
(SELECT cid
FROM nocean) EXCEPT
(SELECT b1.cid
FROM borders b1, borders b2
WHERE b1.cid = b2.cid AND b1.neighbor != b2.neighbor);

CREATE VIEW idpair as 
SELECT onlyone.cid, neighbour.neighbor
FROM onlyone, neighbour 
WHERE onlyone.cid = neighbour.country;

CREATE VIEW lockedname as 
SELECT idpair.cid, country.cname, idpair.neighbor
FROM idpair, country
WHERE idpair.cid = country.cid;

CREATE VIEW surroundname as
SELECT lockedname.cid as c1id, lockedname.cname as c1name, lockedname.neighbor as c2id, country.cname as c2name
FROM lockedname, country 
WHERE lockedname.neighbor = country.cid
ORDER BY c1name ASC;

INSERT INTO Query3(SELECT * FROM surroundname);


DROP VIEW surroundname;
DROP VIEW lockedname;
DROP VIEW idpair;
DROP VIEW onlyone;
DROP VIEW borders;
DROP VIEW nocean;


-- Query 4 statements

CREATE VIEW allaccess as  
(SELECT neighbour.neighbor as cid, oceanAccess.oid
FROM neighbour, oceanAccess
WHERE neighbour.country = oceanAccess.cid) UNION 
(SELECT cid, oid 
FROM oceanAccess);

CREATE VIEW countryinfo as 
SELECT country.cname, allaccess.oid
FROM allaccess, country
WHERE allaccess.cid = country.cid;

CREATE VIEW allinfo as 
SELECT countryinfo.cname, ocean.oname
FROM countryinfo, ocean
WHERE countryinfo.oid = ocean.oid
ORDER BY cname ASC, oname DESC;

INSERT INTO Query4(SELECT * FROM allinfo);


DROP VIEW allinfo;
DROP VIEW countryinfo;
DROP VIEW allaccess;

-- Query 5 statements
CREATE VIEW top as 
SELECT cid, avg(hdi_score) as avghdi
FROM hdi
WHERE year >= 2009 AND year <= 2013
GROUP BY cid
ORDER BY avghdi DESC LIMIT 10;

CREATE VIEW countryname as 
SELECT country.cid, country.cname, top.avghdi
FROM top, country
WHERE top.cid = country.cid
ORDER BY avghdi DESC;

INSERT INTO Query5(SELECT * FROM countryname);

DROP VIEW countryname;
DROP VIEW top;


-- Query 6 statements

CREATE VIEW inclusive as 
SELECT cid, year, hdi_score
FROM hdi
WHERE year >= 2009 AND year <= 2013;

CREATE VIEW positive as 
(SELECT cid
FROM inclusive) EXCEPT 
(SELECT h1.cid
FROM inclusive h1, inclusive h2
WHERE h1.cid = h2.cid AND h1.year > h2.year AND h2.hdi_score > h1.hdi_score);


CREATE VIEW name as 
SELECT positive.cid, country.cname 
FROM country, positive
WHERE positive.cid = country.cid
ORDER BY cname ASC;

INSERT INTO Query6(SELECT * FROM name);


DROP VIEW name;
DROP VIEW positive;
DROP VIEW inclusive;
-- Query 7 statements

CREATE VIEW religions as 
SELECT rid, sum(rpercentage *
((SELECT population FROM country WHERE country.cid = religion.cid))) as followers
FROM religion
GROUP BY rid, rname
ORDER BY followers DESC;

CREATE VIEW religionnames as 
SELECT DISTINCT religions.rid, religion.rname, religions.followers
FROM religions, religion 
WHERE religions.rid = religion.rid
ORDER BY followers DESC;
INSERT INTO Query7(SELECT * FROM religionnames);

DROP VIEW religionnames;
DROP VIEW religions;
-- Query 8 statements

CREATE VIEW popular as
(SELECT cid, lid, lname
FROM language) EXCEPT
(SELECT l1.cid, l1.lid, l1.lname
FROM language l1, language l2
WHERE l1.cid = l2.cid AND l1.lid != l2.lid AND l1.lpercentage < l2.lpercentage);


CREATE VIEW pairs as
SELECT p1.cid as id1, p2.cid as id2, p1.lname 
FROM popular p1, popular p2 
WHERE p1.lid = p2.lid AND p1.cid != p2.cid; 

CREATE VIEW firstcountry as 
SELECT country.cname as c1name, pairs.id2, pairs.lname
FROM pairs, country 
WHERE pairs.id1 = country.cid;

CREATE VIEW secondcountry as 
SELECT firstcountry.c1name, country.cname as c2name, firstcountry.lname
FROM firstcountry, country
WHERE firstcountry.id2 = country.cid
ORDER BY lname ASC, c1name DESC;

INSERT INTO Query8(SELECT * FROM secondcountry);


DROP VIEW secondcountry;

DROP VIEW firstcountry;
DROP VIEW pairs;
DROP VIEW popular; 

-- Query 9 statements

CREATE VIEW nocean as
(SELECT cid 
FROM country) EXCEPT
(SELECT cid
FROM oceanAccess);

CREATE VIEW allAccess as 
SELECT oceanAccess.cid, oceanAccess.oid, ocean.depth
FROM oceanAccess, ocean 
WHERE oceanAccess.oid = ocean.oid; 

CREATE VIEW deepest as 
(SELECT cid, depth
FROM allAccess) EXCEPT
(SELECT a1.cid, a1.depth
FROM allAccess a1, allAccess a2
WHERE a1.cid = a2.cid AND a1.oid != a2.oid AND a1.depth < a2.depth);


CREATE VIEW allspan as 
(SELECT country.cid, country.cname, country.height as totalspan
FROM nocean, country
WHERE nocean.cid = country.cid) UNION 
(SELECT country.cid, country.cname, (country.height + deepest.depth) as totalspan
FROM deepest, country 
WHERE deepest.cid = country.cid);

CREATE VIEW mostspan as 
(SELECT cname, totalspan
FROM allspan) EXCEPT 
(SELECT all1.cname, all1.totalspan
FROM allspan as all1, allspan as all2
WHERE all1.cid != all2.cid and all1.totalspan < all2.totalspan);

INSERT INTO Query9(SELECT * FROM mostspan);


DROP VIEW mostspan;
DROP VIEW allspan;
DROP VIEW deepest;
DROP VIEW allAccess;
DROP VIEW nocean;


-- Query 10 statements

CREATE VIEW border as
SELECT country, sum(length) as total
FROM neighbour
GROUP BY country; 

CREATE VIEW longest as
(SELECT country, total 
FROM border) EXCEPT
(SELECT b1.country, b1.total
FROM border b1, border b2
WHERE b1.country != b2.country AND b1.total < b2.total);


CREATE VIEW answer as 
SELECT country.cname, longest.total as borderslength
FROM country, longest
WHERE country.cid = longest.country;

INSERT INTO Query10(SELECT * FROM answer);


DROP VIEW answer;
DROP VIEW longest; 
DROP VIEW border;







