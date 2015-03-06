-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
--countries and their neighbour ids
CREATE VIEW cnid AS
SELECT c.cid as c1id, 
		c.cname as c1name,
		n.neighbor as c2id
FROM country c, neighbour n
WHERE c.cid = n.country;

--countries with neighbour ids, names, and neighbour elevation
CREATE VIEW cnnameelev AS
SELECT n.c1id as c1id, 
		n.c1name as c1name,
		n.c2id as c2id, 
		c.cname as c2name,
		c.height as c2elev 
FROM cnid n, country c
WHERE n.c2id = c.cid;
				
--country, neighbour combinations where the elevation of the neighbour is less than some other neighbour of the country 
CREATE VIEW lowerthansome AS
SELECT e1.c1id as c1id,
		e1.c1name as c1name,
		e1.c2id as c2id,
		e2.c2name as c2name 
FROM  cnnameelev e1, cnnameelev e2
WHERE e1.c1id = e2.c1id AND e1.c2id != e2. c2id AND e1.c2elev < e2.c2elev
ORDER BY c1name ASC;

--all country/neighbour combinations
CREATE VIEW allcn AS
SELECT n.c1id as c1id, 
		n.c1name as c1name,
		n.c2id as c2id, 
		c.cname as c2name
FROM cnid n, country c
WHERE n.c2id = c.cid
ORDER BY c1name ASC;

--Set difference of all c/n combos and lower than some returns only c/n combinations with heighest elevation neighbouring countries
CREATE VIEW highest AS
(SELECT * FROM allcn) EXCEPT (SELECT * FROM lowerthansome);

INSERT INTO Query1 (SELECT * FROM highest);

DROP VIEW cnid CASCADE;

-- Query 2 statements
--all cids which are not in ocean access (landlocked countries)
CREATE VIEW landlocked AS
(SELECT cid from country) EXCEPT (SELECT cid from oceanaccess);

--get the names of the countries which don't have ocean access
CREATE VIEW landlockednames AS
SELECT c1.cid as cid, c1.cname as cname
FROM country c1, landlocked c2
WHERE c1.cid = c2.cid
ORDER BY cname ASC;

INSERT INTO Query2 (SELECT * FROM landlockednames);
DROP VIEW landlocked CASCADE;


-- Query 3 statements
--all cids which are not in ocean access (landlocked countries)
CREATE VIEW landlocked AS
(SELECT cid from country) EXCEPT (SELECT cid from oceanaccess);

--get the names of the countries which don't have ocean access
CREATE VIEW landlockednames AS
SELECT c1.cid as cid, c1.cname as cname
FROM country c1, landlocked c2
WHERE c1.cid = c2.cid;

--landlocked countries and their neighbors
CREATE VIEW llnn AS
SELECT l.cid as cid, l.cname as cname, n.neighbor as neighbor
FROM landlockednames l, neighbour n
WHERE l.cid  = n.country;

--landlocked countries with more than one neighbour
CREATE VIEW morethanonen AS
SELECT l1.cid as cid, l1.cname as cname, l1.neighbor as neighbor
FROM llnn l1, llnn l2
WHERE l1.cid = l2.cid AND l1.neighbor != l2.neighbor;

--landlocked countries with neighbors and the names of the neighbors
CREATE VIEW neighbournames AS
SELECT l.cid as c1id, l.cname as c1name,
	l.neighbor as c2id, c.cname as c2name 
FROM llnn l, country c
WHERE l.neighbor = c.cid;

--cids of countries with one one neighbour
CREATE VIEW exactlyonecids AS
(SELECT cid FROM llnn) EXCEPT (SELECT cid FROM morethanonen);

--information of the countries from the previous query
CREATE VIEW exactlyoneneigh AS
SELECT n.c1id as c1id, n.c1name as c1name,
		n.c2id as c2id, n.c2name as c2name 
FROM neighbournames n, exactlyonecids e
WHERE n.c1id = e.cid
ORDER BY c1name ASC;

INSERT INTO Query3 (SELECT * FROM exactlyoneneigh);
DROP VIEW landlocked CASCADE;


-- Query 4 statements
--all countries with their neighbors 
CREATE VIEW countneigh AS
SELECT c.cid as cid, c.cname as cname, n.neighbor as nid
FROM country c, neighbour n
WHERE c.cid = n.country;

--find countries which have ocean access directly or have a neighbour with ocean access
CREATE VIEW access AS
SELECT DISTINCT c.cname as cname, o.oid as oid
FROM countneigh c, oceanaccess o
WHERE c.cid = o.cid OR c.nid = o.cid;

--get the names of the oceans of the pairs found in the previous queries
CREATE VIEW oceannames AS
SELECT a.cname as cname, o.oname as oname
FROM access a, ocean o
WHERE a.oid = o.oid
ORDER BY cname ASC, oname DESC;

INSERT INTO Query4 (SELECT * FROM oceannames);
DROP VIEW countneigh CASCADE;

-- Query 5 statements
--get hdi info all between 2009 and 2013
CREATE VIEW hdibetween AS
SELECT *
FROM hdi 
WHERE year BETWEEN 2009 AND 2013;

--get average hdi for each country between 2009 and 2013
CREATE VIEW avgforcid AS
SELECT cid, AVG(hdi_score) as avghdi
FROM hdibetween
GROUP BY cid; 

--get the top 10 avghdis and the cids for each one
CREATE VIEW topten AS
SELECT *
FROM avgforcid
ORDER BY avghdi DESC
LIMIT 10;

CREATE VIEW toptennames AS
SELECT t.cid as cid, c.cname as cname, t.avghdi as avghdi 
FROM topten t, country c
WHERE t.cid = c.cid
ORDER BY avghdi DESC;

INSERT INTO Query5 (SELECT * FROM toptennames);
DROP VIEW hdibetween CASCADE;


-- Query 6 statements
--get hdi info all between 2009 and 2013
CREATE VIEW hdibetween AS
SELECT *
FROM hdi 
WHERE year BETWEEN 2009 AND 2013;
--has cid, year, and hdi_score

--find which have at least once decreased in hdi score from one year to the next
CREATE VIEW decreased AS
SELECT h1.cid as cid, h1.year as year, h1.hdi_score as hdi_score
FROM hdibetween h1, hdibetween h2
WHERE h1.cid = h2.cid AND h1.year < h2.year AND h2.hdi_score < h1.hdi_score;

--countries which have never decreased in hdi score from one year to the next
CREATE VIEW onlyincreased AS
(SELECT * from hdibetween) EXCEPT (SELECT * FROM decreased);

CREATE VIEW relevantinfo AS 
SELECT o.cid as cid, c.cname as cname
FROM onlyincreased o, country c
WHERE o.cid = c.cid 
ORDER BY cname ASC;

INSERT INTO Query6 (SELECT * FROM relevantinfo);
DROP VIEW hdibetween CASCADE;

-- Query 7 statements
--find all countries and religions in each
CREATE VIEW allcr AS
SELECT c.cid as cid, r.rid as rid, r.rpercentage as rpercentage, c.population as population
FROM country c, religion r
WHERE c.cid = r.cid;

--find number of followers given percentage per country
CREATE VIEW popfollow AS
SELECT cid, rid, (rpercentage*population) as pop_following
FROM allcr;

--group all the religions by rid and sum the population following for each country
CREATE VIEW rfollowers AS
SELECT rid, SUM(pop_following) as followers
FROM popfollow
GROUP BY rid;

--get the names for each religion
CREATE VIEW rnames AS
SELECT r1.rid as rid, r2.rname as rname, r1.followers as followers
FROM rfollowers r1, religion r2
WHERE r1.rid = r2.rid;

INSERT INTO Query7 (SELECT * FROM rnames);
DROP VIEW allcr CASCADE;


-- Query 8 statements
--get countries and their neighbours
CREATE VIEW candn AS
SELECT c.cid as cid, n.neighbor as nid
FROM country c, neighbour n
WHERE c.cid = n.country;

--get languages for each country which aren't the most popular (less popular than any other language in that country)
CREATE VIEW notmostpop AS
SELECT l1.cid as cid, l1.lid as lid
FROM language l1, language l2
WHERE l1.cid = l2.cid AND l1.lid != l2.lid AND l1.lpercentage < l2.lpercentage;

--get the most popular language for each country (cid, lid)
CREATE VIEW mostpop AS
(SELECT cid, lid FROM language) EXCEPT (SELECT * FROM notmostpop);

--most popular language for each country
CREATE VIEW cpoplang AS
SELECT c.cid as cid, l.lid as clid, c.nid as nid
FROM candn c, mostpop l
WHERE c.cid = l.cid;

--most popular language for the neighbour of each country
CREATE VIEW npoplang AS
SELECT c.cid as cid, c.clid as clid, c.nid as nid, l.lid as nlid
FROM cpoplang c, mostpop l
WHERE c.nid = l.cid;

--where countries and their neighbours have the same most popular language
CREATE VIEW sharedlang AS
SELECT cid, nid, clid as lid
FROM npoplang
WHERE cid != nid AND clid = nlid;

--just changing ids to names 
CREATE VIEW cnames AS
SELECT c.cname as c1name, s.nid as nid, s.lid as lid
FROM sharedlang s, country c
WHERE s.cid = c.cid;

CREATE VIEW nnames AS
SELECT c1.c1name as c1name, c2.cname as c2name, c1.lid as lid
FROM cnames c1, country c2 
WHERE c1.nid = c2.cid;

CREATE VIEW langname AS
SELECT n.c1name, n.c2name, l.lname as lname 
FROM nnames n, language l
WHERE n.lid = l.lid
ORDER BY lname ASC, c1name DESC;

INSERT INTO Query8 (SELECT * FROM langname);
DROP VIEW candn CASCADE;
DROP VIEW notmostpop CASCADE;

-- Query 9 statements
--countries with ocean access and the depth of the ocean
CREATE VIEW oceanvals AS
SELECT DISTINCT c.cid as cid, o2.depth as odepth
FROM country c, oceanaccess o1, ocean o2
WHERE c.cid = o1.cid AND o1.oid = o2.oid;

--countries with no ocean access
CREATE VIEW nooceanacc AS
(SELECT cid FROM country) EXCEPT (SELECT cid FROM oceanvals);

--set odepth 0 for all countries without access to an ocean
CREATE VIEW nooceanvals AS
SELECT DISTINCT cid, 0 as odepth
FROM nooceanacc;

--not max depth for countries with ocean access
CREATE VIEW notmaxdepth AS
SELECT o1.cid, o1.odepth 
FROM oceanvals o1, oceanvals o2
WHERE o1.cid = o2.cid AND o1.odepth < o2.odepth;

--max depth for each country with ocean access
CREATE VIEW maxdepth AS
(SELECT * FROM oceanvals) EXCEPT (SELECT * FROM notmaxdepth);

--union no access and access
CREATE VIEW odepths AS
(SELECT * FROM maxdepth) UNION (SELECT * FROM nooceanvals);
--cid, odepth

CREATE VIEW heights AS
SELECT o.cid, o.odepth, c.height 
FROM country c, odepths o
WHERE c.cid = o.cid;

CREATE VIEW maxtotalspan AS
SELECT cid, odepth+height as totalspan 
FROM heights 
ORDER BY totalspan DESC
LIMIT 1;

CREATE VIEW getname AS
SELECT c.cname, m.totalspan
FROM maxtotalspan m, country c
WHERE m.cid = c.cid;

INSERT INTO Query9 (SELECT * FROM getname);
DROP VIEW oceanvals CASCADE;


-- Query 10 statements

--find country,neighbour pairs and the length of the border to that neighbour 
--cid, border-length 
CREATE VIEW cnborder AS 
SELECT c.cid, n.length as border
FROM country c, neighbour n
WHERE c.cid = n.country;

--for each country, find the sum of it's borders to it's neighbours 
CREATE VIEW totalborders AS
SELECT cid, SUM(border) as borderslength
FROM cnborder 
GROUP BY cid;

--get the longest border length
CREATE VIEW longest AS
SELECT cid, borderslength 
FROM totalborders
ORDER BY borderslength DESC
LIMIT 1;

--get the name of the country with the longest border
CREATE VIEW result AS
SELECT c.cname, l.borderslength 
FROM country c, longest l
WHERE c.cid = l.cid;

INSERT INTO Query10 (SELECT * FROM result);
DROP VIEW cnborder CASCADE;



