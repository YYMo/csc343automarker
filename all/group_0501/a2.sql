-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW q1ans as
SELECT C.cid, C.cname, country.cid as neighbourid, country.cname as neighbourname
FROM
	(SELECT country.cid, country.cname, H.height
	FROM
		(SELECT NI.cid, max(NI.height) as height
		FROM
			(SELECT neighbour.country as cid, country.cid as neighbourid, country.cname as neighbourname, country.height as height
			FROM country JOIN neighbour ON country.cid = neighbour.neighbor) NI
			GROUP BY NI.cid) H JOIN country ON H.cid = country.cid) C JOIN country on C.height = country.height;

insert into Query1 (select * from q1ans);

DROP VIEW IF EXISTS q1ans;

-- Query 2 statements
CREATE VIEW landlocked as
select cid, cname from  country 
where country.cid not in (select o.cid from oceanAccess o)
order by cname ASC;

insert into Query2 (select * from landlocked);

DROP VIEW IF EXISTS landlocked;

-- Query 3 statements
CREATE VIEW landlocked2 as
select cid, cname from  country 
where country.cid not in (select o.cid from oceanAccess o)
order by cname ASC;
 
--has only one neighbour
CREATE VIEW lonely as
select country from neighbour group by country having count(neighbor) = 1;

--landlocked countries with only one neighbour
CREATE VIEW landlockedLonely as
SELECT LL.country, LL.neighbor
FROM
	(select neighbour.country, neighbour.neighbor from neighbour JOIN landlocked2 ON landlocked2.cid = neighbour.country) LL JOIN
	lonely ON LL.country = lonely.country;

CREATE VIEW q3ans as
SELECT CI.c1id, CI.c1name, CI.c2id, country.cname as c2name
FROM
	(select landlockedLonely.country as c1id, country.cname as c1name, landlockedLonely.neighbor as c2id
	from landlockedLonely JOIN country ON landlockedLonely.country = country.cid) CI JOIN country ON CI.c2id = country.cid;

insert into Query3 (select * from q3ans);

DROP VIEW IF EXISTS landlocked2 CASCADE;
DROP VIEW IF EXISTS lonely CASCADE;

-- Query 4 statements
CREATE VIEW oceanBorderInfo as
SELECT oceanAccess.cid, ocean.oname
FROM oceanAccess JOIN ocean ON oceanAccess.oid = ocean.oid;

CREATE VIEW oceanAccessNeighbour as
SELECT neighbour.country as cid, oceanBorderInfo.oname
FROM oceanBorderInfo join neighbour ON neighbour.neighbor = oceanBorderInfo.cid;

CREATE VIEW q4ans as
(SELECT * FROM oceanBorderInfo)
UNION
(SELECT * FROM oceanAccessNeighbour);

insert into Query4 (select * from q4ans);

DROP VIEW IF EXISTS oceanBorderInfo CASCADE;

-- Query 5 statements
CREATE VIEW hdi0913 as
SELECT cid, hdi_score
FROM hdi
WHERE year >= 2009 AND year <= 2013;

CREATE VIEW avghdi as
SELECT cid, avg(hdi_score) as avghdi
FROM hdi0913
GROUP BY cid
ORDER BY avg(hdi_score) DESC LIMIT 10;

CREATE VIEW q5ans as
SELECT avghdi.cid, country.cname, avghdi.avghdi
FROM avghdi JOIN country ON avghdi.cid = country.cid;

insert into Query5 (select * from q5ans);

DROP VIEW IF EXISTS hdi0913 CASCADE;

-- Query 6 statements

CREATE VIEW c2009 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2009;

CREATE VIEW c2010 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2010;

CREATE VIEW c2011 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2011;

CREATE VIEW c2012 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2012;

CREATE VIEW c2013 as
SELECT cid, hdi_score
FROM hdi
WHERE year = 2013;


CREATE VIEW increasingids as
SELECT c2009.cid
FROM c2009 JOIN c2010 ON c2009.cid = c2010.cid 
		JOIN c2011 ON c2009.cid = c2011.cid 
		JOIN c2012 ON c2009.cid = c2012.cid
		JOIN c2013 ON c2009.cid = c2013.cid
WHERE c2009.hdi_score < c2010.hdi_score 
		AND c2010.hdi_score < c2011.hdi_score 
		AND c2011.hdi_score < c2012.hdi_score
		AND c2012.hdi_score < c2013.hdi_score;

CREATE VIEW increasingnames as
SELECT country.cid, country.cname
FROM increasingids JOIN country ON increasingids.cid = country.cid
ORDER BY country.cname ASC;

insert into Query6 (select * from increasingnames);

DROP VIEW IF EXISTS c2009 CASCADE;
DROP VIEW IF EXISTS c2010 CASCADE;
DROP VIEW IF EXISTS c2011 CASCADE;
DROP VIEW IF EXISTS c2012 CASCADE;
DROP VIEW IF EXISTS c2013 CASCADE;

-- Query 7 statements
CREATE VIEW countryprops as
SELECT country.cid, religion.rid, religion.rname, religion.rpercentage*country.population as followers
FROM religion JOIN country ON religion.cid = country.cid;

CREATE VIEW religionpops as
SELECT rid, rname, sum(followers) as followers
FROM countryprops 
GROUP BY rid, rname;

insert into Query7 (select * from religionpops);

DROP VIEW IF EXISTS countryprops CASCADE;
-- Query 8 statements

--most popular language of every country
CREATE VIEW langpercent as
SELECT cid, max(lpercentage) as lpercentage
FROM language 
GROUP BY cid;

--includes the name of the language
CREATE VIEW mostpoplang as
SELECT langpercent.cid, language.lname, langpercent.lpercentage
FROM langpercent JOIN language ON langpercent.cid = language.cid AND language.lpercentage = langpercent.lpercentage;

--includes the neighbours of each country
CREATE VIEW countrymostpoplang as
SELECT mostpoplang.cid, neighbour.neighbor, mostpoplang.lname, mostpoplang.lpercentage
FROM mostpoplang JOIN neighbour ON mostpoplang.cid = neighbour.country;

--Finds pairs of neighbouring countries where most popular languages are the same
CREATE VIEW pairs as 
SELECT countrymostpoplang.cid, countrymostpoplang.neighbor, countrymostpoplang.lname
FROM countrymostpoplang JOIN mostpoplang ON mostpoplang.cid = countrymostpoplang.neighbor AND mostpoplang.lname = countrymostpoplang.lname;

CREATE VIEW q8ans as
SELECT H.cname as c1name, country.cname as c2name, H.lname
FROM
	(SELECT country.cname, pairs.neighbor, pairs.lname
	FROM pairs JOIN country ON pairs.cid = country.cid) H JOIN country ON country.cid = H.neighbor;

insert into Query8 (select * from q8ans);

DROP VIEW IF EXISTS langpercent CASCADE;
-- Query 9 statements

--countries with ocean access..
CREATE VIEW countryAccess as
SELECT OI.cname, max(OI.totalspan) as totalspan
FROM (SELECT OA.cname, OA.height+ocean.depth as totalspan
	FROM (SELECT country.cname, country.height, oceanAccess.oid
		FROM country JOIN oceanAccess ON country.cid = oceanAccess.cid) OA join ocean ON OA.oid = ocean.oid) OI
GROUP BY OI.cname;

--countries without ocean access..
CREATE VIEW noAccess as
(SELECT cid
FROM country)
	EXCEPT
(SELECT cid
FROM oceanAccess);

CREATE VIEW q9ans as
(SELECT country.cname, country.height as totalspan FROM noAccess JOIN country ON noAccess.cid = country.cid)
	UNION
(SELECT * FROM countryAccess)
ORDER BY totalspan DESC LIMIT 1;

insert into Query9 (select * from q9ans);

DROP VIEW IF EXISTS countryAccess CASCADE;
DROP VIEW IF EXISTS noAccess CASCADE;

-- Query 10 statements
CREATE VIEW borderslength as
SELECT country, sum(length) as borderslength
FROM neighbour
GROUP BY country;

CREATE VIEW q10ans as
SELECT country.cname, borderslength.borderslength
FROM borderslength JOIN country ON country.cid = borderslength.country
ORDER BY borderslength.borderslength DESC LIMIT 1;

insert into Query10 (select * from q10ans);

DROP VIEW IF EXISTS borderslength CASCADE;
