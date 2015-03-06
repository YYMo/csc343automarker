-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.



-- Query 1 statements
INSERT INTO Query1
(select c1id, country.cname as c1name, c2id, c2name
from (select country as c1id, neighbor as c2id, cname as c2name
	  from (neighbour 
			join country
			on neighbor = cid) temp
	  where height = (select max(height)
					   from (neighbour 
							 join country
							 on neighbor = cid) temp2
					   where temp2.country = temp.country)) country_2 , country
where c1id = cid
order by c1name ASC
);


-- Query 2 statements
INSERT INTO Query2
(select country.cid, country.cname
from country,
	 (select cid
	 from country
 	 except
	 select cid
	 from oceanAccess) landlockid
where landlockid.cid = country.cid
order by country.cname ASC
);


-- Query 3 statements
INSERT INTO Query3
(select requiredN2.c1id, requiredN2.c1name, requiredN2.c2id, country.cname as c2name
from country,
	(select requiredN.c1id, country.cname as c1name, requiredN.c2id
	from country,
		(select country as c1id, neighbor as c2id
		from neighbour,
			(select landlock.cid 
			from
				(select country as cid
				from neighbour
				group by cid
				having count(neighbour) = 1) oneneighbor
				,
				(select cid
				from country 
				except
				select cid
				from oceanAccess) landlock
			where oneneighbor.cid = landlock.cid) requiredId
		where neighbour.country = requiredId.cid) requiredN
	where country.cid = requiredN.c1id) requiredN2
where country.cid = requiredN2.c2id
order by requiredN2.c1id ASC
);


-- Query 4 statements
INSERT INTO Query4
((select country.cname, ocean.oname
from oceanAccess, country, ocean
where oceanAccess.cid = country.cid
	and oceanAccess.oid = ocean.oid)
union
(select country.cname, ocean.oname
from country, ocean, 
	(select neighbour.country as cid, oceanAccess.oid
	from neighbour, oceanAccess
	where neighbour.neighbor = oceanAccess.cid) NAccess
where country.cid = NAccess.cid
	AND ocean.oid = NAccess.oid)
order by cname ASC, oname DESC
);


-- Query 5 statements
INSERT INTO Query5
(select ahdi.cid, country.cname, ahdi.avghdi
from country, 
	(select cid, avg(hdi_score) as avghdi
	from
		(select cid, hdi_score
		from hdi
		where year >2008 and year < 2014) hdi_year
	group by cid) ahdi
where country.cid = ahdi.cid
order by avghdi DESC 
limit 10
);


-- Query 6 statements
CREATE VIEW Y2010_Y2009(cid) AS
SELECT hdi_help1.cid 
FROM (select cid, hdi_score
		FROM hdi
		WHERE hdi.year = 2009) AS hdi_help1,
	  (select cid, hdi_score
		FROM hdi
		WHERE hdi.year = 2010) AS hdi_help2
WHERE hdi_help1.cid = hdi_help2.cid AND hdi_help2.hdi_score > hdi_help1.hdi_score;

CREATE VIEW Y2011_Y2010(cid) AS
SELECT hdi_help1.cid 
FROM (select cid, hdi_score
		FROM hdi
		WHERE hdi.year = 2010) AS hdi_help1,
	  (select cid, hdi_score
		FROM hdi
		WHERE hdi.year = 2011) AS hdi_help2
WHERE hdi_help1.cid = hdi_help2.cid AND hdi_help2.hdi_score > hdi_help1.hdi_score;


CREATE VIEW Y2012_Y2011(cid) AS
SELECT hdi_help1.cid 
FROM (select cid, hdi_score
		FROM hdi
		WHERE hdi.year = 2011) AS hdi_help1,
	  (select cid, hdi_score
		FROM hdi
		WHERE hdi.year = 2012) AS hdi_help2
WHERE hdi_help1.cid = hdi_help2.cid AND hdi_help2.hdi_score > hdi_help1.hdi_score;


CREATE VIEW Y2013_Y2012(cid) AS
SELECT hdi_help1.cid 
FROM (select cid, hdi_score
		FROM hdi
		WHERE hdi.year = 2012) AS hdi_help1,
	  (select cid, hdi_score
		FROM hdi
		WHERE hdi.year = 2013) AS hdi_help2
WHERE hdi_help1.cid = hdi_help2.cid AND hdi_help2.hdi_score > hdi_help1.hdi_score;

INSERT INTO Query6
(
SELECT country.cid ,cname
FROM country,
			(SELECT cid from Y2012_Y2011
			INTERSECT
			SELECT cid from Y2011_Y2010
			INTERSECT
			SELECT cid from Y2013_Y2012
			INTERSECT
			SELECT cid from Y2010_Y2009) AS CID
WHERE CID.cid = country.cid
ORDER BY cname ASC
);

DROP view Y2012_Y2011;
DROP view Y2013_Y2012;
DROP view Y2011_Y2010;
DROP view Y2010_Y2009;

-- Query 7 statements
INSERT INTO Query7
(
SELECT DISTINCT religion.rid, rname, followers
FROM religion,
(SELECT rid, sum(rpercentage*population) AS followers
 FROM country, religion
 WHERE country.cid = religion.cid
 Group by rid) AS follower
 WHERE follower.rid = religion.rid
 ORDER by followers DESC
 );

-- Query 8 statements
CREATE VIEW country_with_most_popular_language(cid,lid) AS
SELECT language.cid ,language.lid 
FROM language
EXCEPT
SELECT language.cid ,language.lid 
FROM language, language language1
WHERE language.cid = language1.cid AND language1.lpercentage > language.lpercentage ;

CREATE VIEW country_with_same_most_popular_language(c1cid,c2cid,lid) AS
SELECT (country_with_most_popular_language.cid) AS c1cid, 
(country_with_most_popular_language1.cid) AS c2cid,
(country_with_most_popular_language.lid) AS lid
FROM country_with_most_popular_language, country_with_most_popular_language country_with_most_popular_language1
WHERE country_with_most_popular_language.lid = country_with_most_popular_language1.lid;

CREATE VIEW neighbour_country_with_same_most_popular_language(c1cid,c2cid,lid) AS
SELECT country_with_same_most_popular_language.c1cid, 
country_with_same_most_popular_language.c2cid, 
country_with_same_most_popular_language.lid
FROM neighbour,country_with_same_most_popular_language
WHERE neighbour.country = country_with_same_most_popular_language.c1cid 
AND neighbour.neighbor = country_with_same_most_popular_language.c2cid;

CREATE VIEW cid_correspond_to_name(cid, cid1, c1name, c2name) AS
SELECT country.cid, (country1.cid) AS cid1, (country.cname) AS cname, 
(country1.cname) AS c2name
FROM country,country country1;

CREATE VIEW tuple_with_country_name_and_lid(c1name,c2name,lid) AS
SELECT cid_correspond_to_name.c1name,
cid_correspond_to_name.c2name,
neighbour_country_with_same_most_popular_language.lid
FROM neighbour_country_with_same_most_popular_language, cid_correspond_to_name
WHERE neighbour_country_with_same_most_popular_language.c1cid = cid_correspond_to_name.cid
AND neighbour_country_with_same_most_popular_language.c2cid = cid_correspond_to_name.cid1;

INSERT INTO Query8
(
SELECT DISTINCT c1name, c2name, lname 
FROM tuple_with_country_name_and_lid, language
WHERE tuple_with_country_name_and_lid.lid = language.lid
ORDER BY lname ASC, c1name DESC

);
DROP VIEW tuple_with_country_name_and_lid;
DROP VIEW cid_correspond_to_name;
DROP VIEW neighbour_country_with_same_most_popular_language;
DROP VIEW country_with_same_most_popular_language;
DROP VIEW country_with_most_popular_language;


-- Query 9 statements

INSERT INTO Query9
(select country.cname, ct.totalspan
from country,
	(select cid, (height + maxd) as totalspan
	from (select oceanAccess.cid, country.height, max(ocean.depth) as maxd
			from oceanAccess, country, ocean
			where country.cid = oceanAccess.cid
				and ocean.oid = oceanAccess.oid
			group by oceanAccess.cid, country.height) ocountryhd
	union
	(select cid, height as totalspan
	from country
	except 
	select cid, height
	from (select oceanAccess.cid, country.height, max(ocean.depth) as maxd
			from oceanAccess, country, ocean
			where country.cid = oceanAccess.cid
				and ocean.oid = oceanAccess.oid
			group by oceanAccess.cid, country.height) ocountryhd)) ct
where country.cid = ct.cid
	and totalspan = (select max(ct.totalspan)
					from country, 
						(select cid, (height + maxd) as totalspan
						from (select oceanAccess.cid, country.height, max(ocean.depth) as maxd
								from oceanAccess, country, ocean
								where country.cid = oceanAccess.cid
									and ocean.oid = oceanAccess.oid
								group by oceanAccess.cid, country.height) ocountryhd
						union
						(select cid, height as totalspan
						from country
						except 
						select cid, height
						from (select oceanAccess.cid, country.height, max(ocean.depth) as maxd
								from oceanAccess, country, ocean
								where country.cid = oceanAccess.cid
									and ocean.oid = oceanAccess.oid
								group by oceanAccess.cid, country.height) ocountryhd)) ct
					where country.cid = ct.cid)
);

-- Query 10 statements
INSERT INTO Query10
(
SELECT  cname, sum(length) AS borderslength
From country,neighbour
WHERE country.cid = neighbour.country
Group by cid
HAVING sum(length) >= All(SELECT  sum(length) AS borderslength
	From country,neighbour
	WHERE country.cid = neighbour.country
	Group by cid
)
);
