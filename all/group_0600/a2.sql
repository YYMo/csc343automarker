-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views
-- after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.



/********************************************************************************************************************************************************
**********                                      ACTUAL ASSIGNMENT QUERIES STARY HERE				     						************************
*********************************************************************************************************************************************************/
-- Query 1 statements
create view v1 as 
	(select cid as c1id, cname as c1name, neighbor
	from country c join neighbour n on c.cid = n.country);
create view v2 as
	(select *
	from v1 v join country c on v.neighbor = c.cid); 
create view result1 as
	(select temp.c1id, temp.c1name, temp.cid as c2id, temp.cname as c2name
	from v2 temp
	inner join(
		select c1id, max(height) height
		from v2
		group by c1id) ss on temp.c1id = ss.c1id and temp.height = ss.height order by temp.c1name ASC);
INSERT INTO Query1 (select * from result1);
DROP VIEW result1;
DROP VIEW v2;
DROP VIEW v1;

------------------------------------------------------------------------
-- Query 2 statements: 
/*Find the landlocked countries. 
A landlocked country is a country entirely enclosed by land (e.g., Switzerland). 
Report the id(s) and name(s) of the landlocked countries.*/
create view result2 as
	(SELECT cid,cname
	FROM country
	WHERE country.cid NOT IN (SELECT oceanAccess.cid FROM oceanAccess)
	ORDER BY cname ASC);
INSERT INTO Query2 (select * from result2);
DROP VIEW result2;
------------------------------------------------------------------------
-- Query 3 statements
create view noOcean as
	(select cid 
	from country
	where cid not in (select cid from oceanAccess));
create view allNeighbours as
	(select country, count(neighbor) neighbor
	from neighbour
	group by country);
create view landlocked as
	(select cid 
	from noOcean o join allNeighbours a on o.cid = a.country
	where neighbor < 2);
-- get name, id, and neighbor of all landlocked countries with <2 neighbours
create view view1 as
	(select cid c1id, cname c1name, neighbor c2id
	from country co join neighbour ne on co.cid = ne.country
	where cid in (select * from landlocked));

create view result3 as 
	(select c1id, c1name, c2id, cname c2name  
	from view1 v1 join country co on v1.c2id = co.cid);


INSERT INTO Query3 (select * from result3);
DROP VIEW result3;
DROP VIEW view1;
DROP VIEW landlocked;
DROP VIEW allNeighbours;
DROP VIEW noOcean;


-- Query 4 statements
/*Find the accessible ocean(s) of each country. 
 An ocean is accessible by a country if either the country itself has a coastline on that ocean (direct access to the ocean)
 or the country is neighboring another country that has a coastline on that ocean (indirect access). 
 Report the name of the country and the name of the accessible ocean(s).*/

create view result4 as
	((SELECT c1.cname, o.oname
	FROM country c1
		INNER JOIN neighbour n ON c1.cid=n.country
		INNER JOIN oceanAccess oa ON (n.country=oa.cid OR n.neighbor=oa.cid)
		INNER JOIN ocean o ON (oa.oid=o.oid)
	)
	UNION
	(SELECT c1.cname,o.oname
	FROM country c1
		INNER JOIN oceanAccess oa ON c1.cid=oa.cid
		INNER JOIN ocean o ON (oa.oid=o.oid)
	)
	ORDER BY cname ASC, oname DESC);
INSERT INTO Query4 (select * from result4);
DROP VIEW result4;


-- Query 5 statements
create view fiveYear as
	(select cid, avg(hdi_score) avghdi 
	from hdi
	where year < 2014 and year > 2008
	group by cid);

create view result5 as
	(select co.cid, co.cname, fv.avghdi
	from country co join fiveYear fv on co.cid = fv.cid 
	order by fv.avghdi desc limit 10);
	INSERT INTO Query5 (select * from result5);
	DROP VIEW result5;
	DROP VIEW fiveYear;

------------------------------------------------------------------------
-- Query 6 statements
/*Find the countries for which their Human Development Index (HDI) is constantly increasing over the 5-year period of 2009-2013 (inclusive). 
Constantly increasing means that from year to year there is a positive change (increase) in the country’s HDI. */
--Can first find table
create view result6 as
	(SELECT c.cid,c.cname
	FROM country c
	  INNER JOIN hdi h1 ON c.cid=h1.cid
	  INNER JOIN hdi h2 ON h1.cid=h2.cid 
	  INNER JOIN hdi h3 ON h2.cid=h3.cid
	  INNER JOIN hdi h4 ON h3.cid=h4.cid
	  INNER JOIN hdi h5 ON h4.cid=h5.cid
	WHERE (h1.year=2009 AND h2.year=2010 AND h3.year=2011 AND h4.year=2012 AND h5.year=2013)
		   AND (h2.hdi_score>h1.hdi_score AND h3.hdi_score>h2.hdi_score AND h4.hdi_score>h3.hdi_score AND h5.hdi_score>h4.hdi_score));
INSERT INTO Query6 (select * from result6);
DROP VIEW result6;

-- Query 7 statements
create view view1 as
	(select rid, sum(population * rpercentage/100) followers 
	from country natural join religion
	group by rid);

create view rnames as
	(select distinct rid, rname
	from religion);

create view result7 as
	(select rid rid1, rname, followers
	from view1 natural join rnames
	order by followers desc);
	INSERT INTO Query7 (select * from result7);
	DROP VIEW result7;
	DROP VIEW rnames;
	DROP VIEW view1;


------------------------------------------------------------------------

-- Query 8 statements
/*Find all the pairs of neighboring countries that have the same most popular language. 
For example, <Canada, USA, English> is one example tuple because in both countries, English is the most popular language; 
<Chile, Argentina, Spanish> can be another tuple, and so on. Report the names of the countries and the name of the language. */

create view cpoplan AS 
(

	SELECT c.cid AS cid,cname,lname
	FROM country c INNER JOIN language l ON c.cid=l.cid 
		INNER JOIN 
		(
			SELECT cid, max(lpercentage) lp
			FROM language 
			GROUP BY cid
		)step ON l.cid=step.cid AND l.lpercentage=step.lp
	ORDER BY cname
);

create view result8 as 
	(SELECT c1.cname AS c1name,c2.cname AS c2name,c1.lname
	FROM cpoplan c1
		INNER JOIN neighbour n ON c1.cid=n.country
		INNER JOIN cpoplan c2  ON n.neighbor=c2.cid
	WHERE c1.lname=c2.lname
	ORDER BY c1.lname ASC, c1name DESC);
INSERT INTO Query8 (select * from result8);
DROP VIEW result8;
DROP VIEW cpoplan;

-- Query 9 statements
create view noOcean as
	(select cid
	from country
	where cid not in (select cid from oceanAccess));

create view view1 as 
	(select cname, height as totalspan
	from noOcean no join country co on no.cid = co.cid);

create view view2 as 
	(select cname, max(height + depth) as totalspan
	from (country natural join oceanAccess) tmp join
	ocean oc on tmp.oid = oc.oid
	group by cid);

create view greatest as
	(select max(totalspan) totalspan
	from view2);

create view result9 as
	(select cname, totalspan
	from view2
	where totalspan in(select * from greatest));
INSERT INTO Query9 (select * from result9);
DROP VIEW result9;
DROP VIEW greatest;
DROP VIEW view2;
DROP VIEW view1;
DROP VIEW noOcean;


------------------------------------------------------------------------
-- Query 10 statements
/* Find the country with the longest total border length (with all its neighboring countries).
 Report the country and the total length of its borders. */
 --first create a table with all countries and their total border length as column

create view country_bl AS 
(	
	SELECT c.cname as cname, sum(length) AS borderslength
	FROM country c 
		INNER JOIN neighbour n ON c.cid=n.country
	GROUP BY c.cname
);

create view result10 as
	(SELECT cname, borderslength
	FROM country_bl 
	WHERE borderslength IN (SELECT max(borderslength) FROM country_bl));
INSERT INTO Query10 (select * from result10);
DROP VIEW result10;
DROP VIEW country_bl; 




------------------------------------------------------------------------

