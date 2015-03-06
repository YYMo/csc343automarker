-- Add below your SQL statements. 
-- You can CREATE intermediate VIEWs (as needed). Remember to DROP these VIEWs after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
CREATE VIEW countryandNeighbour AS 
	SELECT country,neighbor
	FROM neighbour; 

CREATE VIEW countryNeighbourandINFO AS
	SELECT country,neighbor,height
        FROM countryandNeighbour JOIN country ON
		neighbor = cid;

CREATE VIEW gettingMax AS
	SELECT country,max(height)
	FROM countryNeighbourandINFO
	group by country;
	
CREATE VIEW countryandneighbor AS
	SELECT country as c1id,ct.cname as c1name,ct1.cid as c2id,ct1.cname as c2name
	FROM gettingMax,country ct,country ct1
	where country=ct.cid and max=ct1.height order by c1name asc;

		
INSERT INTO Query1(c1id, c1name,c2id,c2name) 
	SELECT * 
	FROM countryandneighbor;

DROP VIEW countryandNeighbour CASCADE;

-- Query 2 statements

CREATE VIEW countriesNotLocked AS
	SELECT country.cid, country.cname
	FROM oceanAccess JOIN country ON
		oceanAccess.cid = country.cid;

CREATE VIEW countriesLocked AS
	(SELECT cid, cname 
	FROM country)
		EXCEPT
	(SELECT *
	FROM countriesNotLocked);

INSERT INTO Query2(cid, cname) 
	SELECT * 
	FROM countriesLocked 
		ORDER BY cname ASC;

DROP VIEW countriesLocked;
DROP VIEW countriesNotLocked;


-- Query 3 statements

CREATE VIEW landlockcountries as
	(select cid FROM country) EXCEPT (select cid FROM oceanAccess);

CREATE VIEW landlockvsneighbor as
	select country,neighbor  FROM landlockcountries join neighbour
	on cid=country;
	
CREATE VIEW getCount as
	select country,count(*) as num
	FROM landlockvsneighbor group by country;

CREATE VIEW getSingleLandlock as
	select country as gg FROM getCount
	where num=1;

CREATE VIEW getInfo as
	select gg as c1id,myc.cname as c1name,neighbor as c2id,myc1.cname as c2name
	FROM getSingleLandlock,neighbour,country myc,country myc1
	 where gg=country and country=myc.cid and neighbor=myc1.cid order by c2name ASC;

INSERT INTO Query3(c1id, c1name,c2id,c2name) 
	SELECT * 
	FROM getInfo;	

DROP VIEW landlockcountries cascade;

-- Query 4 statements


CREATE VIEW indirectAccess AS
	SELECT country as cid, oid
	FROM neighbour JOIN oceanAccess ON
		neighbour.neighbor = oceanAccess.cid;
	

CREATE VIEW accessibleCountries AS
	(SELECT *
	FROM indirectAccess)
		UNION
	(SELECT *
	FROM oceanAccess);

 
CREATE VIEW oceanAccessibleNames AS
	SELECT cname, oname
	FROM accessibleCountries JOIN country ON
		country.cid = accessibleCountries.cid
	JOIN ocean ON
		ocean.oid = accessibleCountries.oid;

INSERT INTO Query4(cname, oname)
	SELECT *
	FROM oceanAccessibleNames
		ORDER BY cname ASC, oname DESC;


DROP VIEW oceanAccessibleNames;
DROP VIEW accessibleCountries;
DROP VIEW indirectAccess;

-- Query 5 statements

CREATE VIEW getyears as
	select * FROM hdi where 
	year between 2009 and 2013;
-- I know I'm ordering it twice, but its for good measure since the join might throw off the order
CREATE VIEW getAverage as
	select cid as cdd,avg(hdi_score) as avghdi from getyears group by cid order by avghdi desc limit 10;
	
create view gettinfo as
	select cdd,cname,avghdi from getAverage,country ctyy where ctyy.cid=cdd order by avghdi desc;

INSERT INTO Query5(cid, cname,avghdi)
	SELECT *
	FROM gettinfo;

DROP VIEW getyears cascade;
-- Query 6 statements


CREATE VIEW increaseHDI AS
	SELECT hdi.cid
	FROM hdi JOIN hdi AS hd1 ON
		hdi.cid = hd1.cid and hd1.year = 2010 and hdi.year = 2009
	JOIN hdi AS hd2 ON 
		hdi.cid = hd2.cid and hd2.year = 2011
	JOIN hdi AS hd3 ON
		hdi.cid = hd3.cid and hd3.year = 2012
	JOIN hdi AS hd4 ON
		hdi.cid = hd4.cid and hd4.year = 2013
	WHERE hdi.hdi_score < hd1.hdi_score and
		hd1.hdi_score < hd2.hdi_score and
		hd2.hdi_score < hd3.hdi_score and
		hd3.hdi_score < hd4.hdi_score;

CREATE VIEW increaseNameHDI AS
	SELECT country.cid as cid, cname
	FROM increaseHDI JOIN country ON
		country.cid = increaseHDI.cid;
	
INSERT INTO Query6(cid, cname)
	SELECT *
	FROM increaseNameHDI
		ORDER BY  cname ASC;

DROP VIEW increaseNameHDI;
DROP VIEW increaseHDI;
 



-- Query 7 statements
CREATE VIEW religionvscountry as
	select *,(rpercentage * population) as follower from religion natural join  country;

create view addthem as
	select rid,sum(follower) as followers from religionvscountry group by rid  order by followers desc;

create view getttinfo as
	select distinct rid as rr,rname as rn from religion;  

create view match as
	select rid,rn,followers from addthem,getttinfo where rid=rr;

insert into Query7(rid,rname,followers)
	select * FROM match order by followers desc;

DROP VIEW religionvscountry cascade;
drop view getttinfo cascade;
-- Query 8 statements

CREATE VIEW maxPercentLang AS
	SELECT cid, max(lpercentage) 
	FROM language
		GROUP BY cid;

CREATE VIEW mostPopularLang AS
	SELECT language.cid as cid, lname 
	FROM language JOIN maxPercentLang ON
		language.cid = maxPercentLang.cid and lpercentage = max;

CREATE VIEW neighboursWithSamePopLang AS
	SELECT c1.cname as c1name, c2.cname as c2name, mpl1.lname as lname
	FROM neighbour JOIN country AS c1 ON
		neighbour.country = c1.cid 
	JOIN country AS c2 ON
		neighbour.neighbor = c2.cid
	JOIN mostPopularLang AS mpl1 ON
		neighbour.country = mpl1.cid
	JOIN mostPopularLang AS mpl2 ON
		neighbour.neighbor = mpl2.cid
	WHERE mpl1.lname = mpl2.lname and c1.cname > c2.cname;  
				  
INSERT INTO Query8(c1name, c2name, lname)
	SELECT *
	FROM neighboursWithSamePopLang
		ORDER BY lname ASC, c1name DESC;

DROP VIEW neighboursWithSamePopLang;
DROP VIEW mostPopularLang;
DROP VIEW maxPercentLang;


-- Query 9 statements
CREATE VIEW countriesNoOcean as
(select cid FROM country) except (select cid FROM oceanAccess);

CREATE VIEW countriesOcean as
	select cname,oid,height FROM oceanAccess natural join country;

CREATE VIEW noOceanDifference as
select cname,height as totalspan FROM countriesNoOcean natural join country;

CREATE VIEW yesCountryOceanvsOcean as 
select cname,abs(height + depth) as totalspan FROM countriesOcean natural join ocean;

create view combine as
	(select * from noOceanDifference) union (select * from yesCountryOceanvsOcean);

CREATE VIEW unionboth as
select cname,max(totalspan) as totalspan from combine group by cname;

create view getit as
select * from unionboth u where totalspan>=(select max(totalspan) from unionboth);

INSERT INTO Query9(cname,totalspan)
	SELECT *
	FROM getit;

DROP VIEW countriesNoOcean cascade;
drop view countriesOcean cascade;
-- Query 10 statement
CREATE VIEW totalLenBorder AS
	SELECT cname, sum(length)
	FROM neighbour JOIN country ON
		country.cid = neighbour.country
		GROUP BY cname;

CREATE VIEW maxTotalBorder AS
	SELECT max(sum)
	FROM totalLenBorder;

CREATE VIEW maxTotalCountry AS
	SELECT cname, sum as borderslength
	FROM totalLenBorder JOIN maxTotalBorder ON
		sum = max;

INSERT INTO Query10(cname, borderslength)
	SELECT *
	FROM maxTotalCountry;
		


DROP VIEW maxTotalCountry;
DROP VIEW maxTotalBorder;
DROP VIEW totalLenBorder;

		
