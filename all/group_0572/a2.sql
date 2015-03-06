-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

create or replace view countryHeight (cid, height) as

	select cid, height
	from country ;

create or replace view theNeighbours (cid, nid, height) as
	select country, neighbor, height
	from neighbour, countryHeight
	where neighbor = cid ;

create or replace view highHeights (cid, height) as
	select cid, max(height)
	from (select cid, height from theNeighbours) as heights
	group by cid ;

create or replace view highNeighbours (cid, nid, height) as
	select hH.cid, tN.nid, tN.height
	from highHeights hH, theNeighbours tN
	where hH.cid = tN.cid and hH.height = tN.height ;

create or replace view theNames (cid,cname, nid, nname) as
	select c1.cid, c1.cname, c2.cid, c2.cname
	from country c1, country c2 ;

create or replace view finalAnswer (c1id, c1name, c2id, c2name) as
	select hN.cid, cname, hN.nid, nname
	from highNeighbours hN, theNames tN
	where hN.cid = tN.cid and hN.nid = tN.nid
	order by hN.cid asc;

INSERT INTO Query1 (SELECT * FROM finalAnswer);

drop view countryHeight cascade; 
drop view theNames cascade;  

-- Query 2 statements

create or replace view landlocked (cid) as
	select country.cid
	from country except (select cid from oceanAccess) ;

create or replace view finalAnswer2 (cid, cname) as
	select ll.cid, cname
	from landlocked ll, country c
	where ll.cid = c.cid
	order by cname asc;

INSERT INTO Query2 (SELECT * FROM finalAnswer2); 

drop view landlocked cascade;

-- Query 3 statements

create or replace view landlocked (cid) as
	select country.cid
	from country except (select cid from oceanAccess) ;

create or replace view singleNeighbour (cid) as
	select c.country
	from (select country from neighbour) as c
	group by c.country
	having count(c.country) = 1 ;

create or replace view andTheNeighbour (cid, nid) as
	select cid, neighbor  
	from neighbour, singleNeighbour
	where cid = country ;

create or replace view oneLandlocked (cid, nid) as 
	select aTn.cid, aTN.nid
	from landlocked ll,andTheNeighbour aTN
	where aTN.cid = ll.cid ;

create or replace view theNames (cid,cname, nid, nname) as
	select c1.cid, c1.cname, c2.cid, c2.cname
	from country c1, country c2 ;

create or replace view finalAnswer3 (c1id, c1name, c2id, c2name) as
	select oLL.cid, cname, oLL.nid, nname
	from theNames tN, oneLandlocked oLL
	where oLL.cid = tN.cid and oLL.nid = tN.nid ;

INSERT INTO Query3 (SELECT * FROM finalAnswer3); 

drop view singleNeighbour cascade;
drop view landlocked cascade;


-- Query 4 statements

create or replace view accessible (cid) as
	select distinct c.cid
	from country c, neighbour n, oceanAccess oA
	where c.cid = oA.cid or (n.country = oA.cid and c.cid = n.neighbor) ;

create or replace view theOceans (cid, oid) as
	select distinct aC.cid, oA.oid
	from oceanAccess oA, neighbour n, accessible aC 
	where (aC.cid = oA.cid) or 
		(oA.cid = n.country and n.neighbor = aC.cid
			and aC.cid not in (select cid from oceanAccess)) ; 

create or replace view finalAnswer4 (cname, oname) as
	select cname, o.oname
	from theOceans thO, ocean o, country c
	where thO.oid = o.oid and thO.cid = c.cid
	order by cname asc, oname desc ;

INSERT INTO Query4 (SELECT * FROM finalAnswer4); 

drop view accessible cascade;

-- Query 5 statements

create or replace view scores (cid, avghdi) as
	select cid, avg(hdi_score)
	from hdi
	where year between 2009 and 2013
	group by cid
	order by avg(hdi_score) desc
	limit 10 ;

create or replace view finalAnswer5 (cid, cname, avghdi) as
	select s.cid, cname, avghdi
	from scores s, country c
	where s.cid = c.cid;

INSERT INTO Query5 (SELECT * FROM finalAnswer5); 

drop view scores cascade;

-- Query 6 statements

create or replace view increasing (cid) as
	select distinct a.cid
	from hdi as a, hdi as b, hdi as c, hdi as d, hdi as e
	where (a.cid = b.cid and  b.cid = c.cid and c.cid = d.cid 
		and d.cid = e.cid)
		and (a.year < b.year and b.year < c.year
			and c.year < d.year and d.year < e.year)
		and (a.hdi_score < b.hdi_score and b.hdi_score < c.hdi_score
			and c.hdi_score < d.hdi_score and d.hdi_score < e.hdi_score);

create or replace view finalAnswer6 (cid, cname) as
	select i.cid, cname
	from country c, increasing i
	where i.cid = c.cid
	order by cname asc ;

INSERT INTO Query6 (SELECT * FROM finalAnswer6); 

drop view increasing cascade;

-- Query 7 statements

create or replace view religionPercent (rid, rname, rpercentage) as
	select rid, rname, (rpercentage*population)
	from religion r, country c
	where c.cid = r.cid;


create or replace view finalAnswer7 (rid, rname, followers) as
	select rid, rname, sum(rpercentage)
	from religionPercent
	group by rid, rname
	order by sum(rpercentage) desc ;

INSERT INTO Query7 (SELECT * FROM finalAnswer7); 

drop view religionPercent cascade;

-- Query 8 statements

create or replace view theMax (cid, percent) as
	select cid, max(lpercentage)
	from (select cid, lpercentage from language) as l
	group by cid ;

create or replace view maxCName (cid, name, percent) as
	select tM.cid, cname, percent
	from country c, theMax tM
	where c.cid = tM.cid;

create or replace view maxBothNames (cid, name, percent, lname) as
	select mCN.cid, name, percent, lname
	from language l, maxCName mCN
	where l.lpercentage = mCN.percent and mCN.cid = l.cid;

create or replace view finalAnswer8 (c1name, c2name, lname) as
	select mBN1.name, mBN2.name, mBN1.lname
	from neighbour n, maxBothNames as mBN1, maxBothnames as mBN2
	where mBN1.cid = n.country and mBN2.cid = n.neighbor and mBN1.lname = mBN2.lname
	order by mBN1.lname asc, mBN1.name desc ;

INSERT INTO Query8 (SELECT * FROM finalAnswer8); 

drop view theMax cascade;

-- Query 9 statements

create or replace view noOcean (name, difference) as
	select cname, height
	from country c
	where c.cid not in (select cid from oceanAccess) ;

create or replace view differences (name, difference) as
	select cname, abs(depth - height)
	from country c, ocean o, oceanAccess oA
	where c.cid = oA.cid and oA.oid = o.oid ;

create or replace view maxDifference (difference) as
	select max(difference)
	from (select * from differences union select * from noOcean) as total ;

create or replace view finalAnswer9 (cname, totalsapn) as
	select name, mD.difference
	from maxDifference mD, (select * from differences 						union select * from noOcean) as total
	where mD.difference = total.difference ;

INSERT INTO Query9 (SELECT * FROM finalAnswer9); 

drop view noOcean cascade;
drop view differences cascade;
	
-- Query 10 statements


create or replace view totalBorder (cid, len) as
	select country, sum(length)
	from (select country, length from neighbour) as n
	group by country ;

create or replace view maxBorder (len) as
	select max(len)
	from totalBorder ;

create or replace view finalAnswer10 (cname, borderslength) as
	select cname, mB.len
	from maxBorder mB, totalBorder tB, country c
	where mB.len = tB.len and c.cid = tB.cid ;
	
INSERT INTO Query10 (SELECT * FROM finalAnswer10); 

drop view totalBorder cascade;



