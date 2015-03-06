--csc343h A2 c0leeseu

--query1
drop view if exists detailNeighbor ;
drop view if exists detailCountry ;
drop view if exists groupedCountry ;

create view detailNeighbor(c1id, c2id, c2name, height) as
    select n1.country as c1id, n1.neighbor as c2id, c1.cname as c2name, height
    from neighbour n1 join country c1 on n1.neighbor =c1.cid;

create view detailCountry(c1id, c1name, c2id, c2name, height) as
    select c1id, c1.cname as c1name, c2id, c2name, detailNeighbor.height
    from country c1, detailNeighbor
    where c1.cid = c1id;
    
create view groupedCountry(c1id, maxH) as
    select c1id, max(height) as maxH
    from detailCountry
    group by c1id;

insert into Query1(
    select detailCountry.c1id, c1name, c2id, c2name
    from detailCountry, groupedCountry 
    where detailCountry.c1id = groupedCountry.c1id and height = maxH
    order by c1name asc
);
-- Query 2 statements
insert into Query2(
    select c1.cid, c1.cname
    from country c1
    where c1.cid not in (select cid from oceanAccess)
    order by c1.cname asc
);

-- Query 3 statements
drop view if exists landlocked;
drop view if exists countNeighbour;
drop view if exists onlyOne;
drop view if exists partialAns;

create view landlocked(cid, cname) as
    select cid, cname
    from country
    where cid not in (select cid from oceanAccess);

create view countNeighbour(cid, counted) as
    select neighbour.country, count(neighbour.neighbor)
    from landlocked, neighbour
    where country = cid
    group by country;

create view onlyOne(cid) as
    select cid 
    from countNeighbour
    where counted = 1;

create view partialAns(cid, nid) as
    select country, neighbor
    from neighbour, onlyOne
    where country = cid;
    
insert into Query3(
    select p1.cid, c1.cname, p1.nid, c2.cname
    from partialAns p1, country c1, country c2
    where p1.cid = c1.cid and p1.nid = c2.cid
    order by c1.cname asc
);


--Query4

drop view if exists onameAccess;


create view onameAccess (cname, cid, oname) as
	select country.cname, country.cid, oname
	from country join (oceanAccess join ocean on oceanAccess.oid = ocean.oid) on country.cid = oceanAccess.cid;

insert into Query4 (
	select country.cname, test.oname
	from (country join neighbour on country.cid = neighbour.country) join (select onameAccess.cid, onameAccess.oname from onameAccess) as test on neighbour.neighbor = test.cid
	order by country.cname asc, test.oname desc
	);
	
--query5

drop view if exists avgHdi;

create view avgHdi (cid, avg) as
	select cid, sum(hdi_score)/5 as avg
	from hdi
	where year = 2009 or year = 2010 or year = 2011 or year = 2012 or year = 2013
	group by cid
	order by avg desc;

insert into Query5(
	select avgHdi.cid, country.cname, avgHdi.avg 
	from avgHdi, country
	where avgHdi.cid = country.cid
	limit 10
	order by avgHdi.avg DESC
	);

--query6
drop view if exists pos2010;
drop view if exists pos2011;
drop view if exists pos2012;
drop view if exists pos2013;

create view pos2010 (cid, score) as
	select h2.cid, h2.hdi_score
	from hdi h1, hdi h2
	where h1.year = 2009 and h2.year = 2010 and h1.cid = h2.cid and h1.hdi_score < h2.hdi_score;

create view pos2011 (cid, score) as
	select hdi.cid, hdi.hdi_score
	from hdi, pos2010
	where hdi.year = 2011 and hdi.cid = pos2010.cid and score < hdi_score ;
	
create view pos2012 (cid, score) as
	select hdi.cid, hdi.hdi_score
	from hdi, pos2011
	where hdi.year = 2012 and hdi.cid = pos2011.cid and score < hdi_score ;

create view pos2013 (cid, score) as
	select hdi.cid, hdi.hdi_score
	from hdi, pos2012
	where hdi.year = 2013 and hdi.cid = pos2012.cid and score < hdi_score ;

insert into Query6(
	select country.cid, cname
	from country, pos2013
	where pos2013.cid = country.cid
	order by cname asc
	);
	
--query7
drop view if exists religionFollower;

create view religionFollower(rid, follower) as
	select rid, sum(population*(rpercentage/100)) as follower
	from country, religion
	where country.cid = religion.cid
	group by rid;
	
insert into Query7(
	select distinct(religionfollower.rid), rname, religionfollower.follower
	from religionFollower, religion
	where religionFollower.rid = religion.rid
	order by follower desc);

--query8
drop view if exists mostP;
drop view if exists mostL;
drop view if exists partialAns;

create view mostP(cid, maxp) as
	select cid, max(lpercentage) as maxp
	from language
	group by cid; 
	
create view mostL (cid, lname) as
	select language.cid, lname
	from language, mostP
	where language.cid = mostP.cid and language.lpercentage = mostP.maxP;
	
create view partialAns(c1id, c2id, lname) as
	select L.cid, NL.cid, L.lname
	from mostL L, mostL NL, neighbour
	where L.lname = NL.lname and L.cid = neighbour.country and NL.cid = neighbour.neighbor and L.cid != NL.cid;

insert into query8(
	select c1.cname, c2.cname, lname
	from partialAns, country c1, country c2
	where c1.cid = partialAns.c1id and c2.cid = partialAns.c2id
	order by lname asc, c1.cname desc
	);

--query9
drop view if exists oAccessH;
drop view if exists noAccessH;
drop view if exists partialAns;

create view oAccessH (cname, totalspan) as
	select country.cname, max(country.height+ocean.depth)
	from oceanAccess, country, ocean
	where country.cid = oceanAccess.cid and ocean.oid = oceanAccess.oid
	group by country.cname;

create view noAccessH(cname, totalspan) as
	select distinct(country.cname), country.height
	from oceanAccess, country
	where country.cid not in (select cid from oceanAccess);
	
create view partialAns(cname, totalspan) as
	select cname, totalspan
	from oAccessH 
	union
	select  cname, totalspan
	from noAccessH;
	
insert into Query9(
	select cname, totalspan
	from partialAns
	where totalspan = (select max(totalspan) from partialAns)
	);  

--query10
drop view if exists border;
drop view if exists border2;

create view border(cid, length) as 
	select country, sum(length)
	from neighbour
	group by country;
	
create view border2 (cname, length) as
	select country.cname, length
	from country, border
	where country.cid = border.cid;
	
insert into query10(
	select cname, length
	from border2
	where length = (select max(length) from border2));
