-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
create view final as
select a.country, a.neighbor, a.cname 
from (select country, max(height)
		from (select n.country,c1.cname,n.neighbor,c1.height
				from neighbour n, country c1
				where n.neighbor = c1.cid) allneighbours
				group by country) max 
		inner join 
			(select n.country,c1.cname,n.neighbor,c1.height
			from neighbour n, country c1
			where n.neighbor = c1.cid) a 
on max.country = a.country and max.max = a.height;

insert into Query1(
select final.country as c1cid, c.cname as c1name, 
final.neighbor as c2id, final.cname as c2name
from final join country as c
on final.country = c.cid
order by c1name asc);

drop view final;


-- Query 2 statements

insert into Query2(
select c2.cid as cid, c2.cname as cname
from ((select cid
		from country)
		except
		(select cid
		from oceanAccess)) c1,country c2
where c1.cid = c2.cid
order by cname asc);



-- Query 3 statements
create view cids as
(select cid
from (select c2.cid as cid, c2.cname as cname
		from ((select cid
				from country)
				except
				(select cid
				from oceanAccess)) c1,country c2
		where c1.cid = c2.cid)landlocked)
intersect
(select country as cid
from (select country, count(country)
		from neighbour
		group by country
		having count(country)=1)count);

insert into Query3(
select n.country as c1id,c1.cname as c1name, 
n.neighbor as c2id,c2.cname as c2name 
from cids c, neighbour n, country as c1,country c2
where c.cid= n.country and c1.cid = n.country and c2.cid = n.neighbor
order by c1name asc);

drop view cids;


-- Query 4 statements

insert into Query4(
(select c.cname,o2.oname 
from neighbour n, oceanAccess o1, ocean o2, country c
where o1.cid = n.neighbor and c.cid = country and 
o1.oid = o2.oid)
union
(select c.cname as country, o2.oname  from oceanAccess o1, 
ocean o2,country c
where o1.oid = o2.oid and o1.cid = c.cid) 
order by cname asc,oname desc);




-- Query 5 statements

create view top10 as
select cid, avg(hdi_score) as score 
from (select * from hdi
		where year in (2009,2010,2011,2012,2013))period
group by cid order by score desc limit 10;

insert into Query5(
select c.cid, c.cname,t.score as avghdi
from top10 t, country c
where t.cid =c.cid
order by avghdi desc);

drop view top10;
-- Query 6 statements

create view ab as
select * from hdi
where year >=2009 and year<=2013
order by cid,year desc;

insert into Query6(
select c.cid, c.cname 
from (select cid from country
		except 
		select distinct a.cid
		from ab a, ab b
		where a.year<b.year and a.hdi_score>=b.hdi_score
		and a.cid=b.cid) as a, country c 
where a.cid=c.cid
order by cname asc);


drop view ab;
-- Query 7 statements

insert into Query7(
select rid,rname, sum(rpercentage*population) as followers
from religion r, country c 
where r.cid = c.cid group by rname,rid
order by followers desc);


-- Query 8 statements

create view noName as
select n.country, n.neighbor,m2.lname,m1.maxpercentage 
from neighbour n,
	(select l.cid,l.lname,mp.max as maxpercentage 
	from (select cid, max(lpercentage) 
			from language 
			group by cid)mp, language l
	where mp.cid = l.cid and max = l.lpercentage) m1,
	(select l.cid,l.lname,mp.max as maxpercentage 
	from (select cid, max(lpercentage) 
		from language 
		group by cid)mp, language l
	where mp.cid = l.cid and max = l.lpercentage) m2 
where n.neighbor = m1.cid 
and n.country = m2.cid and m2.lname = m1.lname;


insert into Query8(
select c1.cname as c1name, c2.cname as c2name,lname
from noName n, country c1, country c2
where c1.cid = n.country 
and c2.cid = n.neighbor order by lname asc,c1name desc);



drop view noName;

-- Query 9 statements


create view depthPlusHeight as
select cid, max+height as sum 
from (select c.cid, max(depth),cname,height 
		from oceanAccess oa, ocean o, country c 
		where oa.oid=o.oid and c.cid=oa.cid 
		group by c.cid ) as a
		union
		(select c.cid,c.height as sum 
		from (select cid from country 
				except 
				select cid from oceanAccess) as a, country as c 
		where a.cid=c.cid);
		
insert into Query9(
select c.cname,max as totalspan 
from(select max(sum) from depthPlusHeight) a,country c,
depthPlusHeight as d
where c.cid = d.cid and a.max=d.sum);
drop view depthPlusHeight;

-- Query 10 statements

create view sum as
select country,sum(length) from neighbour group by country;

insert into Query10(
select c.cname, s.sum as borderslength 
from (select max(sum) from sum) m, sum s,country c 
where m.max = s.sum and c.cid = s.country);

drop view sum;



