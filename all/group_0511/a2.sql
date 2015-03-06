SET search_path TO A2;
-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
create or replace view cc as
select c1.cid "c1id", c1.cname "c1name", c2.cid "c2id", c2.cname "c2name", c2.height
from (select * from country) "c1", (select * from country) "c2";

create or replace view allnays as
select *
from cc
where exists (select * from neighbour
					where cc.c1id = country and cc.c2id = neighbor)
union
select *
from cc
where exists (select * from neighbour
					where cc.c1id = neighbor and cc.c2id = country)
;

create or replace view upup as
select c1id, max(height) "up"
from allnays
group by c1id;

create or replace view playback as
select allnays.c1id, allnays.c1name, allnays.c2id, allnays.c2name
from upup join allnays on upup.c1id = allnays.c1id and upup.up = allnays.height
order by allnays.c1name ASC;

insert into Query1 (select * from playback);

drop view playback;
drop view upup;
drop view allnays;
drop view cc;


-- Query 2 statements
insert into Query2(
	select distinct cid, cname
	from country
	where not exists (select * from oceanAccess where country.cid = oceanAccess.cid)
	order by(cname) ASC
	);

-- Query 3 statements
create or replace view cc as
select c1.cid "c1id", c1.cname "c1name", c2.cid "c2id", c2.cname "c2name"
from (select * from country) "c1", (select * from country) "c2";

create or replace view ll as
select distinct cid, cname
from country
where not exists (select * from oceanAccess where country.cid = oceanAccess.cid)
order by(cname) ASC;

create or replace view st as
select c1id, c1name, c2id, c2name
from cc join ll on cc.c1id = ll.cid;

create or replace view allnays as
select *
from st
where exists (select * from neighbour
					where st.c1id = country and st.c2id = neighbor)
union
select *
from st
where exists (select * from neighbour
					where st.c1id = neighbor and st.c2id = country)
;

create or replace view yafv as
select c1id, count(c1id)
from allnays
group by c1id
having count(c1id) = 1;

create or replace view q3final as
select allnays.*
from allnays join yafv on allnays.c1id = yafv.c1id
order by c1name ASC;

insert into Query3 (select * from q3final);

drop view q3final;
drop view yafv;
drop view allnays;
drop view st;
drop view ll;
drop view cc;


-- Query 4 statements
create or replace view v as
select country.cname, oceanAccess.cid, oceanAccess.oid, ocean.oname
from oceanAccess join country on oceanAccess.cid = country.cid
		join ocean on ocean.oid = oceanAccess.oid;

create or replace view cc as
select c1.cid "c1id", c1.cname "c1name", c2.cid "c2id", c2.cname "c2name"
from (select * from country) "c1", (select * from country) "c2";

create or replace view allnays as
select *
from cc
where exists (select * from neighbour
					where cc.c1id = country and cc.c2id = neighbor)
union
select *
from cc
where exists (select * from neighbour
					where cc.c1id = neighbor and cc.c2id = country)
;

create or replace view alloceans as
select allnays.c1name "cname", allnays.c1id "cid", oceanAccess.oid, ocean.oname
from allnays join oceanAccess on allnays.c2id = oceanAccess.cid
		join ocean on oceanAccess.oid = ocean.oid
;

create or replace view q4final as
select cname, oname from v
union 
select cname, oname from alloceans
order by cname ASC, oname DESC;

insert into Query4 (select * from q4final);

drop view q4final;
drop view alloceans;
drop view allnays;
drop view cc;
drop view v;


-- Query 5 statements
create or replace view years as
select *
from hdi
where hdi.year > 2008 and hdi.year < 2014;

create or replace view q5final as
select years.cid, country.cname, avg(hdi_score) as avghdi
from years join country on years.cid = country.cid
group by years.cid, country.cname
order by avghdi DESC
limit 10;

insert into Query5 (select * from q5final);

drop view q5final;
drop view years;


-- Query 6 statements
create or replace view hdi_range as
select *
from hdi
where hdi.year > 2008 and hdi.year < 2014;

insert into Query6 (
					select cid, cname
					from country
					where country.cid in 
					(
						select h1.cid
						from hdi_range "h1" 
							join hdi_range "h2" on h1.cid = h2.cid and h1.year + 1 = h2.year
							join hdi_range "h3" on h1.cid = h3.cid and h2.year + 1 = h3.year
							join hdi_range "h4" on h1.cid = h4.cid and h3.year + 1 = h4.year
							join hdi_range "h5" on h1.cid = h5.cid and h4.year + 1 = h5.year
						where h1.hdi_score < h2.hdi_score 
							and h2.hdi_score < h3.hdi_score 
							and h3.hdi_score < h4.hdi_score 
							and h4.hdi_score < h5.hdi_score
					)
					order by cname ASC
					);

drop view hdi_range;


-- Query 7 statements
create or replace view pop1 as 
select rid, rname, rpercentage*population as "pop"
from country, religion
where religion.cid = country.cid;

create or replace view q7final as
select rid, rname, sum(pop) as "followers"
from pop1
group by rid, rname
order by "followers" DESC;

insert into Query7 (select * from q7final);

drop view q7final;
drop view pop1;

-- Query 8 statements
create or replace view cc as
select c1.cid "c1id", c1.cname "c1name", c2.cid "c2id", c2.cname "c2name"
from (select * from country) "c1", (select * from country) "c2";

create or replace view allnays as
select *
from cc
where exists (select * from neighbour
					where cc.c1id = country and cc.c2id = neighbor)
union
select *
from cc
where exists (select * from neighbour
					where cc.c1id = neighbor and cc.c2id = country)
;

create or replace view eh as
select country.cname, language.*
from language join country on language.cid = country.cid;

create or replace view popsongs as
select cid, max(lpercentage) "pops"
from language
group by cid;

create or replace view mostpop as
select eh.*
from popsongs join eh on popsongs.pops = eh.lpercentage;

create or replace view yas as
select c1id, c1name, mp1.lname "mp1name", c2id, c2name, mp2.lname "mp2name"
from allnays join mostpop "mp1" on mp1.cid = allnays.c1id
		join mostpop "mp2" on mp2.cid = allnays.c2id
;

create or replace view q8final as
select c1name, c2name, mp1name "lname"
from yas 
where mp1name = mp2name
order by lname ASC, c1name DESC;

insert into Query8 (select * from q8final);

drop view q8final;
drop view yas;
drop view mostpop;
drop view popsongs;
drop view eh;
drop view allnays;
drop view cc;


-- Query 9 statements
create or replace view mega as 
select * 
from ocean natural join country natural join oceanAccess;

create or replace view tallest as
select mega.cname, max(height + depth) as "totalspan"
from mega
group by mega.cname; 

create or replace view reallytall as
select max(totalspan) as "totalspan"
from tallest;

insert into Query9 (select tallest.*
					from tallest join reallytall on tallest.totalspan = reallytall.totalspan
					);

drop view reallytall;
drop view tallest;
drop view mega;

-- Query 10 statements
create or replace view sums as
select country, sum(length) "borderlength"
from neighbour
group by country
order by country;

create or replace view thebest as
select *
from sums
where sums.borderlength = (select max(borderlength)
							from sums
							)
;

insert into Query10 (select country.cname, thebest.borderlength "borderslength"
					from country join thebest on country.cid = thebest.country 
					);

drop view thebest;
drop view sums;

