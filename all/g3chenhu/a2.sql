-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
INSERT INTO Query1
(
	select c1.cid c1id, c1.cname c1name, c2t.c2id, c2t.c2name
	from country c1 join(
		select distinct ne.country c1id, ne.neighbor c2id, c2.cname c2name
		from  neighbour ne join country c2 on ne.neighbor=c2.cid
		where c2.height >= all(
			select c2i.height
			from neighbour nei join country c2i on nei.neighbor=c2i.cid
			where nei.country=ne.country
			)
		) c2t
		on c1.cid = c2t.c1id
	order by c1name
);
-- Query 2 statements
INSERT INTO Query2
(
	select distinct c.cid cid, c.cname cname
	from country c
	where c.cid not in(
		select ci.cid
		from country ci join oceanAccess oc on ci.cid=oc.cid
		)
	order by cname
);
-- Query 3 statements
INSERT INTO Query3
(
	select ct.cid c1id, ct.cname c1name, co.cid c2id, co.cname c2name 
	from(  
		select distinct ll.cid cid, ll.cname cname, ne.neighbor cneighbor
		from Query2 ll join neighbour ne on ll.cid=ne.country
		where ne.neighbor = all(
			select net.neighbor
			from Query2 llt join neighbour net on llt.cid=net.country
			where net.country = ne.country
			)
		) ct join country co 
		on ct.cneighbor=co.cid
	order by c1name

);
-- Query 4 statements
INSERT INTO Query4
(
	select oa.cname cname, oa.oname oname
	from( 
			(
			select  co.cname cname, oc.oname oname
			from country co join oceanAccess oa on co.cid=oa.cid
							join ocean oc on oa.oid=oc.oid
			)
			union
			(
			select  c1.cname cname, oc.oname oname
			from country c1 join neighbour ne on c1.cid=ne.country
							join country c2 on ne.neighbor=c2.cid
							join oceanAccess oa on c2.cid=oa.cid
							join ocean oc on oa.oid=oc.oid
			)
	) oa
	order by cname, oname desc
);
-- Query 5 statements
INSERT INTO Query5
(
	select co.cid cid, co.cname cname, sum(h.hdi_score)/5 avghdi
	from country co join hdi h on co.cid=h.cid
	where h.year>=2009 and h.year<=2013
	group by co.cid, co.cname
	order by avghdi desc
	limit 10
);
-- Query 6 statements
INSERT INTO Query6
(
	select h2009.cid cid, h2009.cname cname
	from(
	select co.cid cid, co.cname cname, h.hdi_score h2009
	from country co join hdi h on co.cid=h.cid
	where h.year = 2009
	) h2009 join (
	select co.cid cid, h.hdi_score h2010
	from country co join hdi h on co.cid=h.cid
	where h.year = 2010
	) h2010 on h2009.cid=h2010.cid join(
	select co.cid cid, h.hdi_score h2011
	from country co join hdi h on co.cid=h.cid
	where h.year = 2011
	) h2011 on h2010.cid=h2011.cid join(
	select co.cid cid, h.hdi_score h2012
	from country co join hdi h on co.cid=h.cid
	where h.year = 2012
	) h2012 on h2011.cid=h2012.cid join(
	select co.cid cid, h.hdi_score h2013
	from country co join hdi h on co.cid=h.cid
	where h.year = 2013
	) h2013 on h2012.cid=h2013.cid
	where h2013.cid=h2012.cid and
		  h2013.cid=h2011.cid and
		  h2013.cid=h2010.cid and
		  h2013.cid=h2009.cid and
		  h2012.cid=h2011.cid and
		  h2012.cid=h2010.cid and
		  h2012.cid=h2009.cid and
		  h2011.cid=h2010.cid and
		  h2011.cid=h2009.cid and
		  h2010.cid=h2009.cid and
		  h2013.h2013>h2012.h2012 and
		  h2012.h2012>h2011.h2011 and
		  h2011.h2011>h2010.h2010 and
		  h2010.h2010>h2009.h2009
	order by cname
);
-- Query 7 statements
INSERT INTO Query7
(
	select re.rid rid, re.rname rname, sum(co.population*re.rpercentage) followers
	from country co join religion re on co.cid=re.cid
	group by re.rid, re.rname
	order by followers desc
);
-- Query 8 statements
INSERT INTO Query8
(
	select most1.c1 c1name, most2.c2 c2name, most1.l1 lname
	from(
		select co.cid cid1, co.cname c1, l.lname l1
		from country co join language l on co.cid=l.cid
		where l.lpercentage >= all(
			select l1.lpercentage
			from language l1
			where l.cid=l1.cid
			)
		) most1 join(
		select co.cid cid2, co.cname c2, l.lname l2
		from country co join language l on co.cid=l.cid
		where l.lpercentage >= all(
			select l1.lpercentage
			from language l1
			where l.cid=l1.cid
			)
		)most2 on most1.l1=most2.l2
	where most1.c1 != most2.c2 and (most1.cid1, most2.cid2) in (
		select ne.country cid1, ne.neighbor cid2
		from neighbour ne
		)
	order by lname, c1name desc
);
-- Query 9 statements
INSERT INTO Query9
(
	select t1.cname cname, t1.totalspan totalspan
	from(
		select co.cname cname, (co.height+coalesce(oc.depth, 0)) totalspan
		from country co left join oceanAccess oa on co.cid=oa.cid
						left join ocean oc on oa.oid=oc.oid
		) t1
	where t1.totalspan >= all(
		select (co.height+coalesce(oc.depth, 0)) totalspan
		from country co left join oceanAccess oa on co.cid=oa.cid
						left join ocean oc on oa.oid=oc.oid
		)
);
-- Query 10 statements
INSERT INTO Query10
(
	select t1.cname cname, t1.border borderlength
	from(
		select co.cname cname, sum(ne.length) border
		from country co join neighbour ne on co.cid=ne.country
		group by co.cname
		) t1
	where t1.border >= all(
		select sum(ne.length) border
		from country co join neighbour ne on co.cid=ne.country
		group by co.cname
		)
);





























