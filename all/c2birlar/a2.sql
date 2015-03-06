-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
   Create View NeighbourInfo As
   	  Select country, neighbor, cname, height
  	  From Neighbour join Country on neighbor = cid;
   
   Insert into Query1(
   	  Select country AS c1id, Country.cname AS c1name, 
  	  neighbor AS c2id, NeighbourInfo.cname AS c2name
   	  From NeighbourInfo join Country on NeighbourInfo.country = Country.cid
   	  Group By c1id,c1name
   	  Having max(height)
   	  Order By c1name ASC);
   Drop View NeighbourInfo;

-- Query 2 statements
   Insert into Query2(
   	  Select cid, cname
   	  From country c1
   	  Where Not Exist (Select * From OceanAccess 
   	     	   	   Where c1.cid = OceanAccess.cid));

-- Query 3 statements
   Insert into Query3(
   	  Select Country.cid AS c1id, country.cname AS c1name, 
	  neighbor.cid AS c2id, neighbor.cname AS c2name
   	  From Query2 join Neighbour on Query2.cid = Neighbour.country
   	  Group By Country.cid, Country.cname
   	  Having count(Neighbour.cid) = 1);

-- Query 4 statements
   Create View OceanDirect As 
   	  Select Country.cname, Ocean.oname
   	  From (OceanAccess join Ocean on OceanAccess.oid = Ocean.oid) 
   	  join Country on Country.cid = OceanAccess.cid;
   Create View OceanIndirect AS
   	  Select Country.cname, Ocean.oname
	  From (Neighbour join OceanAcess on Neighbour.oid = OceanAccess.cid) 
	  join Country on Country.cid = OceanAcess.cid;
   Insert into Query4(
   	  Select Country.cname AS cname, Ocean.oname AS oname
	  From OceanDirect Intersect OceanIndirect
	  Order By Country.cname ASC, Ocean.oname Desc);
	Drop View OceanDirect;
	Drop View OceanIndirect;
	  
-- Query 5 statements
   Create View High10 AS
   	  Select cid, Avg(hdi_score) AS avghdi
	  From hdi
	  Where year>= 2009 and year<= 2013
	  Group By cid
	  Order By Avg(hdi_score) DESC
	  limit 10;
   Insert into Query5(
   	  Select cid, cname, avghdi
	  From High10 join Country on High10.cid = Country.cid);
   Drop View High10;
	  
-- Query 6 statements
   Create View ConstantIncrease AS
   	  Select cid
   	  From hdi AS h09
   	  Where h09.year = 2009 
   	  and cid in (Select cid From hdi AS h10
       	      Where h10.year = 2010 and h10.hdi > h09.hdi
	      and cid in (Select cid From hdi AS h11
	      Where h11.year = 2011 and h11.hdi > h10.hdi
	      and cid in (Select cid From hdi AS h12
	      Where h12.year = 2012 and h12.hdi > h11.hdi
	      and cid in (Select cid From hdi as h13
	      Where h13.year = 2013 and h13.hdi > h12.hdi))));
    Insert into Query6(
    	   Select cid, cname
			From ConstantIncrease 
			join Country on ConstantIncrease.cid = Country.cid);
	Drop View ConstantIncrease; 

-- Query 7 statements



-- Query 8 statements



-- Query 9 statements



-- Query 10 statements


