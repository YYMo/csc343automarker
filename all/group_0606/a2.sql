-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements
   Create VIEW Countryinfo AS 
                (Select neighbor AS c1id,country.cid AS c2id, country.cname AS c2name , country.height 
                from country INNER Join neighbour ON country.cid = neighbour.country);
   
   Create VIEW Highest1 AS 
                Select c1id, max(height) as max_height
                from Countryinfo
                group by c1id;
   
  Create VIEW Final_Highest AS 
                Select Highest1.c1id,country.cid as c2id,country.cname as c2name
                from Highest1 INNER Join country ON Highest1.max_height = country.height;
                
   
   Create VIEW Final_Highest2 as 
                Select FH.c1id,co.cname AS c1name,FH.c2id,FH.c2name
                from Final_Highest FH Inner Join country co on FH.c1id = co.cid
                order by c1name;
                
   INSERT into Query1(Select * from Final_Highest2);
   DROP VIEW Final_Highest2;
   DROP VIEW Final_Highest;
   DROP VIEW Highest1;
   DROP VIEW Countryinfo;

-- Query 2 statements
   Create VIEW Landlocked as
               Select country.cid, cname 
               from country Left Join oceanAccess on country.cid = oceanAccess.cid
               where oceanAccess.cid is NULL
               order by cname;
   
   INSERT into Query2(Select * from Landlocked);
   DROP VIEW Landlocked;


-- Query 3 statements
   Create VIEW Landlocked as
               Select country.cid, cname 
               from country Left Join oceanAccess on country.cid = oceanAccess.cid
               where oceanAccess.cid is NULL
               order by cname;
               
   CREATE VIEW NumofNbrs as
             Select Landlocked.cid as c1id, COUNT(neighbour.neighbor) as numofnbrs
             from neighbour Inner Join Landlocked on Landlocked.cid = neighbour.country
             group by Landlocked.cid;


   Create View OneNbr as
             Select NumofNbrs.c1id,country.cname as c1name
             from NumofNbrs Inner Join country on NumofNbrs.c1id = country.cid
             where NumofNbrs.numofnbrs = 1;
    
    Create VIEW OneNbrInfo as
             select OneNbr.c1id,OneNbr.c1name,neighbour.country as c2id
             from OneNbr Inner Join neighbour on  OneNbr.c1id = neighbour.neighbor;
    
    Create VIEW Fullinfo as 
             select OneNbrInfo.c1id,OneNbrInfo.c1name,OneNbrInfo.c2id,country.cname as c2name
             from OneNbrInfo Inner Join country on OneNbrInfo.c2id = country.cid
             order by OneNbrInfo.c1name;
             
    INSERT into Query3(Select * from FullInfo);
    DROP VIEW Fullinfo;
    DROP VIEW OneNbrInfo;
    DROP VIEW OneNbr;      
    DROP VIEW NumofNbrs;
    DROP VIEW Landlocked;
             
-- Query 4 statements
   Create VIEW BorderOcean as
             Select cid as cname, oid as oname 
             from oceanAccess
             order by oid DESC;
   Create VIEW IndirectBorder as 
             select country.cid as cname, oceanAccess.oid as oname 
             from country Inner Join neighbour on country.cid = neighbour.country Inner Join
                           oceanAccess on neighbour.neighbor = oceanAccess.cid
             order by oceanAccess.oid DESC;          
    Create View AllOceanBorders as
             (select cname,oname 
             from BorderOcean Union 
             select cname,oname from IndirectBorder) 
             order by oname DESC, cname ASC;
    
    INSERT into Query4(Select * from AllOceanBorders);
    DROP VIEW AllOceanBorders;
    DROP VIEW IndirectBorder;
    DROP VIEW BorderOcean;

-- Query 5 statements
    CREATE VIEW Past5yrAvg as     --Past 5 year hdi average for each country--
             Select cid, sum(hdi_score)/5 as avghdi 
             from hdi 
             where year BETWEEN 2009 and 2013
             group by cid
             order by avghdi desc;
   
    CREATE VIEW Top10Avg as
            select cid, avghdi 
            from Past5yrAvg
            limit 10;
     
    CREATE View Top10Avgcountryinfo as
             select T10.cid, country.cname, T10.avghdi
             from Top10Avg T10 INNER Join country on T10.cid = country.cid;
      
    INSERT into Query5(Select * from Top10Avgcountryinfo);
    DROP VIEW Top10Avgcountryinfo;
    DROP VIEW Top10Avg;
    DROP VIEW Past5yrAvg;
      
            

-- Query 6 statements
    CREATE VIEW IncreaseYear1 as     --Increase in hdi from 2009-2010--
           select hdi2.cid, hdi2.year, hdi2.hdi_score
           from hdi hdi1 Inner Join hdi hdi2 on hdi1.cid = hdi2.cid
           where hdi1.year = 2009 and hdi2.year = 2010 and hdi1.hdi_score < hdi2.hdi_score;
     
    CREATE VIEW IncreaseYear2 as     --Increase in hdi from 2009-2011--
           select hdi.cid, hdi.year,hdi.hdi_score
           from IncreaseYear1 Inner Join hdi on IncreaseYear1.cid = hdi.cid
           where hdi.year = 2011 and IncreaseYear1.hdi_score < hdi.hdi_score;
    
    CREATE VIEW IncreaseYear3 as     --Increase in hdi from 2009-2012--
           select hdi.cid, hdi.year, hdi.hdi_score
           from IncreaseYear2 Inner Join hdi on IncreaseYear2.cid = hdi.cid
           where hdi.year = 2012 and IncreaseYear2.hdi_score < hdi.hdi_score;
    
    CREATE VIEW IncreaseYear4 as     --Increase in hdi from 2009-2013--
           select hdi.cid
           from IncreaseYear3 Inner Join hdi on IncreaseYear3.cid = hdi.cid
           where hdi.year = 2013 and IncreaseYear3.hdi_score < hdi.hdi_score;
    
    CREATE VIEW Increasingcountries as
           select IncreaseYear4.cid, country.cname 
           from IncreaseYear4 Inner Join country on IncreaseYear4.cid = country.cid
           order by country.cname;
    
    INSERT into Query6(Select * from Increasingcountries);
    DROP VIEW Increasingcountries;
    DROP VIEW IncreaseYear4;
    DROP VIEW IncreaseYear3;
    DROP VIEW IncreaseYear2;
    DROP VIEW IncreaseYear1;


-- Query 7 statements
   Create View FollowReligion as    --followers for each religion for each country--
             Select rname, rid, (rpercentage*population) as follow
             from religion inner join country on religion.cid = country.cid;
   
   Create View WorldFollowers as    --followers for each religion in the world--;
             select rid, rname, sum(follow) as follows
             from FollowReligion group by rid, rname
             order by follows desc;
    
    INSERT into Query7(select * from WorldFollowers); 
    DROP VIEW WorldFollowers;
    DROP VIEW FollowReligion;

-- Query 8 statements
   CREATE VIEW PopularPercent as    --selects percentage of most popular language for each country--
           Select cid, max(lpercentage) as percent 
           from language  
           group by cid;
    
    CREATE VIEW PopularLanguage as
          select L1.cid, L1.lname, c1.cname
          from language L1 Inner Join PopularPercent PP on L1.cid = PP.cid Inner Join
                  country c1 on PP.cid = c1.cid
          where L1.lpercentage = PP.percent;
    
    CREATE VIEW SameLanguage as
          select PL1.cname as c1name, PL2.cname as c2name, PL1.lname 
          from PopularLanguage PL1 inner join neighbour n1 on PL1.cid = n1.country inner join 
                PopularLanguage PL2 on n1.neighbor = PL2.cid
          where PL1.lname = PL2.lname
          order by c1name DESC, lname ASC;
     
     INSERT into Query8(Select * from SameLanguage);
     DROP VIEW SameLanguage;
     DROP VIEW PopularLanguage;
     DROP VIEW PopularPercent;
           


-- Query 9 statements
   CREATE VIEW DepthswithOcean as  --all countries that have a border with an ocean--
        select country.cid,country.cname,country.height,oceanAccess.oid
        from country Inner Join oceanAccess on country.cid = oceanAccess.cid;
   
   CREATE View DepthswithoutOcean as   --Landlocked countries--
        select country.cname, country.height as totalspan
        from country Left Join oceanAccess on country.cid = oceanAccess.cid
        where oceanAccess.cid is Null;
   
   Create View DeepestOceanDepth as
       select DWO.cname, (max(ocean.depth+DWO.height)) as totalspan
       from DepthswithOcean DWO inner join ocean on DWO.oid = ocean.oid
       group by DWO.cname; 
   
   Create View Biggestspan as
        select temp.cname, max(temp.totalspan) as max_totalspan
        from
        (select *
        from DeepestOceanDepth Union
        select *
        from DepthswithoutOcean) as temp
        group by temp.cname; 
    
	 Create View RemovedNameFromBiggestspan as
		select distinct max_totalspan
		from Biggestspan; 
    
    Create VIEW Countrybiggestspan as
         select temp2.cname, temp2.totalspan from
        (select *
        from DeepestOceanDepth UNION
        select *
        from DepthswithoutOcean) as temp2 CROSS JOIN RemovedNameFromBiggestspan
        where totalspan = RemovedNameFromBiggestspan.max_totalspan;
        
    INSERT into Query9(Select * from Countrybiggestspan);
    DROP VIEW Countrybiggestspan;
	DROP VIEW RemovedNameFromBiggestspan; 
    DROP VIEW Biggestspan;
    DROP VIEW DeepestOceanDepth;
    DROP VIEW DepthswithoutOcean;
    DROP VIEW DepthswithOcean;
  
-- Query 10 statements
   CREATE VIEW BorderLengths as
        select country.cid as cname, sum(neighbour.length) as borderslength
        from country INNER JOIN neighbour on country.cid = neighbour.country
        group by country.cid;
   
   CREATE VIEW MaxBorder as
        select cname, borderslength
        from BorderLengths 
        where borderslength = (select max(borderslength) from BorderLengths);
   
   INSERT into Query10(select * from MaxBorder);
   DROP VIEW MaxBorder;
   DROP VIEW BorderLengths;



