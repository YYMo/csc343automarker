-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW datatable AS 
    SELECT neighbour.country AS c1id, neighbour.neighbor AS c2id, country.cname AS c2name,
          country.height AS c2height
    FROM country, neighbour 
    WHERE cid = neighbor; 


CREATE VIEW  maxneighbour AS 
    SELECT dt1.*
    FROM datatable dt1 LEFT OUTER JOIN datatable dt2 
    ON (dt1.c1id = dt2.c1id AND dt1.c2height < dt2.c2height)
    WHERE dt2.c2height is NULL;

INSERT INTO Query1(
    SELECT dt1.c1id AS c1id, cname AS c1name, dt1.c2id AS c2id, dt1.c2name AS c2name
    FROM maxneighbour dt1, country
    WHERE dt1.c1id = cid
    ORDER BY c1name);
    

DROP VIEW maxneighbour;
DROP VIEW datatable;



-- Query 2 statements
INSERT INTO Query2 (SELECT cid, cname 
		     FROM country
		     WHERE cid NOT IN (SELECT cid FROM oceanAccess)
                     ORDER BY cname);

-- Query 3 statements
CREATE VIEW landlocked AS
	SELECT Query2.cid AS c1id, Query2.cname AS c1name, neighbor AS c2id, country.cname AS c2name 
	FROM   Query2,neighbour,country
	WHERE Query2.cid = neighbour.country and  country.cid = neighbor;
INSERT INTO Query3 (SELECT landlocked.c1id AS c1id,c1name, c2id, c2name
                     FROM landlocked,(SELECT c1id FROM landlocked GROUP BY c1id HAVING count(c1id) = 1) AS temp1
  		     WHERE temp1.c1id = landlocked.c1id);
DROP VIEW landlocked;


-- Query 4 statements
CREATE VIEW direct AS
	SELECT cname, oname
	FROM oceanAccess,country,ocean
     	WHERE oceanAccess.cid = country.cid and ocean.oid= oceanAccess.oid;
     	

create view indirect AS
	SELECT cname, oname
	FROM neighbour,oceanAccess, country, ocean
	WHERE neighbor= oceanAccess.cid AND country = country.cid AND oceanAccess.oid= ocean.oid;


INSERT INTO QUERY4 (SELECT * FROM direct 
		     union
		     SELECT* FROM indirect
		     ORDER BY cname ASC,oname DESC);

DROP VIEW direct;
DROP VIEW indirect;


-- Query 5 statements
CREATE VIEW countries AS
	SELECT*
	FROM hdi
	WHERE year >= 2009 AND year <= 2013;

CREATE VIEW topTen AS
	SELECT cid, avg(hdi_score) AS avghdi
	FROM countries
	GROUP BY cid
	ORDER BY avghdi DESC
	LIMIT 10;

INSERT INTO Query5(SELECT country.cid AS cid, cname, avghdi
		    FROM topTen, country
		    WHERE topTen.cid=country.cid);

DROP VIEW topTen CASCADE;
DROP VIEW countries CASCADE;



-- Query 6 statements
CREATE VIEW TimePeriod AS
    SELECT *
    FROM hdi
    WHERE year > 2008 AND year < 2014;

CREATE VIEW increasingHdi AS
    SELECT t1.cid AS cid
    FROM TimePeriod t1, TimePeriod t2, TimePeriod t3, TimePeriod t4, TimePeriod t5
    WHERE t1.cid = t2.cid  AND t2.cid = t3.cid AND t3.cid = t4.cid AND t4.cid = t5.cid 
	   AND t1.year = 2009 AND t2.year = 2010 AND t3.year = 2011 AND t4.year = 2012
	   AND t5.year = 2013 
           AND (t5.hdi_score - t4.hdi_score > 0) AND (t4.hdi_score - t3.hdi_score > 0) 
           AND (t3.hdi_score - t2.hdi_score > 0) AND (t2.hdi_score - t1.hdi_score > 0);

INSERT INTO Query6(
    SELECT country.cid AS cid, country.cname AS cname
    FROM increasingHdi JOIN country
    ON increasingHdi.cid = country.cid
    ORDER BY country.cname
);

DROP VIEW increasingHdi;
DROP VIEW TimePeriod;


-- Query 7 statements

CREATE VIEW followersTable AS
    SELECT religion.rid AS rid, religion.rname AS rname, 
            sum(religion.rpercentage * country.population) AS followers
    FROM religion JOIN country
    ON religion.cid = country.cid
    GROUP BY rid, rname;
 

INSERT INTO Query7(
    SELECT rid, rname, followers
    FROM followersTable
    ORDER BY followers DESC 
);

DROP VIEW followersTable;



-- Query 8 statements

CREATE VIEW most AS
	SELECT  cid, MAX(lpercentage) AS most
	FROM   language
	GROUP BY cid
	ORDER BY cid;

CREATE VIEW mostpop AS
	SELECT language.cid as cid ,lid,lname,lpercentage
	FROM most,language
	WHERE 	most = lpercentage and most.cid = language.cid;

CREATE VIEW countryPop AS
	SELECT country,neighbor,lname AS l1name
	FROM mostpop,neighbour
	WHERE 	mostpop.cid = country;

CREATE VIEW neighborPop AS
	SELECT country AS c1id,countryPop.neighbor AS c2id,l1name,mostpop.lname AS l2name
	FROM mostpop,countryPop
	WHERE 	neighbor = cid;

CREATE VIEW final AS
	select cname AS c1name, c2id,l1name,l2name
	from neighborpop, country
	where	c1id =cid and l1name =l2name;

INSERT INTO Query8 (select c1name, cname as c2name, l1name as lname
		     from final, country
		     where c2id = cid
		     order by lname ASC, c1name DESC);

DROP VIEW final CASCADE;




-- Query 9 statements

CREATE VIEW noAccess AS
	SELECT cid
	FROM country
	except
	SELECT cid
	FROM oceanAccess;

CREATE VIEW deepest AS
	SELECT cid, depth 
	FROM oceanAccess, ocean
	WHERE oceanAccess.oid = ocean.oid;


CREATE VIEW withAccess AS
	SELECT country.cid as cid, height + depth as diff
	FROM deepest,country
	WHERE deepest.cid= country.cid;

CREATE VIEW everything AS
	SELECT country.cid as cid, height as diff
	FROM noAccess, country
	WHERE noAccess.cid= country.cid
	UNION
	SELECT*
	FROM withAccess;

CREATE VIEW final AS
	SELECT max(diff) as diff  
	FROM everything;

INSERT INTO Query9(SELECT cname,final.diff as totalspan FROM everything,final,country WHERE everything.cid = country.cid AND final.diff = everything.diff);

DROP VIEW final;
DROP VIEW noAccess CASCADE;
DROP VIEW deepest CASCADE;



 


	
	

-- Query 10 statements

CREATE VIEW allLengths AS
	SELECT country,sum(length) as borderslength
	FROM neighbour
	GROUP BY country;

CREATE VIEW longest AS
	SELECT MAX(borderslength) as borderslength
	FROM allLengths;
	

INSERT INTO QUERY10(SELECT cname, longest.borderslength as borderslength FROM allLengths,longest,country WHERE allLengths.borderslength = longest.borderslength AND allLengths.country = country.cid);

DROP VIEW longest;
	


