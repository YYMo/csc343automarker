-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the "\i a2.sql" command in psql to execute the SQL commands in this file.

-- Query 1 statements

INSERT INTO Query1 (Select r.c1id,r.c1name,country.cid As c2id,country.cname As c2name

From (Select o.c1id,country.cname As c1name,o.height

		From (	Select neighbour.country As c1id,Max(height) As height
			From neighbour,country
			Where neighbour.neighbor=country.cid 
			Group By neighbour.country) As o,country

		Where o.c1id=country.cid) As r,country

Where r.height=country.height
Order By c1name ASC);


-- Query 2 statements

INSERT INTO Query2 
(Select c.cid,c.cname 
From country c

Where Not Exists
	(Select * From oceanaccess o 
	Where o.cid=c.cid)

Order By c.cname ASC);


-- Query 3 statements

INSERT INTO Query3 (
Select a.cid As c1id,c1.cname As c1name,c2.cid As c2id,c2.cname As c2name

From (Select l.cid,Count(neighbor) As nnum

	From (Select c.cid From country c
    
		Where Not Exists(Select * From oceanaccess o 
        
		Where o.cid=c.cid)) As l,neighbour As n
        
	Where l.cid = n.country

	Group By l.cid) As a,country c1,country c2,neighbour ne

Where a.nnum=1 And c1.cid = a.cid And a.cid = ne.country And c2.cid=ne.neighbor

Order By c1name ASC);


-- Query 4 statements



-- Query 5 statements



-- Query 6 statements



-- Query 7 statements



-- Query 8 statements



-- Query 9 statements



-- Query 10 statements


