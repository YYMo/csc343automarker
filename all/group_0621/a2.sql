-- Add below your SQL statements. 
-- You can create intermediate views (as needed). Remember to drop these views after you have populated the result tables.
-- You can use the &quot;\i a2.sql&quot; command in psql to execute the SQL commands in this file.

-- Query 1 statements

CREATE VIEW C1 AS (
    SELECT cid AS c1id, cname AS c1name, neighbor 
    FROM country JOIN neighbour ON cid = country
);

CREATE VIEW C2 AS (
    SELECT cid AS c2id, cname AS c2name, height 
    FROM country JOIN neighbour ON cid = neighbor
);

CREATE VIEW maxheight AS (
    SELECT DISTINCT c1id, c1name, MAX(height) 
    FROM C1 JOIN C2 ON neighbor = c2id 
    GROUP BY c1id, c1name 
    ORDER BY c1id
);

INSERT INTO Query1 (
    SELECT DISTINCT c1id, c1name, c2id, c2name 
    FROM maxheight JOIN C2 ON height = max 
    ORDER BY c1name ASC
);

DROP VIEW IF EXISTS C1 CASCADE;
DROP VIEW IF EXISTS C2 CASCADE;
DROP VIEW IF EXISTS maxheight CASCADE;

-- Query 2 statements
    
    CREATE VIEW landCID AS (
        SELECT distinct cid 
        FROM country
        WHERE cid NOT IN
            (SELECT distinct cid
             FROM oceanAccess)
    );
    
    INSERT INTO Query2(
        SELECT distinct landCID.cid, cname
        FROM landCID join country on (landCID.cid = country.cid)
        ORDER BY cname ASC
    );

DROP VIEW IF EXISTS landCID CASCADE;

-- Query 3 statements

CREATE VIEW landCID AS (
    SELECT DISTINCT cid AS c1id       
    FROM country                                            
    WHERE cid NOT IN
        (SELECT DISTINCT cid
         FROM oceanAccess)
);

CREATE VIEW landName AS (
    SELECT DISTINCT c1id, cname AS c1name
    FROM landCID JOIN country ON (c1id = country.cid) 
    ORDER BY cname ASC
);

CREATE VIEW oneNeighbour AS (
    SELECT c1id, c1name, COUNT(neighbor) 
    FROM landName JOIN neighbour ON c1id = country 
    GROUP BY c1id, c1name HAVING COUNT(neighbor) = 1
);

CREATE VIEW oneNeighbourCID AS (
    SELECT c1id, c1name, neighbor AS c2id 
    FROM oneNeighbour JOIN neighbour ON c1id = country
);

INSERT INTO Query3 (
    SELECT c1id, c1name, c2id, cname AS c2name 
    FROM oneNeighbourCID JOIN country ON c2id = cid 
    ORDER BY c1name ASC
);

DROP VIEW IF EXISTS landCID CASCADE;
DROP VIEW IF EXISTS landName CASCADE;
DROP VIEW IF EXISTS oneNeighbour CASCADE;
DROP VIEW IF EXISTS oneNeighbourCID CASCADE;

-- Query 4 statements

    --CID of countries and OID of oceans of neighbours
    CREATE VIEW hasOceanInDirect AS(
        SELECT neighbor AS cid, oid
        FROM neighbour N join oceanAccess O ON (N.neighbor = O.cid)
    );
    
    -- CID of countires, with OID and name of neighbours' oceans
    CREATE VIEW hasOceanName AS (
        SELECT cid, H.oname AS oname
        FROM hasOceanInDirect O join ocean H ON (O.oid = H.oid)
    );
    
    -- CID of counries with names of all accessible oceans
    CREATE VIEW bothOp AS (
        (SELECT cid, oname 
        FROM hasOceanName)
        UNION
        (SELECT O.cid AS cid, H.oname AS oname
        FROM ocean H join oceanAccess O ON (O.oid = H.oid))
    );
    
    -- Names of countires with all accessible oceans 
    CREATE VIEW bothNames AS(
        SELECT cname, oname
        FROM bothOp B join country C ON (B.cid = C.cid)
    );
    
    
    INSERT INTO Query4(
        SELECT cname, oname 
        FROM bothNames
        ORDER BY cname ASC, oname DESC
    );

    DROP VIEW IF EXISTS bothNames CASCADE;
    DROP VIEW IF EXISTS hasOceanName CASCADE;
    DROP VIEW IF EXISTS hasOceanInDirect CASCADE;
    DROP VIEW IF EXISTS bothOp CASCADE;

-- Query 5 statements

CREATE VIEW allYears AS (
    (SELECT cid FROM hdi WHERE year=2009) UNION 
    (SELECT cid FROM hdi WHERE year=2010) UNION 
    (SELECT cid FROM hdi WHERE year=2011) UNION 
    (SELECT cid FROM hdi WHERE year=2012) UNION 
    (SELECT cid FROM hdi WHERE year=2013)
);

CREATE VIEW allYearsName AS (
    SELECT allyears.cid, cname
    FROM allyears JOIN country ON allYears.cid = country.cid
);

INSERT INTO Query5 (
    SELECT allYearsName.cid, cname, AVG(hdi_score) AS avghdi 
    FROM allYearsName JOIN hdi ON allYearsName.cid = hdi.cid
    GROUP BY allYearsName.cid, cname 
    ORDER BY avg(hdi_score) DESC limit 10
);

DROP VIEW IF EXISTS allYears CASCADE;
DROP VIEW IF EXISTS allYearsName CASCADE;

-- Query 6 statements

    CREATE VIEW allYears AS(
        (SELECT cid
        FROM hdi
        WHERE year=2009)
        INTERSECT
        (SELECT cid
        FROM hdi
        WHERE year=2010)
        INTERSECT
        (SELECT cid
        FROM hdi
        WHERE year=2011)
        INTERSECT
        (SELECT cid
        FROM hdi
        WHERE year=2012)
        INTERSECT
        (SELECT cid
        FROM hdi
        WHERE year=2013)
    );
    
    CREATE VIEW validCID AS(
        SELECT A.cid
        FROM allYears A natural join hdi A1, 
        allYears B JOIN hdi B1 ON B.cid = B1.cid, 
        allYears C JOIN hdi C1 ON C.cid = C1.cid, 
        allYears D JOIN hdi D1 ON D.cid = D1.cid, 
        allYears E JOIN hdi E1 ON E.cid = E1.cid
        where A.cid = B.cid and B.cid = C.cid and C.cid = E.cid 
        and A1.year=2009 and B1.year=2010 and C1.year=2011 and 
        D1.year=2012 and E1.year=2013 and A1.hdi_score &lt; B1.hdi_score 
        and B1.hdi_score &lt; C1.hdi_score and C1.hdi_score &lt; D1.hdi_score 
        and D1.hdi_score &lt; E1.hdi_score
    );
    
    INSERT INTO Query6(
        SELECT distinct V.cid, cname
        FROM validCID V join country C on (V.cid = C.cid)
        ORDER BY cname ASC
    );
        
        DROP VIEW if exists validCID CASCADE;
        DROP VIEW if exists allYears CASCADE;

-- Query 7 statements

INSERT INTO Query7 (
    SELECT rid, rname, SUM(rpercentage*population) AS followers 
    FROM religion JOIN country ON religion.cid = country.cid 
    GROUP BY rid, rname 
    ORDER BY followers DESC
);

-- Query 8 statements

    CREATE VIEW mostPop AS(
        SELECT distinct cid, lid
        FROM language 
        EXCEPT 
        SELECT A.cid, A.lid
        FROM language A join language B on (A.cid = B.cid)
        WHERE A.lid &lt;&gt; B.lid and A.lpercentage &lt; B.lpercentage
    );
    
    INSERT INTO Query8(
        SELECT distinct N.country as c1name,
        N.neighbor as c2name, L.lname as lname
        FROM ((neighbour N join mostPop M on (N.country = M.cid))
        join mostPop K on (N.neighbor = K.cid)) join language L on
        (L.lid = M.lid)
        WHERE N.country &lt;&gt; N.neighbor and M.lid = K.lid 
        ORDER BY lname ASC, c1name DESC
    );
    
    DROP VIEW IF EXISTS mostPop CASCADE;

-- Query 9 statements

CREATE VIEW withOcean AS (
    SELECT country.cid, country.cname, country.height + max(depth) AS totalspan 
    FROM country JOIN oceanAccess ON country.cid = oceanAccess.cid JOIN ocean ON oceanAccess.oid = ocean.oid 
    GROUP BY country.cid, country.cname, country.height
);

CREATE VIEW noOcean AS (
    SELECT cname, height AS totalspan 
    FROM country 
    WHERE cname NOT IN (
        SELECT cname FROM withOcean
    )
);

INSERT INTO Query9 (
    (SELECT cname, totalspan 
    FROM withOcean 
    UNION 
    SELECT cname, totalspan 
    FROM noOcean) 
    ORDER BY totalspan DESC LIMIT 1
);

DROP VIEW IF EXISTS withOcean CASCADE;
DROP VIEW IF EXISTS noOcean CASCADE;

-- Query 10 statements

    CREATE VIEW largestL AS(
        SELECT country, SUM(length) as borderlength
        FROM neighbour
        GROUP BY country
        HAVING SUM(length) &gt;= (
            SELECT MAX(totalSum)
            FROM (SELECT country, SUM(length) as totalSum
                    FROM neighbour 
                    GROUP BY country                
                ) AS B
        )
    );
        
    INSERT INTO Query10(
        SELECT cname, borderlength
        FROM largestL L join country C on (L.country = C.cid)
    );
    
    DROP VIEW IF EXISTS largestL CASCADE;
