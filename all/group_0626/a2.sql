--11111111111111111111111111111111
--NOT DONE********************************************



--222222222222222222222222222222 DONE!
CREATE VIEW coastal AS
	SELECT country.cid, cname
	FROM country, oceanAccess
	WHERE country.cid = oceanAccess.cid;

INSERT INTO Query2 (
	SELECT cid, cname
	FROM country
	EXCEPT (SELECT * FROM coastal)
	);
	
DROP VIEW coastal;

--3333333333333333333333333333333333
-- NOT DONE******************************************************
CREATE VIEW coastal AS
	SELECT country.cid, cname
	FROM country, oceanAccess
	WHERE country.cid = oceanAccess.cid;

CREATE VIEW landlocked AS
	SELECT country.cid, cname
	FROM country
	EXCEPT (SELECT * FROM coastal);

--CREATE VIEW surrounded AS
--	SELECT country.cid AS c1id, cname AS c1name, country.cid AS c2id, cname AS c2name, count(c2id) AS numc2id
--	FROM country, neighbour
--	WHERE c1id = neighbour.cid AND c2id = neighbour.cid
--	GROUP BY c1id, c1name, c2id, c2name
--	HAVING numc2id < 2

CREATE VIEW surrounded AS
	(SELECT country, count(neighbor) as borders
	FROM neighbour
	GROUP BY country
	HAVING count(neighbor) < 2);
	
INSERT INTO Query3(
	SELECT c1id, c1name, c2id, c2name
	FROM landlocked, surrounded
	WHERE c1id = cid, c1name = cname
	ORDER BY c1name ASC
	);
--4444444444444444444444444444444444 DONE!
CREATE VIEW coastal AS
	SELECT country.cid, cname, oname
	FROM country, oceanAccess, ocean
	WHERE country.cid = oceanAccess.cid AND oceanAccess.oid = ocean.oid;

CREATE VIEW semicoastal AS
	SELECT country.cid, cname, oname
	FROM country, oceanAccess, neighbour, ocean
	WHERE neighbor = oceanAccess.cid AND neighbour.country = country.cid AND oceanAccess.oid = ocean.oid
	EXCEPT (SELECT * FROM coastal);

INSERT INTO Query4(
	(SELECT cname, oname
	FROM coastal)
	UNION
	(SELECT cname, oname
	FROM semicoastal)
	ORDER BY cname ASC, oname DESC
	);
	
DROP VIEW semicoastal;
DROP VIEW coastal;
--555555555555555555555555555555555 DONE!
CREATE VIEW inbetween AS
	SELECT country.cid AS cid, cname, hdi_score
	FROM country, hdi
	WHERE country.cid = hdi.cid AND hdi.year < 2014 AND hdi.year > 2008;

CREATE VIEW average AS
	SELECT cid, cname, AVG(hdi_score) AS avghdi
	FROM inbetween
	GROUP BY cid, cname;
	
INSERT INTO Query5(
	SELECT cid, cname, avghdi
	FROM average
	ORDER BY avghdi DESC
	LIMIT 10
	);

DROP VIEW average;
DROP VIEW inbetween;
--6666666666666666666666666666666666666 DONE!
CREATE VIEW nine AS
	SELECT country.cid AS cid, cname, hdi_score
	FROM country, hdi
	WHERE year=2009 AND country.cid = hdi.cid;
	
CREATE VIEW ten AS
	SELECT country.cid AS cid, cname, hdi_score
	FROM country, hdi
	WHERE year=2010 AND country.cid = hdi.cid;
	
CREATE VIEW eleven AS
	SELECT country.cid AS cid, cname, hdi_score
	FROM country, hdi
	WHERE year=2011 AND country.cid = hdi.cid;
	
CREATE VIEW twelve AS
	SELECT country.cid AS cid, cname, hdi_score
	FROM country, hdi
	WHERE year=2012 AND country.cid = hdi.cid;
	
CREATE VIEW thirteen AS
	SELECT country.cid AS cid, cname, hdi_score
	FROM country, hdi
	WHERE year=2013 AND country.cid = hdi.cid;
	
CREATE VIEW nineten AS
	SELECT ten.cid AS cid, ten.cname AS cname
	FROM nine, ten
	WHERE nine.hdi_score < ten.hdi_score AND nine.cid=ten.cid AND nine.cname=ten.cname;
	
CREATE VIEW teneleven AS
	SELECT eleven.cid AS cid, eleven.cname AS cname
	FROM ten, eleven
	WHERE ten.hdi_score < eleven.hdi_score AND ten.cid=eleven.cid AND ten.cname=eleven.cname;

CREATE VIEW eleventwelve AS
	SELECT twelve.cid AS cid, twelve.cname AS cname
	FROM eleven, twelve
	WHERE eleven.hdi_score < twelve.hdi_score AND eleven.cid=twelve.cid AND eleven.cname=twelve.cname;

CREATE VIEW twelvethirteen AS
	SELECT thirteen.cid AS cid, thirteen.cname AS cname
	FROM twelve, thirteen
	WHERE twelve.hdi_score < thirteen.hdi_score AND twelve.cid=thirteen.cid AND twelve.cname=thirteen.cname;

	
INSERT INTO Query6(
	SELECT twelvethirteen.cid AS cid, twelvethirteen.cname AS cname
	FROM nineten, teneleven, eleventwelve, twelvethirteen
	WHERE nineten.cid = teneleven.cid AND teneleven.cid = eleventwelve.cid AND eleventwelve.cid = twelvethirteen.cid
	);
DROP VIEW twelvethirteen;
DROP VIEW eleventwelve;
DROP VIEW teneleven;
DROP VIEW nineten;
DROP VIEW thirteen;
DROP VIEW twelve;
DROP VIEW eleven;
DROP VIEW ten;
DROP VIEW nine;

--777777777777777777777777777777777777777777 DONE!
CREATE VIEW numfol AS
	SELECT rid, rname, (rpercentage * 0.01 * country.population) AS followers
	FROM country, religion
	WHERE religion.cid = country.cid;
	
INSERT INTO Query7(
	SELECT rid, rname, sum(followers)
	FROM numfol
	GROUP BY rid, rname
);

DROP VIEW numfol;

--88888888888888888888888888888888888888888888


--99999999999999999999999999999999999999999999 DONE!
CREATE VIEW coastal AS
	SELECT country.cid, cname, height
	FROM country, oceanAccess
	WHERE country.cid = oceanAccess.cid;

CREATE VIEW landlocked AS
	SELECT country.cid, cname, height
	FROM country
	EXCEPT (SELECT * FROM coastal);
			
CREATE VIEW deep AS
	SELECT country.cid, cname, MAX(depth) AS deepest
	FROM country, oceanAccess, ocean
	WHERE country.cid = oceanAccess.cid AND oceanAccess.oid = ocean.oid
	GROUP BY country.cid, cname;
	
	
CREATE VIEW elevations AS
	SELECT country.cname, height, deepest
	FROM country, deep
	WHERE country.cid = deep.cid;
	
INSERT INTO Query8(
	SELECT cname, (height + deepest) AS totalspan
	FROM elevations
	UNION
	SELECT cname, height AS totalspan
	FROM landlocked
	);
	
DROP VIEW elevations;
DROP VIEW deep;
DROP VIEW landlocked;
DROP VIEW coastal;

--11111111111111111111111111111111111111111000000000000000000 DONE!

INSERT INTO Query10(
	SELECT cname, sum(length) AS borderslength
	FROM country, neighbour
	WHERE country.cid = neighbour.country
	GROUP BY cname
	);