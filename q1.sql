INSERT INTO query1
SELECT c1.cid c1id, c1.cname c1name, c2.cid c2id, c2.cname c2name
FROM country c1, country c2, neighbour n 
WHERE c1.cid = n.country AND c2.cid = n.neighbor 
	AND c2.height >= ALL(SELECT c3.height c3height
						FROM country c3, neighbour n2 
						WHERE c1.cid = n2.country AND c3.cid = n2.neighbor)
ORDER BY c1name ASC;