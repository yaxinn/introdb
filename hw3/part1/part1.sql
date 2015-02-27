DROP VIEW IF EXISTS q1a, q1b, q1c, q1d, q2, q3, q4, q5, q6, q7;

-- Question 1a
CREATE VIEW q1a(id, amount)
AS
  -- SELECT 1,1 -- replace this line
  SELECT cmte_id, transaction_amt 
    FROM committee_contributions 
    WHERE transaction_amt > 5000
;

-- Question 1b
CREATE VIEW q1b(id, name, amount)
AS
  -- SELECT 1,1,1 -- replace this line
  SELECT cmte_id, name, transaction_amt 
    FROM committee_contributions 
    WHERE transaction_amt > 5000
;

-- Question 1c
CREATE VIEW q1c(id, name, avg_amount)
AS
  -- SELECT 1,1,1 -- replace this line
  SELECT cmte_id, name, AVG(transaction_amt)
    FROM committee_contributions
    WHERE transaction_amt > 5000
    GROUP BY cmte_id, name
;

-- Question 1d
CREATE VIEW q1d(id, name, avg_amount)
AS
  -- SELECT 1,1,1 -- replace this line
  SELECT id, name, avg_amount
    FROM q1c
    WHERE avg_amount > 10000
;

-- Question 2
CREATE VIEW q2(from_name, to_name)
AS
  -- SELECT 1,1 -- replace this line
  SELECT from_name, to_name
    FROM (SELECT c.name, d.name
            FROM intercommittee_transactions i, committees c, committees d
            WHERE i.cmte_id=d.id and d.pty_affiliation='DEM' and i.other_id=c.id and c.pty_affiliation='DEM'
            GROUP BY (d.name, c.name), d.name, c.name
            ORDER by COUNT((d.name, c.name)) DESC
            LIMIT 10) AS topten(from_name, to_name)
    ORDER BY from_name
;

-- Question 3
CREATE VIEW q3(name)
AS
  -- SELECT 1 -- replace this line
  SELECT c.name
    FROM committees c
    WHERE c.id NOT IN 
      (SELECT s.cmte_id
        FROM committee_contributions s
        WHERE s.cand_id=(SELECT id FROM candidates WHERE name='OBAMA, BARACK'))
    ORDER BY c.name
;

-- Question 4.
CREATE VIEW q4 (name)
AS
  -- SELECT 1 -- replace this line
  WITH stat AS (SELECT cand_id              
                  FROM committee_contributions 
                  GROUP BY cand_id, cmte_id),
       tot AS (SELECT COUNT(*)*0.01 as total FROM committees),
       calc AS (SELECT cand_id, COUNT(cand_id), total
                  FROM stat, tot
                  GROUP BY cand_id, total)
  SELECT b.name
    FROM  calc a, candidates b
    WHERE a.cand_id=b.id AND a.count > a.total
    ORDER BY b.name
;

-- Question 5
CREATE VIEW q5 (name, total_pac_donations) AS
  -- SELECT 1,1 -- replace this line
  WITH org AS (SELECT cmte_id, transaction_amt 
                FROM individual_contributions
                WHERE entity_tp='ORG')
  SELECT c.name, SUM(o.transaction_amt)
    FROM committees c LEFT OUTER JOIN org o
    ON c.id=o.cmte_id
    GROUP BY c.id, c.name
    ORDER BY c.name
;

-- Question 6
CREATE VIEW q6 (id) AS
  -- SELECT 1 -- replace this line
  WITH contri AS (SELECT c.cand_id, c.entity_tp
                    FROM committee_contributions c
                    WHERE c.entity_tp='PAC' OR c.entity_tp='CCM'
                    GROUP BY c.cand_id, c.entity_tp)
  SELECT cand_id
  FROM (SELECT cand_id, COUNT(cand_id)
          FROM contri
          GROUP BY cand_id) AS reduced(cand_id, count)
  WHERE count=2
;

-- Question 7
CREATE VIEW q7 (cand_name1, cand_name2) AS
  -- SELECT 1,1 -- replace this line
  WITH cand_RI_cmte AS (SELECT cand_id, cmte_id
                          FROM committee_contributions 
                          WHERE state='RI'
                          GROUP BY cand_id, cmte_id),
       cand_share_join AS (SELECT a.cand_id AS cand_id1, b.cand_id AS cand_id2
                            FROM cand_RI_cmte a JOIN cand_RI_cmte b
                            ON a.cmte_id=b.cmte_id
                            WHERE a.cand_id!=b.cand_id
                            GROUP BY cand_id1, cand_id2)
  SELECT a.name, b.name
    FROM candidates a, candidates b, cand_share_join c
    WHERE c.cand_id1=a.id AND c.cand_id2=b.id
    ORDER BY a.name
;
