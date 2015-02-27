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
    FROM (SELECT d.name, c.name
            FROM intercommittee_transactions i, committees c, committees d
            WHERE i.cmte_id=d.id and d.pty_affiliation='DEM' and
                  i.other_id=c.id and c.pty_affiliation='DEM'
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
  SELECT *
    FROM  calc
;

-- Question 5
CREATE VIEW q5 (name, total_pac_donations) AS
  SELECT 1,1 -- replace this line
;

-- Question 6
CREATE VIEW q6 (id) AS
  SELECT 1 -- replace this line
;

-- Question 7
CREATE VIEW q7 (cand_name1, cand_name2) AS
  SELECT 1,1 -- replace this line
;
