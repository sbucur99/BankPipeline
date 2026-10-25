// Batch fraud rules for PaySim-style graphs.
// Parameters: $runId, $flaggedAt
// Returns one row per flagged (or labeled) transaction for Redshift.

MATCH (o:Account)-[:SENT]->(t:Transaction)-[:RECEIVED]->(d:Account)
OPTIONAL MATCH (other:Transaction)-[:RECEIVED]->(d)
WITH o, t, d, count(DISTINCT other) AS fanIn
OPTIONAL MATCH (o)-[:SENT]->(outTxn:Transaction)
WITH o, t, d, fanIn, count(DISTINCT outTxn) AS fanOut
WITH o, t, d, fanIn, fanOut,
     [
       CASE
         WHEN t.type IN ['TRANSFER', 'CASH_OUT']
          AND t.oldbalanceOrg > 0
          AND t.newbalanceOrig = 0
          AND t.amount >= 200000
         THEN 'drain-large-amount'
       END,
       CASE WHEN fanIn >= 10 THEN 'high-fan-in-dest' END,
       CASE WHEN fanOut >= 10 THEN 'high-fan-out-orig' END,
       CASE
         WHEN t.type = 'TRANSFER'
          AND EXISTS {
            MATCH (d)-[:SENT]->(cash:Transaction)
            WHERE cash.type = 'CASH_OUT'
              AND cash.step >= t.step
              AND cash.step <= t.step + 1
              AND abs(cash.amount - t.amount) < 1.0
          }
         THEN 'transfer-then-cashout'
       END
     ] AS hits
WITH o, t, d, fanIn, fanOut,
     [h IN hits WHERE h IS NOT NULL] AS ruleHits
WHERE size(ruleHits) > 0 OR t.isFraud = 1 OR t.predictedFraud = true
SET t.predictedFraud = true,
    t.ruleHits = ruleHits,
    t.lastRunId = $runId,
    t.flaggedAt = $flaggedAt
RETURN
  t.id AS txn_id,
  t.step AS step,
  t.type AS type,
  t.amount AS amount,
  o.id AS name_orig,
  d.id AS name_dest,
  t.isFraud AS is_fraud,
  t.isFlaggedFraud AS is_flagged_fraud,
  t.predictedFraud AS predicted_fraud,
  reduce(s = '', h IN ruleHits | s + CASE WHEN s = '' THEN h ELSE '|' + h END) AS rule_hits,
  fanIn AS fan_in,
  fanOut AS fan_out
