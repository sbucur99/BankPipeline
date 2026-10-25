-- Run after Glue Python job writes flags CSV.
-- Replace BUCKET, PREFIX, and IAM_ROLE.

BEGIN;

TRUNCATE fraud.transactions_flags_staging;

COPY fraud.transactions_flags_staging
FROM 's3://BUCKET/serving/flags/'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/paysim-graph-fraud-redshift'
FORMAT AS CSV
IGNOREHEADER 1
TIMEFORMAT 'auto';

DELETE FROM fraud.transactions_flags
 USING fraud.transactions_flags_staging s
 WHERE fraud.transactions_flags.txn_id = s.txn_id
   AND fraud.transactions_flags.run_id = s.run_id;

INSERT INTO fraud.transactions_flags
SELECT * FROM fraud.transactions_flags_staging;

COMMIT;
