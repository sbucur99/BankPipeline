CREATE SCHEMA IF NOT EXISTS fraud;

CREATE TABLE IF NOT EXISTS fraud.transactions_flags (
    txn_id             VARCHAR(64) NOT NULL,
    step               INTEGER,
    type               VARCHAR(32),
    amount             DOUBLE PRECISION,
    name_orig          VARCHAR(64),
    name_dest          VARCHAR(64),
    is_fraud           SMALLINT,
    is_flagged_fraud   SMALLINT,
    predicted_fraud    BOOLEAN,
    rule_hits          VARCHAR(512),
    fan_in             INTEGER,
    fan_out            INTEGER,
    run_id             VARCHAR(36),
    flagged_at         TIMESTAMPTZ
)
DISTKEY (txn_id)
SORTKEY (flagged_at, step);

CREATE TABLE IF NOT EXISTS fraud.transactions_flags_staging (LIKE fraud.transactions_flags);

-- Power BI should query this view (latest run per txn).
CREATE OR REPLACE VIEW fraud.v_latest_flags AS
SELECT *
FROM (
    SELECT
        f.*,
        ROW_NUMBER() OVER (PARTITION BY txn_id ORDER BY flagged_at DESC) AS rn
    FROM fraud.transactions_flags f
) ranked
WHERE rn = 1;

-- Glue/Step Functions COPY template (replace bucket/prefix/iam_role):
-- COPY fraud.transactions_flags_staging
-- FROM 's3://BUCKET/serving/flags/'
-- IAM_ROLE 'arn:aws:iam::ACCOUNT:role/paysim-graph-fraud-redshift'
-- FORMAT AS CSV
-- IGNOREHEADER 1
-- TIMEFORMAT 'auto';
--
-- DELETE FROM fraud.transactions_flags
--  USING fraud.transactions_flags_staging s
--  WHERE fraud.transactions_flags.txn_id = s.txn_id
--    AND fraud.transactions_flags.run_id = s.run_id;
--
-- INSERT INTO fraud.transactions_flags
-- SELECT * FROM fraud.transactions_flags_staging;
--
-- TRUNCATE fraud.transactions_flags_staging;
