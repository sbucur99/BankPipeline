CREATE CONSTRAINT account_id IF NOT EXISTS
FOR (a:Account) REQUIRE a.id IS UNIQUE;

CREATE CONSTRAINT txn_id IF NOT EXISTS
FOR (t:Transaction) REQUIRE t.id IS UNIQUE;

CREATE INDEX txn_step IF NOT EXISTS FOR (t:Transaction) ON (t.step);
CREATE INDEX txn_type IF NOT EXISTS FOR (t:Transaction) ON (t.type);
CREATE INDEX txn_predicted IF NOT EXISTS FOR (t:Transaction) ON (t.predictedFraud);
