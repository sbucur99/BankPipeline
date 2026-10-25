"""Glue Python shell: run fraud Cypher on Neo4j, write flags CSV to S3 for Redshift COPY."""

import csv
import io
import uuid
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions
from neo4j import GraphDatabase

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "NEO4J_URL",
        "NEO4J_USER",
        "NEO4J_PASSWORD",
        "FLAGS_S3_URI",
        "CYPHER_S3_URI",
    ],
)

s3 = boto3.client("s3")


def split_s3(uri: str):
    path = uri.replace("s3://", "")
    bucket, key = path.split("/", 1)
    return bucket, key


def load_cypher(uri: str) -> str:
    bucket, key = split_s3(uri)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return body


driver = GraphDatabase.driver(
    args["NEO4J_URL"],
    auth=(args["NEO4J_USER"], args["NEO4J_PASSWORD"]),
)

run_id = str(uuid.uuid4())
flagged_at = datetime.now(timezone.utc).isoformat()

cypher = load_cypher(args["CYPHER_S3_URI"])

rows = []
with driver.session() as session:
    session.run(
        """
        CREATE CONSTRAINT account_id IF NOT EXISTS
        FOR (a:Account) REQUIRE a.id IS UNIQUE
        """
    )
    session.run(
        """
        CREATE CONSTRAINT txn_id IF NOT EXISTS
        FOR (t:Transaction) REQUIRE t.id IS UNIQUE
        """
    )
    result = session.run(cypher, runId=run_id, flaggedAt=flagged_at)
    for record in result:
        rows.append(dict(record))

driver.close()

buf = io.StringIO()
fieldnames = [
    "txn_id",
    "step",
    "type",
    "amount",
    "name_orig",
    "name_dest",
    "is_fraud",
    "is_flagged_fraud",
    "predicted_fraud",
    "rule_hits",
    "fan_in",
    "fan_out",
    "run_id",
    "flagged_at",
]
writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
writer.writeheader()
for row in rows:
    writer.writerow(
        {
            "txn_id": row.get("txn_id"),
            "step": row.get("step"),
            "type": row.get("type"),
            "amount": row.get("amount"),
            "name_orig": row.get("name_orig"),
            "name_dest": row.get("name_dest"),
            "is_fraud": row.get("is_fraud"),
            "is_flagged_fraud": row.get("is_flagged_fraud"),
            "predicted_fraud": row.get("predicted_fraud"),
            "rule_hits": row.get("rule_hits"),
            "fan_in": row.get("fan_in"),
            "fan_out": row.get("fan_out"),
            "run_id": run_id,
            "flagged_at": flagged_at,
        }
    )

bucket, key = split_s3(args["FLAGS_S3_URI"])
if key.endswith("/"):
    key = f"{key}run={run_id}/flags.csv"
s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue().encode("utf-8"))
print(f"wrote {len(rows)} flag rows to s3://{bucket}/{key}")
