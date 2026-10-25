"""Glue streaming: Kinesis transactions -> Neo4j MERGE + curated Parquet."""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "STREAM_NAME",
        "CURATED_PATH",
        "CHECKPOINT_PATH",
        "NEO4J_URL",
        "NEO4J_USER",
        "NEO4J_PASSWORD",
        "AWS_REGION",
    ],
)

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

raw = glue_ctx.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "streamName": args["STREAM_NAME"],
        "startingPosition": "LATEST",
        "inferSchema": "true",
        "classification": "json",
        "region": args["AWS_REGION"],
    },
    transformation_ctx="kinesis_txns",
)


def write_microbatch(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    df = (
        batch_df.withColumn("step", F.col("step").cast("int"))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("oldbalanceOrg", F.col("oldbalanceOrg").cast("double"))
        .withColumn("newbalanceOrig", F.col("newbalanceOrig").cast("double"))
        .withColumn("oldbalanceDest", F.col("oldbalanceDest").cast("double"))
        .withColumn("newbalanceDest", F.col("newbalanceDest").cast("double"))
        .withColumn("isFraud", F.col("isFraud").cast("int"))
        .withColumn("isFlaggedFraud", F.col("isFlaggedFraud").cast("int"))
        .na.drop(subset=["nameOrig", "nameDest", "amount"])
        .withColumn(
            "txnId",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("step"),
                    F.col("type"),
                    F.col("nameOrig"),
                    F.col("nameDest"),
                    F.col("amount").cast("string"),
                ),
                256,
            ),
        )
        .withColumn("ingestedAt", F.current_timestamp())
    )

    df.write.mode("append").partitionBy("step").parquet(args["CURATED_PATH"])

    graph_df = df.select(
        "txnId",
        "step",
        "type",
        "amount",
        "nameOrig",
        "oldbalanceOrg",
        "newbalanceOrig",
        "nameDest",
        "oldbalanceDest",
        "newbalanceDest",
        "isFraud",
        "isFlaggedFraud",
    )

    graph_df.write.format("org.neo4j.spark.DataSource").mode("Append").option(
        "url", args["NEO4J_URL"]
    ).option("authentication.type", "basic").option(
        "authentication.basic.username", args["NEO4J_USER"]
    ).option(
        "authentication.basic.password", args["NEO4J_PASSWORD"]
    ).option(
        "query",
        """
        MERGE (o:Account {id: event.nameOrig})
        MERGE (d:Account {id: event.nameDest})
        MERGE (t:Transaction {id: event.txnId})
        SET t.step = event.step,
            t.type = event.type,
            t.amount = event.amount,
            t.oldbalanceOrg = event.oldbalanceOrg,
            t.newbalanceOrig = event.newbalanceOrig,
            t.oldbalanceDest = event.oldbalanceDest,
            t.newbalanceDest = event.newbalanceDest,
            t.isFraud = event.isFraud,
            t.isFlaggedFraud = event.isFlaggedFraud,
            t.source = coalesce(t.source, 'stream')
        MERGE (o)-[:SENT]->(t)
        MERGE (t)-[:RECEIVED]->(d)
        WITH t, event
        WHERE event.type IN ['TRANSFER', 'CASH_OUT']
          AND event.oldbalanceOrg > 0
          AND event.newbalanceOrig = 0
          AND event.amount >= 200000
        SET t.predictedFraud = true,
            t.ruleHit = coalesce(t.ruleHit, '') + '|drain-and-cashout'
        """,
    ).save()


glue_ctx.forEachBatch(
    frame=raw,
    batch_function=write_microbatch,
    options={
        "windowSize": "30 seconds",
        "checkpointLocation": args["CHECKPOINT_PATH"],
    },
)
job.commit()
