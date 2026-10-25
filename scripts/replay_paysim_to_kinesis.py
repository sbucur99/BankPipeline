"""Replay a PaySim CSV into Kinesis so Glue streaming can run beside batch."""

import argparse
import csv
import json
import time

import boto3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True)
    parser.add_argument("--stream", required=True)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--sleep-ms", type=int, default=50)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", region_name=args.region)
    sent = 0
    with open(args.csv, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            payload = {
                "step": int(row["step"]),
                "type": row["type"],
                "amount": float(row["amount"]),
                "nameOrig": row["nameOrig"],
                "oldbalanceOrg": float(row["oldbalanceOrg"]),
                "newbalanceOrig": float(row["newbalanceOrig"]),
                "nameDest": row["nameDest"],
                "oldbalanceDest": float(row["oldbalanceDest"]),
                "newbalanceDest": float(row["newbalanceDest"]),
                "isFraud": int(row["isFraud"]),
                "isFlaggedFraud": int(row["isFlaggedFraud"]),
            }
            kinesis.put_record(
                StreamName=args.stream,
                Data=json.dumps(payload).encode("utf-8"),
                PartitionKey=payload["nameOrig"],
            )
            sent += 1
            if args.limit and sent >= args.limit:
                break
            if args.sleep_ms:
                time.sleep(args.sleep_ms / 1000.0)
    print(f"sent {sent} records to {args.stream}")


if __name__ == "__main__":
    main()
