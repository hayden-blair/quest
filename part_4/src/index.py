import json
import os
import urllib.request

import boto3

_secrets = boto3.client("secretsmanager")

def _get_token():
    secret_arn = os.environ["DATABRICKS_TOKEN_SECRET_ARN"]
    resp = _secrets.get_secret_value(SecretId=secret_arn)
    return resp["SecretString"]

def handler(event, context):
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    job_id = os.environ["DATABRICKS_JOB_ID"]
    token = _get_token()

    url = f"{host}/api/2.1/jobs/run-now"
    payload = json.dumps({"job_id": int(job_id)}).encode("utf-8")

    req = urllib.request.Request(
        url,
        data=payload,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        body = json.loads(resp.read().decode("utf-8"))

    return {"statusCode": 200, "run_id": body.get("run_id")}