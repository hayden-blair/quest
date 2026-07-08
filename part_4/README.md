# Databricks Daily Job with Downstream S3 Event Pipeline

This project provisions a chained, serverless data pipeline on AWS using
Terraform, deployed in the `us-east-2` region. A daily Databricks job produces a
JSON file in S3, and the arrival of that file automatically triggers a second,
downstream Databricks job.

The flow has two stages:

1. **Scheduled trigger (head of the chain)** - an EventBridge schedule invokes a
   Lambda function once per day, which reads a Databricks token from Secrets
   Manager and starts a Databricks job via the Jobs API. That Databricks job
   writes a JSON file to an S3 bucket as part of its work.
2. **Event-driven downstream stage** - writing that JSON file to S3 emits an
   S3 event notification, which places a message on an SQS queue. A consumer
   Lambda is invoked automatically by that queue and starts a second, separate
   Databricks job.

The two stages are linked: stage 2 fires as a direct consequence of the file
produced by stage 1. Running the daily job therefore sets off the entire chain.
The stages share one Databricks token (stored once in Secrets Manager) and
trigger different Databricks jobs.

## Architecture

```
Daily schedule:
  EventBridge (daily cron)
        |
        v
  Lambda -> Databricks job A ------ writes JSON file --> S3 bucket
   |                                                        |
   v                                                        v
  Secrets Manager (token)                     S3 event notification (.json)
                                                            |
                                                            v
                                                        SQS queue
                                                            |
                                                            v
                                              Consumer Lambda -> Databricks job B
                                                            |
                                                            v
                                                  Secrets Manager (token)
```

## Files

- `main.tf` - all Terraform resources for the pipeline
- `terraform.tfvars` - non-secret configuration (workspace host, job IDs)
- `src/index.py` - the Lambda handler, shared by both Lambda functions

Both Lambda functions run the same handler. They differ only by the
`DATABRICKS_JOB_ID` environment variable, so each triggers a different
Databricks job from the same code.

## Security design

### The token is never stored in this repository

The Databricks token is deliberately kept out of all source, configuration, and
Terraform state files. Terraform creates an empty Secrets Manager secret; the
token value is added afterward with a single AWS CLI command that talks directly
to AWS and never passes through Terraform.

As a result, nothing in this submission contains the token: not `main.tf`, not
`terraform.tfvars`, and not the Terraform state.

### Least-privilege IAM roles

Each Lambda function has its own IAM role scoped to exactly what it needs:

- The scheduled Lambda's role can write logs and read the token secret.
- The consumer Lambda's role can write logs, read the token secret, and
  receive/delete messages from the SQS queue.

The scheduled Lambda is intentionally not granted any access to the queue, and
neither role holds permissions it does not use.

## Prerequisites

- An AWS account with credentials configured (`aws configure`), default region
  `us-east-2`
- Terraform CLI installed
- A Databricks workspace, two jobs to run, and a personal access token
- Read access to the target S3 bucket, plus permission to set its notification
  configuration

## Configuration

Edit `terraform.tfvars` and set your workspace URL and both job IDs:

```hcl
databricks_host            = "https://your-workspace.cloud.databricks.com"
databricks_job_id          = "123456789"   # daily job that writes the JSON file (stage 1)
databricks_consumer_job_id = "987654321"   # downstream job triggered by the file (stage 2)
```

Each job ID is the numeric ID shown in the URL when you open the job in
Databricks. The token is not set here; see the deploy steps.

The S3 bucket that receives the JSON file is referenced by name in `main.tf`
as an existing bucket (it is not created or managed by this Terraform). The
event notification is filtered to the specific object key the daily job writes.
Update the bucket name and key filters in the `aws_s3_bucket_notification`
resource if the job writes to a different location.

## Deploy

1. Initialize and apply:

   ```
   terraform init
   terraform apply
   ```

   This creates both Lambda functions and their IAM roles, the EventBridge
   schedule, an empty Secrets Manager secret, the SQS queue and its policy, the
   S3 event notification on the existing bucket, and the event source mapping
   that connects the queue to the consumer Lambda.

2. Set the token value (required; the Lambdas cannot run without it):

   ```
   aws secretsmanager put-secret-value \
     --secret-id databricks-daily-trigger-token \
     --secret-string "dapi-your-real-token" \
     --region us-east-2
   ```

   This command sends the token straight to AWS Secrets Manager. It does not
   touch Terraform or any local file.

Note: once applied, the event source mapping is live and will immediately
consume any messages already on the queue, which triggers the downstream job. If
test messages are already queued, expect a downstream job run right after apply.

## Testing

### Stage 1 - daily Databricks trigger

Invoke the Lambda manually rather than waiting for the schedule:

```
aws lambda invoke --function-name databricks-daily-trigger response.json
cat response.json
```

A response containing a `run_id` means the daily Databricks job was triggered.
When that job completes and writes its JSON file to S3, it sets off stage 2
automatically.

### Stage 2 - S3 to SQS to consumer Lambda

Stage 2 is triggered by the JSON file landing in S3. It can be exercised
directly by writing (or re-writing) the target object, without waiting for the
daily job. The flow is: S3 notification -> SQS message -> consumer Lambda
invocation -> downstream Databricks job.

Confirm the consumer ran by tailing its logs:

```
aws logs tail /aws/lambda/databricks-sqs-consumer --region us-east-2 --follow
```

Then confirm the downstream job started on the Databricks jobs page.

The full chain has been verified end to end: a real object-created event drives
the consumer Lambda to trigger the downstream Databricks job.

## Schedule

The daily job runs at 12:00 UTC (5:00 AM Pacific Daylight Time). Adjust the
`schedule_expression` in the `aws_cloudwatch_event_rule` resource to change the
time. EventBridge cron expressions are always in UTC regardless of the
deployment region.

## Updating the Lambda code

After editing `src/index.py`, run `terraform apply` again. Terraform repackages
the source and pushes the new code to both functions automatically; no separate
upload step is needed.

## Teardown

```
terraform destroy
```

Notes:

- The Secrets Manager secret is scheduled for deletion with a recovery window
  rather than removed immediately, so recreating with the same name shortly
  after a destroy may require forcing deletion first:
  `aws secretsmanager delete-secret --secret-id databricks-daily-trigger-token --force-delete-without-recovery --region us-east-2`
- The S3 bucket is referenced as an existing external resource, so `destroy`
  does not delete it. It does remove the notification configuration this project
  added.

## Notes and possible extensions

- The Lambda's `run-now` call starts a Databricks job and returns a run ID
  immediately; it does not wait for the job to finish. Because stage 2 depends on
  stage 1's job actually completing and writing its file, the chain is driven by
  the S3 object landing, not by the Lambda invocation succeeding. Job outcomes
  are monitored on the Databricks side.
- The consumer Lambda deletes an SQS message only after a successful
  invocation. If the handler errors, the message returns to the queue and
  retries. Adding a dead-letter queue would capture messages that repeatedly
  fail, rather than letting them retry indefinitely; this is the recommended
  next hardening step for the downstream stage.
- A CloudWatch alarm on Lambda errors would provide alerting if either function
  fails. Both Lambdas already write logs to CloudWatch via the
  `AWSLambdaBasicExecutionRole`.
