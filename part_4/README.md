# Part 4: Databricks Data Pipeline (Terraform)

This project provisions a chained, serverless data pipeline on AWS using
Terraform, deployed in the `us-east-2` region. It automates the quest: a daily
Databricks job runs Parts 1 and 2 (BLS sync and DataUSA API fetch) and writes a
JSON file to S3; the arrival of that file automatically triggers a second
Databricks job that runs the Part 3 analytics and reports.

The Lambdas orchestrate; Databricks does the data work. See the root
`README.md` for the rationale behind that division.

The flow has two stages:

1. **Scheduled trigger (head of the chain)** - an EventBridge schedule invokes a
   Lambda function once per day, which reads a Databricks token from Secrets
   Manager and starts the `parts_1_and_2` Databricks Job via the Jobs API. That
   job runs the Part 1 notebook then the Part 2 notebook, and Part 2 writes the
   population JSON file to S3.
2. **Event-driven downstream stage** - writing that JSON file to S3 emits an
   S3 event notification, which places a message on an SQS queue. A consumer
   Lambda is invoked automatically by that queue and starts the `part_3`
   Databricks Job, which runs the Part 3 analytics notebook and refreshes the
   analysis dashboard.

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
  Lambda -> "parts_1_and_2" job ---- writes JSON file --> S3 bucket
   |         (Part 1 then Part 2)                            |
   v                                                         v
  Secrets Manager (token)                      S3 event notification (.json)
                                                            |
                                                            v
                                                        SQS queue
                                                            |
                                                            v
                                    Consumer Lambda -> "part_3" job
                                     |                  (analytics + dashboard)
                                     v
                             Secrets Manager (token)
```

## Files

- `main.tf` - all Terraform resources for the pipeline
- `terraform.tfvars` - non-secret configuration (workspace host, job IDs)
- `src/index.py` - the Lambda handler, shared by both Lambda functions
- `databricks_jobs/parts_1_and_2.yaml` - definition of the daily job (Part 1
  then Part 2, sourced from Git)
- `databricks_jobs/part_3.yaml` - definition of the downstream job (Part 3
  analytics then dashboard refresh, sourced from Git)

Both Lambda functions run the same handler. They differ only by the
`DATABRICKS_JOB_ID` environment variable, so each triggers a different
Databricks job from the same code.

## The Databricks side (required for the pipeline to run)

The Lambdas trigger Databricks Jobs by numeric ID, so those jobs must exist in
the target workspace before the pipeline can run end to end. The job definitions
are included here as declarative YAML under `databricks_jobs/`, and both source
their notebooks from the project's Git repository rather than from workspace
state. To reproduce:

- Create the `parts_1_and_2` and `part_3` jobs in the Databricks workspace from
  the YAML definitions.
- Note each job's numeric ID and set them in `terraform.tfvars`
  (`databricks_job_id` for `parts_1_and_2`, `databricks_consumer_job_id` for
  `part_3`).
- Provision a Databricks personal access token and place it in Secrets Manager
  (see the deploy steps).

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
- A Databricks workspace with the `parts_1_and_2` and `part_3` jobs defined, and
  a personal access token
- Read access to the target S3 bucket, plus permission to set its notification
  configuration

## Configuration

Edit `terraform.tfvars` and set your workspace URL and both job IDs:

```hcl
databricks_host            = "https://your-workspace.cloud.databricks.com"
databricks_job_id          = "123456789"   # the parts_1_and_2 job (stage 1)
databricks_consumer_job_id = "987654321"   # the part_3 job (stage 2)
```

Each job ID is the numeric ID shown in the URL when you open the job in
Databricks. The token is not set here; see the deploy steps.

The S3 bucket that receives the JSON file is referenced by name in `main.tf`
as an existing bucket (it is not created or managed by this Terraform). The
event notification is filtered to the specific object key the Part 2 notebook
writes. Update the bucket name and key filters in the
`aws_s3_bucket_notification` resource if the job writes to a different location.

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
consume any messages already on the queue, which triggers the `part_3` job. If
test messages are already queued, expect a downstream job run right after apply.

## Testing

### Stage 1 - the parts_1_and_2 job

Invoke the scheduled Lambda manually rather than waiting for the schedule:

```
aws lambda invoke --function-name databricks-daily-trigger response.json
cat response.json
```

A response containing a `run_id` means the `parts_1_and_2` job was triggered.
When Part 2 writes its JSON file to S3, it sets off stage 2 automatically.

### Stage 2 - S3 to SQS to the part_3 job

Stage 2 is triggered by the JSON file landing in S3. It can be exercised
directly by writing (or re-writing) the target object, without waiting for the
daily job. The flow is: S3 notification -> SQS message -> consumer Lambda
invocation -> `part_3` job.

Confirm the consumer ran by tailing its logs:

```
aws logs tail /aws/lambda/databricks-sqs-consumer --region us-east-2 --follow
```

Then confirm the `part_3` job started on the Databricks jobs page.

The full chain has been verified end to end: a real object-created event drives
the consumer Lambda to trigger the `part_3` Databricks job.

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
  the `parts_1_and_2` job actually completing and writing its file, the chain is
  driven by the S3 object landing, not by the Lambda invocation succeeding. Job
  outcomes are monitored on the Databricks side.
- The consumer Lambda deletes an SQS message only after a successful
  invocation. If the handler errors, the message returns to the queue and
  retries. Adding a dead-letter queue would capture messages that repeatedly
  fail, rather than letting them retry indefinitely; this is the recommended
  next hardening step for the downstream stage.
- A CloudWatch alarm on Lambda errors would provide alerting if either function
  fails. Both Lambdas already write logs to CloudWatch via the
  `AWSLambdaBasicExecutionRole`.
