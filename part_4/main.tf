terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

variable "aws_region" {
  default = "us-east-2"
}

variable "databricks_host" {
  type = string
}


variable "databricks_job_id" {
  type = string
}

variable "databricks_consumer_job_id" {
  type        = string
  description = "Databricks job ID triggered by the SQS consumer Lambda"
}

# Package the Lambda source into a zip
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/src"
  output_path = "${path.module}/build/lambda.zip"
}

# Store the Databricks token in Secrets Manager
resource "aws_secretsmanager_secret" "databricks_token" {
  name = "databricks-daily-trigger-token"
}

# IAM role for the Lambda
resource "aws_iam_role" "lambda_role" {
  name = "databricks-trigger-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Let the Lambda read the token secret
resource "aws_iam_role_policy" "read_secret" {
  name = "read-databricks-token"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = aws_secretsmanager_secret.databricks_token.arn
    }]
  })
}

resource "aws_iam_role" "consumer_lambda_role" {
  name = "sqs-consumer-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "consumer_lambda_logs" {
  role       = aws_iam_role.consumer_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Read the same token secret the scheduled Lambda uses
resource "aws_iam_role_policy" "consumer_read_secret" {
  name = "read-databricks-token"
  role = aws_iam_role.consumer_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "secretsmanager:GetSecretValue"
      Resource = aws_secretsmanager_secret.databricks_token.arn
    }]
  })
}

# Let Lambda pull and delete messages from the queue
resource "aws_iam_role_policy" "consumer_read_queue" {
  name = "consume-sqs"
  role = aws_iam_role.consumer_lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ]
      Resource = aws_sqs_queue.json_events.arn
    }]
  })
}

# The Lambda function
resource "aws_lambda_function" "databricks_trigger" {
  function_name    = "databricks-daily-trigger"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  handler          = "index.handler"
  runtime          = "python3.12"
  role             = aws_iam_role.lambda_role.arn
  timeout          = 30

  environment {
    variables = {
      DATABRICKS_HOST             = var.databricks_host
      DATABRICKS_JOB_ID           = var.databricks_job_id
      DATABRICKS_TOKEN_SECRET_ARN = aws_secretsmanager_secret.databricks_token.arn
    }
  }
}

resource "aws_lambda_function" "sqs_consumer" {
  function_name    = "databricks-sqs-consumer"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  handler          = "index.handler"
  runtime          = "python3.12"
  role             = aws_iam_role.consumer_lambda_role.arn
  timeout          = 30

  environment {
    variables = {
      DATABRICKS_HOST             = var.databricks_host
      DATABRICKS_JOB_ID           = var.databricks_consumer_job_id
      DATABRICKS_TOKEN_SECRET_ARN = aws_secretsmanager_secret.databricks_token.arn
    }
  }
}

# EventBridge daily schedule
resource "aws_cloudwatch_event_rule" "daily" {
  name                = "databricks-daily-trigger"
  schedule_expression = "cron(0 12 * * ? *)"  # 12:00 UTC = 5:00 AM PDT
}

resource "aws_cloudwatch_event_target" "lambda" {
  rule      = aws_cloudwatch_event_rule.daily.name
  target_id = "lambda"
  arn       = aws_lambda_function.databricks_trigger.arn
}

resource "aws_lambda_permission" "eventbridge" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.databricks_trigger.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily.arn
}

resource "aws_lambda_event_source_mapping" "sqs_to_consumer" {
  event_source_arn = aws_sqs_queue.json_events.arn
  function_name    = aws_lambda_function.sqs_consumer.arn
  batch_size       = 1
}

# --- S3 -> SQS pipeline (separate from the daily Databricks trigger) ---

# Reference the existing bucket (does not create or manage it)
data "aws_s3_bucket" "json_landing" {
  bucket   = "rearc-quest-107628756615-us-east-2-an"
}

# Queue that gets a message when the target file lands
resource "aws_sqs_queue" "json_events" {
  name     = "json-landing-events"
}

# Allow this specific bucket to send messages to the queue
resource "aws_sqs_queue_policy" "json_events" {
  queue_url = aws_sqs_queue.json_events.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "s3.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.json_events.arn
      Condition = {
        ArnEquals = {
          "aws:SourceArn" = data.aws_s3_bucket.json_landing.arn
        }
      }
    }]
  })
}

# Notify the queue when this specific object is created
resource "aws_s3_bucket_notification" "json_landing" {
  bucket   = data.aws_s3_bucket.json_landing.id

  queue {
    queue_arn     = aws_sqs_queue.json_events.arn
    events        = ["s3:ObjectCreated:*"]
    filter_prefix = "datausa/"
    filter_suffix = "annual_us_pop_2013_thru_2024.json"
  }

  depends_on = [aws_sqs_queue_policy.json_events]
}