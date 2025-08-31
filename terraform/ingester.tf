resource "aws_sqs_queue" "ingester_queue" {
  name = "ingester-queue"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 1209600

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter_queue.arn
    maxReceiveCount     = 5 
  })
}

resource "aws_iam_role" "eventbridge_to_sqs_role" {
  name = "eventbridge-to-sqs-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "eventbridge_to_sqs_policy" {
  name        = "eventbridge-to-sqs-policy"
  description = "Policy to allow EventBridge to send messages to SQS"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sqs:SendMessage",
        Effect = "Allow",
        Resource = aws_sqs_queue.ingester_queue.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "eventbridge_to_sqs_attachment" {
  role       = aws_iam_role.eventbridge_to_sqs_role.name
  policy_arn = aws_iam_policy.eventbridge_to_sqs_policy.arn
}

resource "aws_cloudwatch_event_rule" "ingester_rule" {
  name        = "ingester-event-rule"
  description = "Ingester EventBridge rule"
  event_pattern = jsonencode({
    source = ["jobs"]
  })
}

resource "aws_cloudwatch_event_target" "ingester_target" {
  rule      = aws_cloudwatch_event_rule.ingester_rule.name
  arn       = aws_sqs_queue.ingester_queue.arn
  role_arn  = aws_iam_role.eventbridge_to_sqs_role.arn
}

# Create an event source mapping between the SQS queue and the Lambda function
resource "aws_lambda_event_source_mapping" "ingester_event_source" {
  event_source_arn = aws_sqs_queue.ingester_queue.arn
  function_name    = aws_lambda_alias.ingester_alias.arn # Use the alias ARN for the Lambda function
  batch_size       = 1
  enabled          = true
}

resource "aws_lambda_function" "ingester" {
  function_name = "ingester"
  description   = "Job Ingester"
  memory_size   = 128
  timeout       = 20
  handler       = "bootstrap"
  runtime       = "provided.al2"
  publish       = true # Automatically publish a version

  role = aws_iam_role.lambda_execution_role.arn

  s3_bucket = aws_s3_bucket.lambda.bucket 
  s3_key    = "ingester.zip"

  dead_letter_config {
    target_arn = aws_sqs_queue.dead_letter_queue.arn
  }
  
  depends_on = [
    aws_s3_object.ingester_zip
  ]         
}

# Create an alias for the published version
resource "aws_lambda_alias" "ingester_alias" {
  name             = "prod"
  function_name    = aws_lambda_function.ingester.function_name
  function_version = aws_lambda_function.ingester.version
}

# Apply provisioned concurrency to the alias
resource "aws_lambda_provisioned_concurrency_config" "ingester_provisioned" {
  function_name          = aws_lambda_function.ingester.function_name
  qualifier              = aws_lambda_alias.ingester_alias.name
  provisioned_concurrent_executions = 2
}

resource "aws_iam_role" "lambda_execution_role" {
  name               = "lambda_execution_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name   = "lambda_policy"
  role   = aws_iam_role.lambda_execution_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
        ]
        Effect   = "Allow"
        Resource = "*"
      },
      {
        Action = [
          "sqs:SendMessage",
        ]
        Effect   = "Allow"
        Resource = aws_sqs_queue.jobs_todo.arn
      }
    ]
  })
}
