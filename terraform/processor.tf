resource "aws_sqs_queue" "jobs_todo" {
  name = "jobs-todo"

  visibility_timeout_seconds = 30
  message_retention_seconds  = 1209600

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dead_letter_queue.arn
    maxReceiveCount     = 5 
  })
}

resource "aws_iam_role" "processor_lambda_execution_role" {
  name = "processor-lambda-execution-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  # Attach permissions for SQS and CloudWatch Logs
  inline_policy {
    name = "processor-lambda-policy"
    policy = jsonencode({
      Version = "2012-10-17",
      Statement = [
        {
          Action = [
            "sqs:ReceiveMessage",
            "sqs:DeleteMessage",
            "sqs:GetQueueAttributes"
          ],
          Effect = "Allow",
          Resource = aws_sqs_queue.jobs_todo.arn
        },
        {
          Action = [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ],
          Effect = "Allow",
          Resource = "arn:aws:logs:*:*:*"
        }
      ]
    })
  }
}

resource "aws_lambda_function" "processor" {
  function_name = "processor"
  description   = "Processes messages from the queue"
  runtime       = "provided.al2"
  handler       = "bootstrap"
  memory_size   = 128
  timeout       = 900 # 15 minutes to allow for the long running jobs

  role          = aws_iam_role.processor_lambda_execution_role.arn

  s3_bucket = aws_s3_bucket.lambda.bucket 
  s3_key    = "processor.zip"

  depends_on = [
    aws_s3_object.processor_zip
  ]
  
  dead_letter_config {
    target_arn = aws_sqs_queue.dead_letter_queue.arn
  }
}

resource "aws_lambda_event_source_mapping" "processor_event_source" {
  event_source_arn = aws_sqs_queue.jobs_todo.arn
  function_name    = aws_lambda_function.processor.arn
  batch_size       = 1
  enabled          = true
}


# Create an alias for the published version
resource "aws_lambda_alias" "processor_alias" {
  name             = "prod"
  function_name    = aws_lambda_function.processor.function_name
  function_version = aws_lambda_function.processor.version
}

# Apply provisioned concurrency to the alias
resource "aws_lambda_provisioned_concurrency_config" "processor_provisioned" {
  function_name          = aws_lambda_function.processor.function_name
  qualifier              = aws_lambda_alias.processor_alias.name
  provisioned_concurrent_executions = 2
}