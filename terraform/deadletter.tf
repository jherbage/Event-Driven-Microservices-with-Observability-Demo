resource "aws_sqs_queue" "dead_letter_queue" {
  name                      = "dead-letter-queue"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 1209600 # 14 days to process them
}

output "dead_letter_queue_arn" {
  value = aws_sqs_queue.dead_letter_queue.arn
}

output "dead_letter_queue_url" {
  value = aws_sqs_queue.dead_letter_queue.id
}