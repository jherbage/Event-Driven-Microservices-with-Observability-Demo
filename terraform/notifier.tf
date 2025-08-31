resource "aws_sns_topic" "job_end_state_topic" {
  name = "job-end-state-topic"
}

output "job_end_state_topic_arn" {
  value = aws_sns_topic.job_end_state_topic.arn
}