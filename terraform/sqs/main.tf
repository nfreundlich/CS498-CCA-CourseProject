resource "aws_sqs_queue" "batch_job" {
    name = "${var.initials}_cca_ted_batch_job_${var.stage}"
    visibility_timeout_seconds = 900
}