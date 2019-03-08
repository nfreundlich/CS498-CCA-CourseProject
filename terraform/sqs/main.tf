resource "aws_sqs_queue" "transfers" {
    name = "${var.initials}_cca_ted_transfers_${var.stage}"
    visibility_timeout_seconds = 900
}

resource "aws_sqs_queue" "extractions" {
    name = "${var.initials}_cca_ted_extractions_${var.stage}"
    visibility_timeout_seconds = 900
}