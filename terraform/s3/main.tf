resource "aws_s3_bucket" "raw" {
    acl = "private"
    bucket = "${var.initials}-cca-ted-raw-${var.stage}"
}

resource "aws_s3_bucket" "extracted" {
    acl = "private"
    bucket = "${var.initials}-cca-ted-extracted-${var.stage}"
}

output "s3_bucket_extracted_arn" {
    value = "${aws_s3_bucket.extracted.arn}"
}