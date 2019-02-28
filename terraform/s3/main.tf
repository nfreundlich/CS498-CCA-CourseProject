resource "aws_s3_bucket" "cca_ted_raw" {
    acl = "private"
    bucket = "${var.initials}-cca-ted-raw-${var.stage}"
}

resource "aws_s3_bucket" "cca_ted_extracted" {
    acl = "private"
    bucket = "${var.initials}-cca-ted-extracted-${var.stage}"
}