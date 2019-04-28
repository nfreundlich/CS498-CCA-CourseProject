resource "aws_s3_bucket" "raw" {
    acl = "private"
    bucket = "${var.initials}-cca-ted-raw-${var.stage}"
}

resource "aws_s3_bucket" "extracted" {
    acl = "private"
    bucket = "${var.initials}-cca-ted-extracted-${var.stage}"
}

resource "aws_s3_bucket" "fetch_recommendations" {
    acl = "private"
    bucket = "${var.initials}-cca-ted-fetch-recommendations-${var.stage}"
}

resource "aws_s3_bucket" "glue_scripts" {
    acl = "private"
    bucket = "${var.initials}-glue-scripts-${var.stage}"
}

resource "aws_s3_bucket_object" "make_recommendations_script" {
  bucket = "${var.initials}-glue-scripts-${var.stage}"
  etag = "${md5(file("../glue/make_recommendations.py"))}"
  key    = "make_recommendations.py"
  source = "../glue/make_recommendations.py"
}

resource "aws_s3_bucket_object" "merge_files_script" {
  bucket = "${var.initials}-glue-scripts-${var.stage}"
  etag = "${md5(file("../glue/merge_files.py"))}"
  key    = "merge_files.py"
  source = "../glue/merge_files.py"
}

output "s3_bucket_extracted_arn" {
    value = "${aws_s3_bucket.extracted.arn}"
}