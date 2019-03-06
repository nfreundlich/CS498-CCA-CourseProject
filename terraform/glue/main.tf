resource "aws_glue_catalog_database" "extracted" {
    location_uri = "s3://${var.initials}-cca-ted-extracted-${var.stage}"
    name = "cca_ted_extracted_${var.stage}"
}

resource "aws_glue_crawler" "extracted" {
    database_name = "${aws_glue_catalog_database.extracted.name}"
    name = "cca_ted_extracted_${var.stage}"
    role = "${var.iam_role_arn}"
    s3_target = [
        {
            path = "s3://${var.initials}-cca-ted-extracted-${var.stage}"
        }
    ]
}

# IAM
resource "aws_iam_role_policy" "crawler" {
    name = "cca_ted_crawler_${var.stage}"
    policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetObject",
                "s3:PutObject"
            ],
            "Effect": "Allow",
            "Resource": "${var.s3_bucket_extracted_arn}*"
        }
    ]
}
EOF
    role = "${var.iam_role_id}"
}