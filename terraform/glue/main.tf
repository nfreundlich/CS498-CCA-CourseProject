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
            path = "s3://${var.initials}-cca-ted-extracted-${var.stage}/merged"
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
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListAllMyBuckets",
                "s3:GetBucketAcl",
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeRouteTables",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
                "iam:ListRolePolicies",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "cloudwatch:PutMetricData"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:*"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*/*",
                "arn:aws:s3:::*/*aws-glue-*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::crawler-public*",
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:*:*:/aws-glue/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": [
                        "aws-glue-service-resource"
                    ]
                }
            },
            "Resource": [
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*",
                "arn:aws:ec2:*:*:instance/*"
            ]
        }
    ]
}
EOF
    role = "${var.iam_role_id}"
}

# Bucket for Glue script
resource "aws_s3_bucket" "raw" {
    acl = "private"
    bucket = "${var.initials}-glue-script-${var.stage}"
}

resource "aws_s3_bucket_object" "object" {
  bucket = "${var.initials}-glue-script-${var.stage}"
  key    = "glue_merge_script.py"
  source = "../glue/glue_load_from_directory.py"
  # The filemd5() function is available in Terraform 0.11.12 and later
  # For Terraform 0.11.11 and earlier, use the md5() function and the file() function:
  # etag = "${md5(file("path/to/file"))}"
  etag = "${md5("../glue/glue_load_from_directory.py")}"
}

# create the glue job
resource "aws_glue_job" "example" {
  name     = "${var.initials}-glue-merge-${var.stage}"
  role_arn = "${var.iam_role_arn}"

  command {
    script_location = "s3://${var.initials}-glue-script-${var.stage}/glue_merge_script.py"
  }

  default_arguments = {
    "--BUCKET" = "${var.initials}-cca-ted-extracted-${var.stage}",
    "--YEAR" = "2019"
  }
}