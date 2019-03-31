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
        }
    ]
}
EOF
    role = "${var.iam_role_id}"
}

# IAM
resource "aws_iam_role_policy" "glue_service" {
    name = "cca_ted_glue_service_${var.stage}"
    policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData",
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeRouteTables",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
                "glue:*",
                "iam:ListRolePolicies",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListAllMyBuckets",
                "s3:GetBucketAcl"
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
                "s3:DeleteObject",
                "s3:GetObject",
                "s3:PutObject"
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
                "arn:aws:s3:::aws-glue-*",
                "arn:aws:s3:::crawler-public*"
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
                "arn:aws:ec2:*:*:instance/*",
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*"
            ]
        }
    ]
}
EOF
    role = "${var.iam_role_id}"
}

resource "aws_glue_job" "merge_files" {
  command {
    script_location = "s3://${var.initials}-glue-scripts-${var.stage}/merge_files.py"
  }
  default_arguments = {
    "--BUCKET" = "${var.initials}-cca-ted-extracted-${var.stage}",
    "--YEAR" = "2019"
  }
  name = "merge_files_${var.stage}"
  role_arn = "${var.iam_role_arn}"
}