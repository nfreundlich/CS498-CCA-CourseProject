terraform {
    backend "s3" {
        encrypt = true
        key = "terraform.tfstate"
        region = "eu-west-1"
    }
}

provider "aws" {
    region = "eu-west-1"
}

module "s3_dev" {
    initials = "${var.initials}"
    source = "./s3"
    stage = "dev"
}

module "s3_prod" {
    initials = "${var.initials}"
    source = "./s3"
    stage = "prod"
}

module "sqs_dev" {
    initials = "${var.initials}"
    source = "./sqs"
    stage = "dev"
}

module "sqs_prod" {
    initials = "${var.initials}"
    source = "./sqs"
    stage = "prod"
}

module "glue_dev" {
    iam_role_arn = "${aws_iam_role.glue.arn}"
    iam_role_id = "${aws_iam_role.glue.id}"
    initials = "${var.initials}"
    s3_bucket_extracted_arn = "${module.s3_dev.s3_bucket_extracted_arn}"
    source = "./glue"
    stage = "dev"
}

module "glue_prod" {
    iam_role_arn = "${aws_iam_role.glue.arn}"
    iam_role_id = "${aws_iam_role.glue.id}"
    initials = "${var.initials}"
    s3_bucket_extracted_arn = "${module.s3_prod.s3_bucket_extracted_arn}"
    source = "./glue"
    stage = "prod"
}

resource "aws_iam_role" "glue" {
    assume_role_policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "sts:AssumeRole",
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Sid": ""
        }
    ]
}
EOF
    name = "AWSGlueServiceRoleCCATED"
}

resource "aws_iam_role_policy_attachment" "glue" {
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
    role = "${aws_iam_role.glue.id}"
}