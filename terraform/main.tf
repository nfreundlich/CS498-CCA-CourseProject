terraform {
    backend "s3" {
        encrypt = true
        key = "terraform.tfstate"
        region = "eu-west-3"
    }
}

provider "aws" {
    region = "eu-west-3"
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