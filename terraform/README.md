These steps assume you've got some [AWS credentials set up locally](https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html).

1. [Install Terraform](https://learn.hashicorp.com/terraform/getting-started/install.html)
2. Create an S3 bucket, in eu-west-3 (Paris), to store the Terraform state in. When you run `terraform init`, you'll be asked for its name.
3. `cd` into this directory
4. Run `terraform init`
5. Run `terraform apply`