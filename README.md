# System architecture

Diagram [here](https://docs.google.com/drawings/d/1mLhY9xiNVu2kDNetq86GDVVuERsA6yTmXaHhlLxCL_c/edit)

# Project setup

## 1. Terraform setup

To set up AWS buckets, follow steps in ./terraform/README.md.

## 2. Serverless

We use the Serverless framework to develop and deploy AWS Lambda functions (https://serverless.com/framework/docs/providers/aws/guide/intro/).

* **Install node.js**: Download [here](https://nodejs.org/en/download/)

* **Install serverless** Documentation [here](https://serverless.com/framework/docs/providers/aws/guide/installation/)

* **Setup AWS credentials**: Documentation [here](https://serverless.com/framework/docs/providers/aws/guide/credentials/)

* Install plugins: `serverless pugin install --name serverless-python-requirements --initials nfr --aws-account-id 414969896231 --stage dev`

**Create SQS queue**:
* Doc [here](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-create-queue.html)
* To read messages in a queue:
    * `aws sqs receive-message --queue-url https://sqs.eu-west-1.amazonaws.com/[queue identifier] 
--region 'eu-west-1'`


**Deploy**:
* `cd lambda-layers; serverless deploy`
* `cd serverless; serverless deploy`

* Remark: layer version [here](https://eu-west-1.console.aws.amazon.com/lambda/home?region=eu-west-1#/layers)

**Install aws cli**:
Documentation [here](https://docs.aws.amazon.com/cli/latest/userguide/install-macos.html)

**Check your S3 buckets**:
 aws s3 ls "[your initials]-cca-ted-raw-dev/2019/03/"

## 3. Lambdas
* `cd serverless`

**Start transfer job**:
* `sls invoke -f start_transfer_job --aws-account-id [your account id] 
                                 --initials [your initials] 
                                 --layers-version 6 
                                 --stage dev 
                                 --data '{"year": 2019, "month": 3}'`

**Start extract job**:
* `sls invoke -f start_extract_job --aws-account-id 414969896231 --initials nfr --layers-version 6 --stage dev`


**Glue**:
#TODO

**Athena**:
#TODO

