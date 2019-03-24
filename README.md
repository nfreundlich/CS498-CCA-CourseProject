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

**Add a step function**:
- define a state machine

**Glue**:
#TODO
launch the glue job manually after initial stuff is done
launch glue for each year
gotta download the file
then regenerate the year - every day?
3-4 megs / daily file

ERIC's thing:
Hi. For Glue, make sure you have pulled from branch glue_merge because the data files created with older versions of the extract_xml script won't work. I've been creating the jobs using the AWS interface (https://eu-west-1.console.aws.amazon.com/glue/home?region=eu-west-1#etl:tab=jobs), but it will probably be automated at some point. Click "Add Job", make sure the type is "Spark" and the language is "Python", make sure an IAM role is chosen that has permissions to read from and write to S3. You can use any datasource, I don't actually read from any of the databases. For "Choose a data target" select "Create tables in your data target", Data store is Amazon S3, Format is parquet, and target path is the extracted bucket with "/merged" on the end of it. Then you can click through the rest of the screens until you get to the script at which point cut and paste the contents of glue/glue_load_from_directory.py in, making sure you change the value of "s3_extracted_bucket" because it currently points to my bucket.
To run it click on "Run job" from the edit script page and make sure you open the "Security configuration, script libraries and job parameters" section and under Job parameters add key "--YEAR" with the value the year you want to merge.
I want to emphasize that the script will fail unless you have extracted the data with the latest version of the extract_xml_lambda.py so make sure you have pulled from branch glue_merge and then deployed the code from the serverless directory.

Better way to set up the Glue job from the interface. Click "Add job", in the first screen make sure "ETL Language" is Python and under "This job runs" select "an existing script that you provide." Click on next and then "Save job and edit script." Paste the contents of the .py file in. I'm not sure how to add the Glue job programmatically, I imagine that would go through terraform.

**Athena**:
#TODO
--> already done from glue

**Quicksight**:
#TODO

**How to test this**:
--> setup a local falke AWS resource
--> called localstack - fakes AWS API-s, bring up the localstack
--> write tests against local stack
