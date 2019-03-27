# System architecture

Diagram [here](https://docs.google.com/drawings/d/1mLhY9xiNVu2kDNetq86GDVVuERsA6yTmXaHhlLxCL_c/edit)

# Project setup

## 1. Terraform setup

To set up AWS buckets, follow steps in ./terraform/README.md.

Remark: if you change the bucket configuration in terraform, you need to delete the _.terraform_ folder and run the initialization again.

## 2. Serverless

We use the [Serverless](https://serverless.com/framework/docs/providers/aws/guide/intro/) framework to deploy AWS Lambda functions.

* **Install node.js**: Download [here](https://nodejs.org/en/download/)

* **Install serverless** Documentation [here](https://serverless.com/framework/docs/providers/aws/guide/installation/)

* **Setup AWS credentials**: Documentation [here](https://serverless.com/framework/docs/providers/aws/guide/credentials/)

* **Install plugins**: `serverless pugin install --name serverless-python-requirements --initials [initials] --aws-account-id [account id] --stage [stage] --layers-version [version]`

* **The configuration will create**":
    * S3 buckets (adding initials and staging environment for each): cca-ted-raw, cca-ted-extracted, glue-script
    * SQS queues: cca_ted_transfers, cca_ted_extractions
    * Glue jobs (glue-merged) and crawlers (ted-extracted)

**Install aws cli**:
Documentation [here](https://docs.aws.amazon.com/cli/latest/userguide/install-macos.html)

**Deploy**:
* Deploy lambda layers: `cd lambda-layers; serverless deploy`
* Deploy serverless application: `cd serverless; serverless deploy --aws-account-id [account id] --initials [initials] --layers-version [version] --stage [stage]`
* Remark: layer version [here](https://eu-west-1.console.aws.amazon.com/lambda/home?region=eu-west-1#/layers)


## 3. Initial transfer and extract job
**Deploy lambda functions**:
* `cd serverless`
* `sls deploy --aws-account-id [account id] --initials nfr --layers-version [version] --stage [stage]`

**Start transfer job**:
* Transfer files via ftp to S3 raw bucket
* `sls invoke -f start_transfer_job --aws-account-id [your account id] 
                                 --initials [your initials] 
                                 --layers-version [version] 
                                 --stage dev 
                                 --data '{"year": 2019, "month": 3}'`

**Start extract job**:
* Extracts previously transferred files to S3 extracted bucket
* `sls invoke -f start_extract_job --aws-account-id [account id] --initials [initials] --layers-version [version] --stage dev`

**Using a step function / state machine**:
* TBD

**Glue**:
* The glue job is executed manually from AWS console
* The job (glue_merge) takes 2 parameters, _--YEAR_ and _--BUCKET_, already initialized during job creation, 
        with your initials and year 2019. This is configurable from Glue job parameters interface in AWS console. 

**Crawlers**:
* Once the glue job finished with success, we have to run the crawlers
* From AWS console - Glue, run _ted_extracted_ crawler. Based on the glue script's output, 
        this will generate a table (_merged_) readable in Athena. 

**Athena**:
* Athena can access the table _merged_, generated from _ted_extracted_dev_ database 

**Quicksight**:
* Create a QuickSight account
* During the creation, the account must be set up to allow access to both Athena and the related S3 bucket (_ted_extracted_dev_)
* Import data to SPICE and create visualization based on it 

**Other considerations**:
* Ideas for testing the serverless enviroment: if time allows, set up a localstack enviroment and run unit tests locally

