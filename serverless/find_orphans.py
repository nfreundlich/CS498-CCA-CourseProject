import json
import boto3

raw_bucket_name = "2-cca-ted-raw-dev"
extracted_bucket_name = "2-cca-ted-extracted-dev"

s3 = boto3.resource('s3')
sqs = boto3.client('sqs')

rBucket = s3.Bucket(raw_bucket_name)
eBucket = s3.Bucket(extracted_bucket_name)

def get_raw_files(year, month):
    # get list of raw files
    raw_files = []
    raw_keys = []
    for item in rBucket.objects.filter(Prefix=year + "/" + month):
        file_name = item.key.split("/")[-1].split(".")[0]
        raw_files.append(file_name)
        raw_keys.append(item.key)    
    
    return raw_files, raw_keys
    
def get_extracted_files(year, month):
    # get list of extracted files
    extracted_files = []
    for item in eBucket.objects.filter(Prefix=year+month):
        file_name = item.key.split(".")[0]
        extracted_files.append(file_name)
    
    return extracted_files
    
def compare_files(raw_files, raw_keys, extracted_files):
    missing_files = []
    # loop through the raw files and see which have not been extracted
    for i, rFile in enumerate(raw_files):
        if rFile not in extracted_files:
            missing_files.append(raw_keys[i])
            sqs.send_message(
                    QueueUrl=f'https://sqs.eu-west-3.amazonaws.com/241824408229/2_cca_ted_extractions_dev',
                    MessageBody=json.dumps({
                        'key':raw_keys[i]
                    })
                )
#         break
    
    return missing_files
    
def lambda_handler(event, context):
    year = event['year']
    month = event['month']
    
    raw_files, raw_keys = get_raw_files(year, month)
    extracted_files = get_extracted_files(year, month)
    
    missing_files = compare_files(raw_files, raw_keys, extracted_files)
    
    return {
        'statusCode': 200,
        'body': json.dumps(missing_files)
    }
