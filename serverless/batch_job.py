import json
import logging
import os

import boto3
from fs import open_fs
from fs.walk import Walker

import extract_xml_lambda

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

s3_raw_bucket = f'{os.environ["INITIALS"]}-cca-ted-raw-{os.environ["STAGE"]}'
s3_extracted_bucket = f'{os.environ["INITIALS"]}-cca-ted-extracted-{os.environ["STAGE"]}'

# TODO Handler errors and exceptions

def start_transfer_job(event, context):
    logger.info('Starting transfer job')
    dir_to_walk = 'daily-packages/'
    if 'year' in event and 'month' in event:
        dir_to_walk = f'{dir_to_walk}{event["year"]}/{event["month"]}/'
    logger.info('Walking %s', dir_to_walk)
    with open_fs(f'ftp://guest:guest@ted.europa.eu/{dir_to_walk}') as fs:
        sqs = boto3.client('sqs')
        for path in fs.walk.files(filter=['*.tar.gz']):
            # TODO Batch and send
            sqs.send_message(
                QueueUrl=f'https://sqs.eu-west-3.amazonaws.com/{os.environ["AWS_ACCOUNT_ID"]}/{os.environ["INITIALS"]}_cca_ted_transfers_{os.environ["STAGE"]}',
                MessageBody=json.dumps({
                    'path': dir_to_walk + path[1:]
                })
            )
    logger.info('Finished walking %s', dir_to_walk)
    logger.info('Finished starting transfer job')
    return {
        'statusCode': 200
    }

def process_transfers(event, context):
    logger.info('Processing transfers')
    paths = [json.loads(record['body'])['path'] for record in event['Records']]
    _transfer_files_from_ftp_to_s3(paths)
    logger.info('Finished processing transfers')
    return {
        'statusCode': 200
    }

def _transfer_files_from_ftp_to_s3(paths):
    with open_fs('ftp://guest:guest@ted.europa.eu/') as ftp_fs:
        for path in paths:
            # TODO Don't do if already in S3
            tmp_path = f'/tmp/{os.path.split(path)[1]}'
            with open(tmp_path, 'wb') as tmp_file:
                logger.info('Downloading %s to %s', path, tmp_path)
                ftp_fs.download(path, tmp_file)
                logger.info('Finished downloading %s to %s', path, tmp_path)
            s3_key = path.replace('daily-packages/', '')
            full_s3_path = f'{s3_raw_bucket}/{s3_key}'
            logger.info('Uploading %s to %s', tmp_path, full_s3_path)
            s3.upload_file(
                tmp_path,
                s3_raw_bucket,
                s3_key
            )
            logger.info('Finished uploading %s to %s', tmp_path, full_s3_path)
            os.remove(tmp_path)

def start_extraction_job(event, context):
    pass

def process_extractions(event, context):
    paths = [json.loads(record['body'])['path'] for record in event['Records']]
    _transfer_files_from_ftp_to_s3(paths)
    logger.info('Downloading files from S3')
    downloaded_file_paths = _download_files_from_s3(paths)
    logger.info('Finished downloading files from S3')
    logger.info('Extracting files')
    extract_xml_lambda.extract_files(downloaded_file_paths)
    logger.info('Finished extracting files')
    logger.info('Writing to Parquet')
    df = extract_xml_lambda.load_data('/tmp')
    file_name = downloaded_file_paths[0].split('/')[-1].split('.')[0] + '.parquet'
    df.to_parquet('/tmp/' + file_name)
    logger.info('Finished writing to Parquet')
    s3 = boto3.client('s3')
    logger.info('Uploading to S3')
    s3.upload_file(
        Filename = os.path.join('/tmp/', file_name),
        Bucket = s3_extracted_bucket,
        Key = file_name
    )
    logger.info('Finished uploading to S3')
    return {
        'statusCode': 200
    }

def _download_files_from_s3(paths):
    downloaded_file_paths = []
    for path in paths:
        s3_key = path.replace('daily-packages/', '')
        tmp_path = os.path.join('/tmp', os.path.split(path)[1])
        s3.download_file(s3_raw_bucket, s3_key, tmp_path)
        downloaded_file_paths.append(tmp_path)
    return downloaded_file_paths