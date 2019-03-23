from datetime import datetime
import itertools
import json
import logging
import os

import boto3
from fs import open_fs
from fs.errors import CreateFailed
from fs.walk import Walker

import extract_xml_lambda

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sqs = boto3.client('sqs')

s3_raw_bucket = f'{os.environ["INITIALS"]}-cca-ted-raw-{os.environ["STAGE"]}'
s3_extracted_bucket = f'{os.environ["INITIALS"]}-cca-ted-extracted-{os.environ["STAGE"]}'

# TODO Handler errors and exceptions


def start_transfer_job(event, context):
    """
    Launch files transfer from ftp to s3.
    Usage: sls invoke -f start_transfer_job --aws-account-id [your account id]
                                 --initials [your initials]
                                 --layers-version 6
                                 --stage dev
                                 --data '{"year": 2019, "month": 3}'
    :param event: --data '{"year": 2019, "month": 3}'; year and month to be downloaded
    :param context: TBD
    :return: message in sqs
    """
    logger.info('Starting transfer job')
    year, month = None, None
    if 'year' in event and 'month' in event:
        year = event['year']
        month = event['month']
    for year_month in _get_year_month_iterator(year, month):
        dir_to_walk = f'daily-packages/{year_month[0]}/{year_month[1]}/'
        try:
            with open_fs('ftp://guest:guest@ted.europa.eu/' + dir_to_walk) as fs:
                logger.info('Walking %s', dir_to_walk)
                for path in fs.walk.files(filter=['*.tar.gz']):
                    # TODO Batch and send
                    sqs.send_message(
                        QueueUrl=f'https://sqs.eu-west-3.amazonaws.com/{os.environ["AWS_ACCOUNT_ID"]}/{os.environ["INITIALS"]}_cca_ted_transfers_{os.environ["STAGE"]}',
                        MessageBody=json.dumps({
                            'path': dir_to_walk + path[1:]
                        })
                    )
                logger.info('Finished walking %s', dir_to_walk)
        except CreateFailed:
            logger.warn('Directory %s not found', dir_to_walk)
    logger.info('Finished starting transfer job')
    return {
        'statusCode': 200
    }

def _get_year_month_iterator(year=None, month=None):
    if year is not None and month is not None:
        years = [year]
        months = [f'{month:02d}']
    else:
        today = datetime.today()
        years = range(2011, today.year + 1)
        months = [f'{month:02d}' for month in range(1, 13)]
    return itertools.product(years, months)

def process_transfers(event, context):
    """
    Downloads files from ftp to s3.
    :param event: TBD
    :param context: TBD
    :return: zipped files to s3 in raw_bucket
    """
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

def start_extract_job(event, context):
    """
    Launch extract job.
    Usage: sls invoke -f start_extract_job --aws-account-id 414969896231 --initials nfr --layers-version 6 --stage dev
    :param event: TBD
    :param context: TBD
    :return: exract info to sqs ted_extraction
    """
    logger.info('Starting extract job')
    s3_key_prefix = ''
    if 'year' in event and 'month' in event:
        s3_key_prefix = f'{event["year"]}/{event["month"]}'
    s3_paginator = s3.get_paginator('list_objects_v2')
    pages = s3_paginator.paginate(Bucket=s3_raw_bucket, Prefix=s3_key_prefix)
    for page in pages:
        for object_ in page['Contents']:
            sqs.send_message(
                QueueUrl=f'https://sqs.eu-west-3.amazonaws.com/{os.environ["AWS_ACCOUNT_ID"]}/{os.environ["INITIALS"]}_cca_ted_extractions_{os.environ["STAGE"]}',
                MessageBody=json.dumps({
                    'key': object_['Key']
                })
            )
    logger.info('Finished starting extract job')
    return {
        'statusCode': 200
    }

def process_extractions(event, context):
    """
    Reads messages from ted_extractions sqs and extracts files.
    :param event: TBD
    :param context: TBD
    :return: parquet files on to s3_extracted bucket
    """
    s3_keys = [json.loads(record['body'])['key'] for record in event['Records']]
    logger.info('Downloading files from S3')
    downloaded_file_paths = _download_files_from_s3(s3_keys)
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
    prefix = file_name[:4]
    
    # check if the event is from a batch job or not
    record_body = json.loads(event['Records'][0]['body'])
    batch_job = True
    if 'batch' in record_body and record_body['batch'] == False:
        batch_job = False
        
    # if it is a batch job upload to the year/month key
    if batch_job:
        s3.upload_file(
            Filename = os.path.join('/tmp/', file_name),
            Bucket = s3_extracted_bucket,
            Key = prefix + "/" + file_name
        )
    # else upload to merged/new_data/ so the file is available for queries, but still kept separate for future merging
    else:
        s3.upload_file(
            Filename = os.path.join('/tmp/', file_name),
            Bucket = s3_extracted_bucket,
            Key = "merged/new_data/" + file_name
        )
    logger.info('Finished uploading to S3')
    return {
        'statusCode': 200
    }

def _download_files_from_s3(s3_keys):
    downloaded_file_paths = []
    for key in s3_keys:
        # TODO Use this join technique throughout
        tmp_path = os.path.join('/tmp', os.path.split(key)[1])
        s3.download_file(s3_raw_bucket, key, tmp_path)
        downloaded_file_paths.append(tmp_path)
    return downloaded_file_paths