import json
import logging
import os

import boto3
from fs import open_fs
from fs.walk import Walker

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def start(event, context):
    dir_to_walk = 'daily-packages/'
    if 'year' in event and 'month' in event:
        dir_to_walk = f'{dir_to_walk}{event["year"]}/{event["month"]}/'
    logger.info('Walking %s', dir_to_walk)
    with open_fs(f'ftp://guest:guest@ted.europa.eu/{dir_to_walk}') as fs:
        sqs = boto3.client('sqs')
        for path in fs.walk.files(filter=['*.tar.gz']):
            # TODO Batch and send
            sqs.send_message(
                QueueUrl=f'https://sqs.eu-west-3.amazonaws.com/{os.environ["AWS_ACCOUNT_ID"]}/{os.environ["INITIALS"]}_cca_ted_batch_job_{os.environ["STAGE"]}',
                MessageBody=json.dumps({
                    'path': dir_to_walk + path
                })
            )
    logger.info('Finished walking %s', dir_to_walk)
    return {
        'statusCode': 200
    }

def process_item(event, context):
    for record in event['Records']:
        logger.info(json.loads(record['body']))