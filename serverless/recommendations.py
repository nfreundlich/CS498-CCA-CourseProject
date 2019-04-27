import json
import logging
import os
import time

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

athena = boto3.client('athena')

def fetch(event, context):
    logger.info('Fetching')
    logger.info('Starting query execution')
    start_query_execution_response = athena.start_query_execution(
        QueryExecutionContext={
            'Database': f'cca_ted_extracted_{os.environ["STAGE"]}'
        },
        QueryString='''SELECT *
                       FROM recommendations
                       WHERE contractor IN (
                           SELECT DISTINCT contractor
                           FROM recommendations
                           LIMIT 10
                       )''',
        ResultConfiguration={
            'OutputLocation': f's3://{os.environ["INITIALS"]}-cca-ted-fetch-recommendations-{os.environ["STAGE"]}'
        }
    )
    logger.info('Finished starting query execution')
    time.sleep(15)
    logger.info('Fetching query results')
    get_query_results_response = athena.get_query_results(
        QueryExecutionId=start_query_execution_response['QueryExecutionId']
    )
    logger.info('Finished fetching query results')
    rows = [row['Data'] for row in get_query_results_response['ResultSet']['Rows'][1:]]
    recommendations = []
    for row in rows:
        recommendation = {}
        recommendation['rating'] = row[0]['VarCharValue']
        recommendation['contractor'] = row[1]['VarCharValue']
        recommendation['service'] = row[2]['VarCharValue']
        recommendations.append(recommendation)
    logger.info('Finished fetching')
    return {
        'body': json.dumps(recommendations),
        'headers': {
            'Content-Type': 'application/json'
        },
        'statusCode': 200
    }