import json
from ftplib import FTP
import datetime
import os
# import tarfile
import urllib.request
import json
import boto3
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')
AWS_BUCKET_NAME = '1-cca-ted-raw-dev'

# Function download_files:
# FTPs to ftp_path, gets list of files in the directory for the current year and month (note that this may cause
# problems on the first day of the month downloading the files from the last day of the previous month); makes a list 
# of files to be download. Then downloads the files with wget (ftp was not downloading the entire file); unzips the 
# tarballs and then deletes the tar.gz file.
# 
# Params:
# - data_path -> path to download data to
# - ftp_path -> URI for FTP
# - username, password -> username and password for FTP login
# - year, month -> year and month to download data for, if None will use current month and year
# - max_files -> max number of files to download, useful for debugging
# - delete_files -> whether to delete the files that have been successfully extracted
#
# Returns:
# - list of files downloaded and successfully extracted
#
# Note that sometimes using the URL throws an error, in this case use the IP address: 91.250.107.123

# Function download_files:
# FTPs to ftp_path, gets list of files in the directory for the current year and month (note that this may cause
# problems on the first day of the month downloading the files from the last day of the previous month); makes a list 
# of files to be download. Then downloads the files with wget (ftp was not downloading the entire file); unzips the 
# tarballs and then deletes the tar.gz file.
# 
# Params:
# - data_path -> path to download data to
# - ftp_path -> URI for FTP
# - username, password -> username and password for FTP login
# - year, month -> year and month to download data for, if None will use current month and year
# - max_files -> max number of files to download, useful for debuggin
# - delete_files -> whether to delete the files that have been successfully extracted
#
# Returns:
# - list of files downloaded and successfully extracted
#
# Note that sometimes using the URL throws an error, in this case use the IP address: 91.250.107.123

def download_files(data_path="/tmp", ftp_path="91.250.107.123", username="guest", password="guest", year=None, month=None, max_files=1, delete_files=True):
    ## USE FTP TO GET THE LIST OF FILES TO DOWNLOAD
    with FTP(ftp_path, user=username, passwd=password) as ftp:
        # create the directory name for the current month and year
        # we may want to do this for yesterday 
        now = datetime.datetime.now()
        if year is None:
            year = now.year
        if month is None:
            month = now.month
        
        month = str(month).zfill(2)
        year = str(year)
         
        # go to that directory and get the files in it
        ftp.cwd('daily-packages/' + year + "/" + month) 
        dir_list = ftp.nlst() 
        files_to_download = []

        # loop through the files
        for file in dir_list:
            # download the file with wget since ftplib seems to only download a small part of the file
            file_path = "ftp://"+username+":"+password+"@" + ftp_path + "/daily-packages/" + year + "/" + month + "/" + file
            files_to_download.append(file_path)
    
    # the newest file will be the last one in the sorted list
    if max_files is not None:
        files_to_download = sorted(files_to_download)[-max_files:]
    
    # download the files with wget so we can download the entire file without errors            
    downloaded_files = []
    for file in files_to_download:
        try:
            logger.info('Downloading file %s', file)
            file_name = file.split("/")[-1]
            d_file = urllib.request.urlretrieve(file, os.path.join(data_path, file_name))[0]
            downloaded_files.append(d_file)
        except Exception as e:
            logger.info('Error downloading file %s', file)
            
    return downloaded_files

def upload_to_s3(data_path="/tmp", key="raw_data"):
    bucket = s3.Bucket(AWS_BUCKET_NAME)
    
    # find the directories in the download dir
    files = os.listdir(data_path)
    
    for file in files:
        # check if the file already exists so we don't create duplicates
        logger.info('Uploading %s to S3 bucket %s key %s', file, AWS_BUCKET_NAME, key)
        try:
            s3.meta.client.upload_file(Filename = os.path.join(data_path, file), Bucket = AWS_BUCKET_NAME, Key = key + "/" + file)
        except Exception as e:
            logger.info('Error uploading %s to S3 bucket %s key %s', file, AWS_BUCKET_NAME, key)

def lambda_handler(event, context):
    new_files = download_files(max_files=1, delete_files=True)
    
    upload_to_s3(data_path="/tmp")
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(new_files)
    }
