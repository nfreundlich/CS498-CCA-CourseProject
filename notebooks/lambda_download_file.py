## A Lambda function to get the files for the current month from the TED FTP, download the latest one (or more can be specified as
## a parameter to the function). The function used to extract the tarballs and then upload the XML files individually, but this
## takes quite a long time so it has been modified to instead just upload the .tar.gz file to S3. Then the parsing function
## can handle the downloading and extraction.

import json
from ftplib import FTP
import datetime
import os
import tarfile
import urllib.request
import json
import boto3

s3 = boto3.resource('s3')
AWS_BUCKET_NAME = 'cca498'

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
            year = str(now.year)
        if month is None:
            month = datetime.datetime.now().strftime('%m')

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
            print("\nDownloading", file)
            file_name = file.split("/")[-1]
            d_file = urllib.request.urlretrieve(file, os.path.join(data_path, file_name))[0]
            downloaded_files.append(d_file)
        except Exception as e:
            print("Error downloading", file, e)
            
    # Extracting the files and uploading them to S3 individually takes WAY too long,
    # instead let's just upload the tarball and then we can download that and extract it
    # when we process the data
    
    # extracted_files = []
    # # extract the tarballs
    # for file in downloaded_files:
    #     print("\nExtracting:", file)
    #     try:
    #         if (file.endswith("tar.gz")):
    #             tar = tarfile.open(file, "r:gz")
    #             tar.extractall(data_path)
    #             tar.close()
    #         elif (file.endswith("tar")):
    #             tar = tarfile.open(file, "r:")
    #             tar.extractall()
    #             tar.close()
            
    #         extracted_files.append(file)
    #         if delete_files:
    #             # if everything was properly extracted we can delete the file
    #             os.remove(file)
    #     except:
    #         print("Error extracting", file)

    return downloaded_files

def upload_to_s3(data_path="/tmp", key="raw_data"):
    bucket = s3.Bucket(AWS_BUCKET_NAME)
    
    # find the directories in the download dir
    files = os.listdir(data_path)
    for file in files:
        try:
            s3.meta.client.upload_file(Filename = os.path.join(data_path, file), Bucket = AWS_BUCKET_NAME, Key = key + "/" + file)
        except Exception as e:
            print(e)
    
def lambda_handler(event, context):
    new_files = download_files(max_files=1, delete_files=True)
    
    upload_to_s3(data_path="/tmp")
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(new_files)
    }


