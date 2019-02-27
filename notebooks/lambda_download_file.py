## This is the download_files function I had previously written adapted to work as a Lambda call.
## What stills needs to be done:
##  - Make sure we don't download files that have already been download
##  - Save the file to an S3 bucket

import json
from ftplib import FTP
import datetime
import os
import tarfile
import urllib.request
import json


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

def download_files(data_path="/tmp", ftp_path="91.250.107.123", username="guest", password="guest", year=None, month=None, max_files=None, delete_files=True):
    # open the log of downloaded files so we know what NOT to download
    # downloaded_files = pd.read_csv(os.path.join(LOG_PATH, "download_logs.txt"), header=None).values
    downloaded_files = []
    
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
            # if the file is not in the logs
            if file not in downloaded_files:
                # download the file with wget since ftplib seems to only download a small part of the file
                file_path = "ftp://"+username+":"+password+"@" + ftp_path + "/daily-packages/" + year + "/" + month + "/" + file
                files_to_download.append(file_path)
    
    # delete the downloaded files
    del(downloaded_files)
    
    if max_files is not None:
        files_to_download = files_to_download[:max_files]
    
    # download the files with wget so we can download the entire file without errors            
    downloaded_files = []
    for file in files_to_download:
        try:
            print("\nDownloading", file)
            file_name = file.split("/")[-1]
            d_file = urllib.request.urlretrieve(file, os.path.join(data_path, file_name))[0]
            downloaded_files.append(d_file)
        except Exception as e:
            print(e)
            print("Error downloading", file)
    
    extracted_files = []
    # extract the tarballs
    for file in downloaded_files:
        print("\nExtracting:", file)
        try:
            if (file.endswith("tar.gz")):
                tar = tarfile.open(file, "r:gz")
                tar.extractall(data_path)
                tar.close()
            elif (file.endswith("tar")):
                tar = tarfile.open(file, "r:")
                tar.extractall()
                tar.close()
            
            extracted_files.append(file)
            if delete_files:
                # if everything was properly extracted we can delete the file
                os.remove(file)
            
            # try to open the file in append mode, if it doesn't work create a new file
            try:
                f = open(os.path.join(LOG_PATH, "download_logs.txt"),"a")
            except:
                f = open(os.path.join(LOG_PATH, "download_logs.txt"),"w+")
            file_name = file.split("/")[1]
            f.write(file_name + "\n")
            f.close()
            
        except:
            print("Error extracting", file)

    return extracted_files

def lambda_handler(event, context):
    new_files = download_files(max_files=1, delete_files=True)
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(new_files)
    }
