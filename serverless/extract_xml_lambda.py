import json
import numpy as np
import collections
import datetime
import os
import tarfile
import urllib.request
import boto3
import io
import xmltodict
import pandas as pd
import shutil
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3')
s3_extracted_bucket = f'{os.environ["INITIALS"]}-cca-ted-extracted-{os.environ["STAGE"]}'

USE_COLS = ['AA_AUTHORITY_TYPE', 'AA_AUTHORITY_TYPE__CODE', 'AC_AWARD_CRIT',
       'AC_AWARD_CRIT__CODE', 'CATEGORY', 'DATE', 'YEAR', 'DS_DATE_DISPATCH',
       'FILE', 'HEADING', 'ISO_COUNTRY__VALUE', 'LG', 'LG_ORIG',
       'NC_CONTRACT_NATURE', 'NC_CONTRACT_NATURE__CODE', 'NO_DOC_OJS',
       'ORIGINAL_CPV_CODE', 'ORIGINAL_CPV_TEXT',
       'PR_PROC', 'PR_PROC__CODE', 'REF_NO',
       'RP_REGULATION', 'RP_REGULATION__CODE', 'TD_DOCUMENT_TYPE',
       'TD_DOCUMENT_TYPE__CODE', 'TY_TYPE_BID', 'TY_TYPE_BID__CODE',
       'COMPLEMENTARY_INFO__ADDRESS_REVIEW_BODY__ADDRESS',
       'COMPLEMENTARY_INFO__ADDRESS_REVIEW_BODY__COUNTRY__VALUE',
       'COMPLEMENTARY_INFO__ADDRESS_REVIEW_BODY__OFFICIALNAME',
       'COMPLEMENTARY_INFO__ADDRESS_REVIEW_BODY__POSTAL_CODE',
       'COMPLEMENTARY_INFO__ADDRESS_REVIEW_BODY__TOWN',
       'COMPLEMENTARY_INFO__DATE_DISPATCH_NOTICE',
       'CONTRACTING_BODY__ADDRESS_CONTRACTING_BODY__ADDRESS',
       'CONTRACTING_BODY__ADDRESS_CONTRACTING_BODY__COUNTRY__VALUE',
       'CONTRACTING_BODY__ADDRESS_CONTRACTING_BODY__E_MAIL',
       'CONTRACTING_BODY__ADDRESS_CONTRACTING_BODY__OFFICIALNAME',
       'CONTRACTING_BODY__ADDRESS_CONTRACTING_BODY__POSTAL_CODE',
       'CONTRACTING_BODY__ADDRESS_CONTRACTING_BODY__TOWN',
       'CONTRACTING_BODY__ADDRESS_CONTRACTING_BODY__URL_GENERAL',
       'CONTRACTING_BODY__ADDRESS_CONTRACTING_BODY__n2016:NUTS__CODE',
       'FORM', 'IA_URL_GENERAL', 'INITIATOR', 'MA_MAIN_ACTIVITIES',
       'MA_MAIN_ACTIVITIES__CODE',
       'OBJECT_CONTRACT__CPV_MAIN__CPV_CODE__CODE',
       'OBJECT_CONTRACT__OBJECT_DESCR__CPV_ADDITIONAL__CPV_CODE__CODE',
       'OBJECT_CONTRACT__OBJECT_DESCR__ITEM',
       'OBJECT_CONTRACT__OBJECT_DESCR__SHORT_DESCR',
       'OBJECT_CONTRACT__OBJECT_DESCR__n2016:NUTS__CODE',
       'OBJECT_CONTRACT__SHORT_DESCR', 'OBJECT_CONTRACT__TITLE',
       'OBJECT_CONTRACT__TYPE_CONTRACT__CTYPE', 'n2016:CA_CE_NUTS',
       'n2016:CA_CE_NUTS__CODE', 'n2016:PERFORMANCE_NUTS',
       'n2016:PERFORMANCE_NUTS__CODE',
       'COMPLEMENTARY_INFO__ADDRESS_REVIEW_BODY__PHONE',
       'CONTRACTING_BODY__ADDRESS_CONTRACTING_BODY__PHONE',
       'CONTRACTING_BODY__ADDRESS_CONTRACTING_BODY__URL_BUYER',
       'CONTRACTING_BODY__CA_ACTIVITY__VALUE',
       'CONTRACTING_BODY__CA_TYPE__VALUE', 'LEGAL_BASIS__VALUE',
       'OBJECT_CONTRACT__REFERENCE_NUMBER', 'REF_NOTICE__NO_DOC_OJS',
       'VALUES__VALUE', 'VALUES__VALUE__CURRENCY', 'VALUES__VALUE__TYPE', 'VALUE_EUR',
       'AWARD_CONTRACT__ITEM',
       'AWARD_CONTRACT__AWARDED_CONTRACT__DATE_CONCLUSION_CONTRACT',
       'AWARD_CONTRACT__TITLE', 'OBJECT_CONTRACT__VAL_TOTAL',
       'OBJECT_CONTRACT__VAL_TOTAL__CURRENCY',
       'PROCEDURE__NOTICE_NUMBER_OJ', 'n2016:TENDERER_NUTS',
       'n2016:TENDERER_NUTS__CODE', 'CONTRACTING_BODY__URL_DOCUMENT',
       'CONTRACTING_BODY__URL_PARTICIPATION', 'DT_DATE_FOR_SUBMISSION',
       'IA_URL_ETENDERING', 'OBJECT_CONTRACT__OBJECT_DESCR__DURATION',
       'OBJECT_CONTRACT__OBJECT_DESCR__DURATION__TYPE',
       'PROCEDURE__DATE_RECEIPT_TENDERS',
       'PROCEDURE__LANGUAGES__LANGUAGE__VALUE',
       'PROCEDURE__OPENING_CONDITION__DATE_OPENING_TENDERS',
       'PROCEDURE__OPENING_CONDITION__TIME_OPENING_TENDERS',
       'PROCEDURE__TIME_RECEIPT_TENDERS',
       'AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__COUNTRY__VALUE',
       'AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__OFFICIALNAME',
       'AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__POSTAL_CODE',
       'AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__TOWN',
       'AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__n2016:NUTS__CODE',
       'AWARD_CONTRACT__AWARDED_CONTRACT__TENDERS__NB_TENDERS_RECEIVED',
       'AWARD_CONTRACT__AWARDED_CONTRACT__VALUES__VAL_TOTAL',
       'AWARD_CONTRACT__AWARDED_CONTRACT__VALUES__VAL_TOTAL__CURRENCY',
       'AWARD_CONTRACT__CONTRACT_NO', 'AWARD_CONTRACT__LOT_NO',
       'COMPLEMENTARY_INFO__ADDRESS_REVIEW_BODY__E_MAIL',
       'COMPLEMENTARY_INFO__ADDRESS_REVIEW_BODY__FAX',
       'COMPLEMENTARY_INFO__ADDRESS_REVIEW_INFO__COUNTRY__VALUE',
       'COMPLEMENTARY_INFO__ADDRESS_REVIEW_INFO__OFFICIALNAME',
       'COMPLEMENTARY_INFO__ADDRESS_REVIEW_INFO__TOWN',
       'COMPLEMENTARY_INFO__INFO_ADD', 'LEFTI__SUITABILITY',
       'PROCEDURE__DURATION_TENDER_VALID',
       'PROCEDURE__DURATION_TENDER_VALID__TYPE',
       'FD_OTH_NOT__OBJ_NOT__BLK_BTX',
       'FD_OTH_NOT__OBJ_NOT__CPV__CPV_MAIN__CPV_CODE__CODE',
       'FD_OTH_NOT__OBJ_NOT__INT_OBJ_NOT',
       'FD_OTH_NOT__STI_DOC__P__ADDRESS_NOT_STRUCT__ADDRESS',
       'FD_OTH_NOT__STI_DOC__P__ADDRESS_NOT_STRUCT__BLK_BTX',
       'FD_OTH_NOT__STI_DOC__P__ADDRESS_NOT_STRUCT__COUNTRY__VALUE',
       'FD_OTH_NOT__STI_DOC__P__ADDRESS_NOT_STRUCT__E_MAIL',
       'FD_OTH_NOT__STI_DOC__P__ADDRESS_NOT_STRUCT__ORGANISATION',
       'FD_OTH_NOT__STI_DOC__P__ADDRESS_NOT_STRUCT__PHONE',
       'FD_OTH_NOT__STI_DOC__P__ADDRESS_NOT_STRUCT__POSTAL_CODE',
       'FD_OTH_NOT__STI_DOC__P__ADDRESS_NOT_STRUCT__TOWN',
       'FD_OTH_NOT__TI_DOC', 'VERSION']

LIST_COLS = ['ORIGINAL_CPV',
 'ORIGINAL_CPV_CODE',
 'ORIGINAL_CPV_TEXT',
 'ORIGINAL_CPV__CODE',
 'OBJECT_CONTRACT__SHORT_DESCR',
 'PROCEDURE__LANGUAGES__LANGUAGE__VALUE',
 'OBJECT_CONTRACT__OBJECT_DESCR__CPV_ADDITIONAL__CPV_CODE__CODE',
 'OBJECT_CONTRACT__OBJECT_DESCR__ITEM',
 'OBJECT_CONTRACT__OBJECT_DESCR__SHORT_DESCR',
 'OBJECT_CONTRACT__OBJECT_DESCR__n2016:NUTS__CODE',
 'OBJECT_CONTRACT__OBJECT_DESCR__DURATION',
 'OBJECT_CONTRACT__OBJECT_DESCR__DURATION__TYPE',
 'COMPLEMENTARY_INFO__INFO_ADD',
 'LEFTI__SUITABILITY',
 'AWARD_CONTRACT__ITEM',
 'AWARD_CONTRACT__AWARDED_CONTRACT__DATE_CONCLUSION_CONTRACT',
 'AWARD_CONTRACT__TITLE',
 'n2016:TENDERER_NUTS',
 'n2016:TENDERER_NUTS__CODE',
 'AWARD_CONTRACT__CONTRACT_NO',
 'n2016:PERFORMANCE_NUTS',
 'n2016:PERFORMANCE_NUTS__CODE',
 'n2016:CA_CE_NUTS',
 'n2016:CA_CE_NUTS__CODE',
 'AWARD_CONTRACT__LOT_NO',
 'LG_ORIG',
 'AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__COUNTRY__VALUE',
 'AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__OFFICIALNAME',
 'AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__POSTAL_CODE',
 'AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__TOWN',
 'AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__n2016:NUTS__CODE',
 'FD_OTH_NOT__STI_DOC__P__ADDRESS_NOT_STRUCT__BLK_BTX',
 'FD_OTH_NOT__OBJ_NOT__BLK_BTX',
 'MA_MAIN_ACTIVITIES',
 'MA_MAIN_ACTIVITIES__CODE',
 'AWARD_CONTRACT__AWARDED_CONTRACT__TENDERS__NB_TENDERS_RECEIVED',
 'AWARD_CONTRACT__AWARDED_CONTRACT__VALUES__VAL_TOTAL',
 'AWARD_CONTRACT__AWARDED_CONTRACT__VALUES__VAL_TOTAL__CURRENCY',
 'FD_OTH_NOT__TI_DOC']
 
def download_file(event):
    objects = event['Records']
    
    downloaded_files = []
    
    for object_ in objects:
        key = object_['s3']['object']['key']
        bucket = object_['s3']['bucket']['name']
        logger.info('Downloading %s from %s', key, bucket)
    
        file_name = key.split("/")[-1]
    
        s3.Bucket(bucket).download_file(key, os.path.join("/tmp", file_name))
        
        downloaded_files.append(os.path.join("/tmp", file_name))
        logger.info('Finished downloading %s from %s', key, bucket)
        
    return downloaded_files

def extract_files(files, delete_files=True, data_path="/tmp"):
    extracted_files = []
    
    for file in files:
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
        except:
            logger.error("Error extracting %s", file)
            
    return extracted_files

# convert all currencies to EUR
def convert_currencies(values, currencies):
    url = "https://api.exchangeratesapi.io/latest"
    content = urllib.request.urlopen(url).read()
    exchange_rates = json.loads(content.decode())
    results = []
    
    for value, currency in zip(values, currencies):
        if currency == "EUR":
            results.append(value)
            
        else:
            try:
                exchange_rate = exchange_rates['rates'][currency]
                converted_value = float(value) / exchange_rate
                results.append(str(converted_value))
            # if we don't have a rate for the currency use NaN
            except:
                results.append("")
                
    return results

def unwind_descriptions(short_desc):
    # get the text from the OrderedDicts in the short descriptions
    for i, foo in enumerate(short_desc):
        if type(foo) != str:
            if type(foo) == list:
                for j, bar in enumerate(foo):
                    if type(bar) == collections.OrderedDict:
                        bar = bar['#text']
                        short_desc[i][j] = bar
            elif type(foo) == collections.OrderedDict:
                foo = foo['#text']
                short_desc[i] = foo

    # flatten the lists
    for i, foo in enumerate(short_desc):
        if type(foo) == list:
            foo = " ".join(foo)
            short_desc[i] = foo
            
    return short_desc

# function to recursively extract data from XML files
def extract_xml(xml_dict, parent_key="", results_dict={}):
    # make sure the input is a an ordered dictionary
    if isinstance(xml_dict, collections.OrderedDict):
        for key1, value1 in xml_dict.items():
            # remove unneeded characters from the key
            if key1[0] == "@" or key1[0] == "#":
                key1 = key1[1:]
            
            # FT means FT, we can ignore these
            if key1 == "FT":
                continue
            
            # add the parent key for clarity
            if len(parent_key):
                # if the current key is text we will not append it to the parent
                if key1 != "text":
                    new_key = parent_key + "__" + key1
                else:
                    new_key = parent_key
            else:
                new_key = key1
            
            # if the value is a string directly add it
            if isinstance(value1, str):
                # if the key is "P" the value is a new paragraph and should be appended
                # not overwritten, if the key is "FT" it is a font thing and should also be appended
                if key1 != "P" and key1 != "FT":
                    # if the key does NOT exist add it
                    if new_key not in results_dict:
                        results_dict[new_key] = value1
                    # else instead of overwriting the data let's make a list of the values
                    else:
                        if isinstance(results_dict[new_key], list):
                            results_dict[new_key].append(value1)
                        elif isinstance(results_dict[new_key], str):
                            results_dict[new_key] = [results_dict[new_key]]
                            results_dict[new_key].append(value1)
                else:
                    if parent_key in results_dict:
                        if isinstance(results_dict[parent_key], list):
                            results_dict[parent_key].append(value1)
                        elif isinstance(results_dict[parent_key], str):
                            listed_vals = [results_dict[parent_key], value1]
                            results_dict[parent_key] = listed_vals
                    else:
                        results_dict[parent_key] = value1
            
            # else if it is a list loop through and add the items
            # note that this will overwrite the previous entries
            elif isinstance(value1, list):
                item_string = []
                for item in value1:
                    if isinstance(item, collections.OrderedDict):
                        results_dict = extract_xml(item, new_key, results_dict)
                    elif isinstance(item, str):
                        item_string.append(item)
                if len(item_string) > 0:
                    if key1 != "P":
                        results_dict[new_key] = item_string
                    else:
                        results_dict[parent_key] = item_string
                        
            # else if the value is an OrderedDict recurse
            elif isinstance(value1, collections.OrderedDict):
                # handle Ps differently, they are paragraphs and do not need to be recursed into
                if key1 != "P":
                    results_dict = extract_xml(value1, new_key, results_dict)
                else:
                    # if the key is P and is has text use the text, otherwise recurse as usual
                    try:
                        results_dict[parent_key] = value1['#text']
                    except:
                        results_dict = extract_xml(value1, new_key, results_dict)
                    
    elif isinstance(xml_dict, str):
        results_dict[parent_key] = xml_dict
    
    elif isinstance(xml_dict, list):
        pass
    
    return results_dict
    
data_path = "/tmp"

## Function load_data - 
## Params -
## - data_dir - directory to extract from
## - language - languages to extract from the XML
## - doc_type_filter - if specified function will only return XML documents of the specified type
## Returns - 
## - dataframe of parsed documents
def load_data(data_dir, language="EN", doc_type_filter=['Contract award notice', 'Contract notice', 'Contract award', 'Additional information']):
    language_tenders = []
    all_tenders = []
        
    # loop through the files
    for dir_ in os.listdir(data_dir):
        try:
            files = os.listdir(os.path.join(data_dir, dir_))
        except:
            continue
        date = dir_.split("_")[0]
        xml_files = [file for file in files if file.endswith('.xml')]
        for file in xml_files:
            # read the contents of the file
            # logger.info('Parsing data from %s', file)
            with io.open(os.path.join(data_dir, dir_, file), 'r', encoding="utf-8") as f:
                xml = f.read()
                parsed_xml = xmltodict.parse(xml)
                
                if doc_type_filter is not None and parsed_xml['TED_EXPORT']['CODED_DATA_SECTION']['CODIF_DATA']['TD_DOCUMENT_TYPE']['#text'] not in doc_type_filter:
                    continue
                
                # get some header info
                forms_section = parsed_xml['TED_EXPORT']['FORM_SECTION']
                notice_data = parsed_xml['TED_EXPORT']['CODED_DATA_SECTION']['NOTICE_DATA']
                
                header_info = {}
                header_info['DATE'] = date
                header_info['YEAR'] = date[:4]
                header_info['FILE'] = file
                # extract the info from the codified data section
                header_info = extract_xml(parsed_xml['TED_EXPORT']['CODED_DATA_SECTION']['CODIF_DATA'], "", header_info)
                
                # extract the info from the notice_data section, except we don't need the URI_LIST
                notice_data.pop("URI_LIST")
                header_info = extract_xml(notice_data, "", header_info)
                
                if isinstance(notice_data['ORIGINAL_CPV'], list):
                    header_info['ORIGINAL_CPV_CODE'] = []
                    header_info['ORIGINAL_CPV_TEXT'] = []
                    for cpv_info in notice_data['ORIGINAL_CPV']:
                        header_info['ORIGINAL_CPV_CODE'].append(cpv_info['@CODE'])
                        header_info['ORIGINAL_CPV_TEXT'].append(cpv_info['#text'])
                else:
                    header_info['ORIGINAL_CPV_CODE'] = notice_data['ORIGINAL_CPV']['@CODE']
                    header_info['ORIGINAL_CPV_TEXT'] = notice_data['ORIGINAL_CPV']['#text']

                try:
                    header_info['REF_NO'] = notice_data['REF_NOTICE']['NO_DOC_OJS']
                except:
                    header_info['REF_NO'] = ""
                    
                forms = forms_section.keys()
                
                for form in forms:
                    try:
                        form_contents = forms_section[form]
                        # if there is a list of forms there are multiple languages and we should pick the one we want   
                        if isinstance(form_contents, list):
                            for i, form_content in enumerate(form_contents):
                                all_tenders.append((header_info, form_content))
                                if language is not None and form_content['@LG'] == language:
                                    language_tenders.append((header_info, form_content))
                                    
                        # if the form is a dictionary there is only one and we should use it regardless of language
                        elif isinstance(form_contents, collections.OrderedDict):
                            all_tenders.append((header_info, form_contents))
                            language_tenders.append((header_info, form_contents))
                    except Exception as e:
                        logger.error("File %s", file)
                        
            # logger.info('Finished parsing data from %s', file)
            
        # delete the directory we just read from to avoid conflicts and duplicates
        # this may not be necessary and we may want to revisit it
        shutil.rmtree(os.path.join(data_dir, dir_))
        
    if language == None:
        language_tenders = all_tenders
    
    parsed_data = []
    
    # we don't need all tenders anymore, let's delete it
    del(all_tenders)
    
    for (header, tender) in language_tenders:
        flattened = {}
        
        # add some fields
        for key in header.keys():
            flattened[key] = header[key]
        
        flattened = extract_xml(tender, "", flattened)
        
        # older documents have the value in different columns, we should catch those
        if "VALUES_LIST__VALUES__SINGLE_VALUE__VALUE" in flattened and "VALUES__VALUE" not in flattened:
            flattened['VALUES__VALUE'] = flattened['VALUES_LIST__VALUES__SINGLE_VALUE__VALUE']
            flattened['VALUES__VALUE__CURRENCY'] = flattened['VALUES_LIST__VALUES__SINGLE_VALUE__VALUE__CURRENCY']
        elif "VALUES_LIST__VALUES__RANGE_VALUE__VALUE" in flattened and "VALUES__VALUE" not in flattened:
            flattened['VALUES__VALUE'] = flattened['VALUES_LIST__VALUES__RANGE_VALUE__VALUE'][0]
            flattened['VALUES__VALUE__CURRENCY'] = flattened['VALUES_LIST__VALUES__RANGE_VALUE__VALUE__CURRENCY'][0]
        parsed_data.append(flattened)
        
    # clean up unneeded data
    del(language_tenders)
    
    df = pd.DataFrame(parsed_data)
    
    # garbage collecting       
    del(parsed_data)
    
    # try convert Currencies to Euros, some doc types don't have this so it's not a big deal if there's an error
    try:
        df['VALUE_EUR'] = convert_currencies(df['VALUES__VALUE'].values, df['VALUES__VALUE__CURRENCY'].values)
    except:
        logger.error("Error converting currencies")
        
    return_df = pd.DataFrame(columns=USE_COLS)
    for col in USE_COLS:
        # catch the possibility that the column doesn't exist in the dataframe
        try:
            column_data = df[col].values

            is_list_col = (col in LIST_COLS)
            for i, item in enumerate(column_data):
                # if the column should be a list
                if is_list_col:
                    # if it is a list nothing needs to be done
                    if isinstance(item, list):
                        column_data[i] = list(item)
                    # else if it is not a list make it into a list
                    else:
                        column_data[i] = [item]                   
                # if the column should NOT be a list
                else:
                    # if it is NOT a list make sure it is a string
                    if not isinstance(item, list):
                        column_data[i] = item
                    # else if it IS a list
                    else:
                        column_data[i] = ';'.join(str(x) for x in item)

            return_df[col] = column_data   
        except:
            pass
    
    # add some additional columns containing the first items in some of the list columns
    # if there is an error the column is not a list so just use the value in it
    try:
        return_df['MAIN_CPV_CODE'] = return_df['ORIGINAL_CPV_CODE'].map(lambda x: x[0])
    except:
        return_df['MAIN_CPV_CODE'] = return_df['ORIGINAL_CPV_CODE']
    try:
        return_df['MAIN_n2016:TENDERER_NUTS__CODE'] = return_df['n2016:TENDERER_NUTS__CODE'].map(lambda x: x[0])
    except:
        return_df['MAIN_n2016:TENDERER_NUTS__CODE'] = return_df['n2016:TENDERER_NUTS__CODE']
    try:
        return_df['MAIN_n2016:PERFORMANCE_NUTS__CODE'] = return_df['n2016:PERFORMANCE_NUTS__CODE'].map(lambda x: x[0])
    except:
        return_df['MAIN_n2016:PERFORMANCE_NUTS__CODE'] = return_df['n2016:PERFORMANCE_NUTS__CODE']
    try:
        return_df['MAIN_MA_MAIN_ACTIVITIES__CODE'] = return_df['MA_MAIN_ACTIVITIES__CODE'].map(lambda x: x[0])
    except:
        return_df['MAIN_MA_MAIN_ACTIVITIES__CODE'] = return_df['MA_MAIN_ACTIVITIES__CODE']
    try:    
        return_df['MAIN_OBJECT_CONTRACT__OBJECT_DESCR__DURATION'] = return_df['OBJECT_CONTRACT__OBJECT_DESCR__DURATION'].map(lambda x: x[0])
    except:
        return_df['MAIN_OBJECT_CONTRACT__OBJECT_DESCR__DURATION'] = return_df['OBJECT_CONTRACT__OBJECT_DESCR__DURATION']
    try:    
        return_df['MAIN_AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__COUNTRY__VALUE'] = return_df['AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__COUNTRY__VALUE'].map(lambda x: x[0])
    except:
        return_df['MAIN_AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__COUNTRY__VALUE'] = return_df['AWARD_CONTRACT__AWARDED_CONTRACT__CONTRACTORS__CONTRACTOR__ADDRESS_CONTRACTOR__COUNTRY__VALUE']
    
    return_df.dropna(axis=1, how="all", inplace=True)
    
    return return_df

def lambda_handler(event, context):
    downloaded_files = download_file(event)
    logger.info("Extracting files")
    extracted_files = extract_files(downloaded_files)
    logger.info("Parsing data")
    df = load_data("/tmp")
    logger.info("Done parsing")
    file_name = downloaded_files[0].split("/")[-1].split(".")[0] + ".parquet"
    # replace "_" with "-" as underscores may cause problems with Glue?
    file_name = str.replace(file_name, "_", "-")
    logger.info("File name " + file_name)
    df.to_parquet("/tmp/" + file_name)
    year = file_name[:4]
    month = file_name[4:6]
    prefix = year + "/" + month
    # upload the file to S3
    s3.meta.client.upload_file(Filename = os.path.join("/tmp/", file_name), Bucket = s3_extracted_bucket, Key = prefix + "/" + file_name)
    
    return {
        'statusCode': 200,
        'body': json.dumps(file_name)
    }