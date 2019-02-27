## Lambda doesn't have pandas or numpy by default, we need to figure out how to add them to the environment.
## This may help: https://hackersandslackers.com/using-pandas-with-aws-lambda/

import json
import datetime
import os
import tarfile
import urllib.request
import boto3
import io
import pandas as pd
import collections
import numpy as np
import xmltodict


s3 = boto3.resource('s3')

def download_file(event):
    objects = event['key1']['Records']
    
    downloaded_files = []
    
    for object_ in objects:
        key = object_['s3']['object']['key']
        bucket = object_['s3']['bucket']['name']
    
        file_name = key.split("/")[-1]
    
        s3.Bucket(bucket).download_file(key, os.path.join("/tmp", file_name))
        
        downloaded_files.append(os.path.join("/tmp", file_name))
        
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
            print("Error extracting", file)
            
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
                results.append(converted_value)
            # if we don't have a rate for the currency use NaN
            except:
                results.append(np.nan)
                
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
def load_data(data_dir, language="EN", doc_type_filter=None):
    parsed_xmls = []
    
    language_tenders = []
    all_tenders = []
    
        
    # loop through the files
    for dir_ in os.listdir(data_dir):
        files = os.listdir(os.path.join(data_path, dir_))
        date = dir_.split("_")[0]
        for file in files:
            # read the contents of the file
            with io.open(os.path.join(data_path, dir_, file), 'r', encoding="utf-8") as f:
                xml = f.read()
                parsed_xml = xmltodict.parse(xml)
                
                if doc_type_filter is not None and parsed_xml['TED_EXPORT']['CODED_DATA_SECTION']['CODIF_DATA']['TD_DOCUMENT_TYPE']['#text'] != doc_type_filter:
                    continue
                    
                parsed_xmls.append(parsed_xml)
                
                # get some header info
                forms_section = parsed_xml['TED_EXPORT']['FORM_SECTION']
                notice_data = parsed_xml['TED_EXPORT']['CODED_DATA_SECTION']['NOTICE_DATA']
                
                header_info = {}
                header_info['DATE'] = date
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
                            
                        if isinstance(form_contents, list):
                            for i, form_content in enumerate(form_contents):
                                all_tenders.append((header_info, form_content))
                                if language is not None and form_content['@LG'] == language:
                                    language_tenders.append((header_info, form_content))
                        elif isinstance(form_contents, collections.OrderedDict):
                            all_tenders.append((header_info, form_contents))
                            if language is not None and form_contents['@LG'] == language:
                                language_tenders.append((header_info, form_contents))
                    except Exception as e:
                        print("File 1", file, e)

    if language == None:
        language_tenders = all_tenders
    
    parsed_data = []
    
    for (header, tender) in language_tenders:
        flattened = {}
        
        # add some fields
        for key in header.keys():
            flattened[key] = header[key]
        
        flattened = extract_xml(tender, "", flattened)
        
        parsed_data.append(flattened)

    df = pd.DataFrame(parsed_data)
        
    # try convert Currencies to Euros, some doc types don't have this so it's not a big deal if there's an error
    try:
        df['VALUE_EUR'] = convert_currencies(df['VALUES_VALUE'].values, df['VALUES_VALUE_CURRENCY'].values)
    except:
        pass
    
    return_df = pd.DataFrame(columns=USE_COLS)
    for col in df.columns:
        if col in USE_COLS:
            return_df[col] = df[col]

    return return_df

def lambda_handler(event, context):
    downloaded_files = download_file(event)
    extracted_files = extract_files(downloaded_files)
    df = load_data("/tmp")
    print(df.head())
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(extracted_files)
    }
