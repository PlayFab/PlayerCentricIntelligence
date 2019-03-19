from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.storage.blob import BlockBlobService
from azure.kusto.ingest import (
    KustoIngestClient,
    IngestionProperties,
    FileDescriptor,
    BlobDescriptor,
    DataFormat,
    ReportLevel,
)
from azure.kusto.ingest.status import KustoIngestStatusQueues
import os
import pandas as pd
from datetime import datetime
import json
import pprint
import time

######################################################
#                   function defs
######################################################

def get_adx_cfg_as_json_dict(cfg_file_fq = None): 

    if cfg_file_fq is None:
        dir_path = os.path.dirname(os.path.realpath(__file__))  # Get path to THIS script file...
        cfg_file_fq = '{}\\ADX_Config.json'.format(dir_path)  # and assume configuration file is named "ADX_Config.json"

    if not os.access(cfg_file_fq, os.R_OK): 
        raise ValueError('Unable to open configuration file [{}] for read!'.format(cfg_file_fq))
    
    with open(cfg_file_fq, 'r') as cfg_fh:
        try:
            adx_cfg_json = json.load(cfg_fh) 
        except:
            raise ValueError('Unable to parse configuration file [{}] into JSON document object!'.format(cfg_file_fq))

    return adx_cfg_json

def get_col_map_as_json_dict(cfg_file_fq = None): 

    if cfg_file_fq is None:
        dir_path = os.path.dirname(os.path.realpath(__file__))  # Get path to THIS script file...
        cfg_file_fq = '{}\\ADX_ColumnMapping.json'.format(dir_path)  # and assume configuration file is named "ADX_Config.json"

    if not os.access(cfg_file_fq, os.R_OK): 
        raise ValueError('Unable to open configuration file [{}] for read!'.format(cfg_file_fq))
    
    with open(cfg_file_fq, 'r') as cfg_fh:
        try:
            col_map_as_json_dict = json.load(cfg_fh) 
        except:
            raise ValueError('Unable to parse configuration file [{}] into JSON document object!'.format(cfg_file_fq))

    return col_map_as_json_dict

def get_blob_src_schema(blob_name):
    blob_name_split = blob_name.split('/')
    blob_src_schema = blob_name_split[0]

    return blob_src_schema

def get_blob_src_obj(blob_name):
    blob_name_split = blob_name.split('/')
    blob_name_element = blob_name_split[1]
    blob_src_obj = blob_name_element[13:]
    
    return blob_src_obj.replace('.csv.gz', '', 1)

def get_blob_dt(blob_name):
    blob_name_split = blob_name.split('/')
    blob_name_element = blob_name_split[1]
    blob_dt = blob_name_element[:12]
    
    return blob_dt

def blob_matches_obj(blob_name, schema_name, object_name):
    blobSrcSchema = get_blob_src_schema(blob_name)
    blobSrcObj:str = get_blob_src_obj(blob_name)
    if blobSrcSchema == src_schema_name and blobSrcObj == src_object_name:
        return True
    else:
        return False

######################################################
#                   GLOBALS
######################################################

bIsProduction:bool = False

reportLevel = ReportLevel.DoNotReport if bIsProduction else ReportLevel.FailuresAndSuccesses

adx_cfg_json = get_adx_cfg_as_json_dict() 

col_map_as_json_dict = get_col_map_as_json_dict() 

cluster_name = adx_cfg_json.get('ADX_CLUSTER_NAME')
cluster_region = adx_cfg_json.get('ADX_CLUSTER_REGION')

kusto_uri = "https://{}.{}.kusto.windows.net".format(cluster_name, cluster_region)
kusto_ingest_uri = "https://ingest-{}.{}.kusto.windows.net".format(cluster_name, cluster_region)

aad_tenant_id = adx_cfg_json.get('AAD_TENANT_ID')
app_client_secret = adx_cfg_json.get('APP_CLIENT_SECRET')
app_client_id = adx_cfg_json.get('APP_CLIENT_ID')

container = adx_cfg_json.get('BLOB_CONTAINER_NAME')
account_name = adx_cfg_json.get('STORAGE_ACCOUNT_NAME')
account_key = adx_cfg_json.get('STORAGE_ACCOUNT_KEY')
sas_token = adx_cfg_json.get('SAS_TOKEN')

# kcsb_ingest = KustoConnectionStringBuilder.with_aad_device_authentication(kusto_ingest_uri, aad_tenant_id)
# kcsb_data = KustoConnectionStringBuilder.with_aad_device_authentication(kusto_uri, aad_tenant_id)

kcsb_ingest = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    kusto_ingest_uri, app_client_id, app_client_secret, aad_tenant_id
)

kcsb_data = KustoConnectionStringBuilder.with_aad_application_key_authentication(
    kusto_uri, app_client_id, app_client_secret, aad_tenant_id
)

kusto_client = KustoClient(kcsb_data)

column_mapping = col_map_as_json_dict.get('OBJECT_CONFIG')

blob_service = BlockBlobService(account_name, account_key)

# Function to find latest blob from all available
# Assumes format of <source> + '/' + <YYYYMMDDHHmm> + '_' + <object name>
blob_names_generator = blob_service.list_blobs(container)
# for blob in blob_names_generator:
#     length = BlockBlobService.get_blob_properties(blob_service, container, blob.name).properties.content_length
#     print('Length of blob {} in bytes {}'.format(blob.name, length))

blobs_list = [blob.name for blob in blob_names_generator]

for obj in column_mapping:
    
    src_schema_name = obj.get('SRC_SCHEMA_NAME')
    src_object_name = obj.get('SRC_OBJECT_NAME')

    all_blob_dt_for_obj = [get_blob_dt(blob) for blob in blobs_list if blob_matches_obj(blob, src_schema_name, src_object_name)]

    if len(all_blob_dt_for_obj) < 1:
        continue # Stop here if there is no blob for the current object

    last_dt = max(all_blob_dt_for_obj)
    last_blob = [blob for blob in blobs_list if blob_matches_obj(blob, src_schema_name, src_object_name) and get_blob_dt(blob) == last_dt]

    file_name = str(last_blob[0]) # Assumed only one blob has latest timestamp
 
    blob_path = "https://" + account_name + ".blob.core.windows.net/" + container + "/" + file_name + sas_token
    
    kusto_database  = obj.get('TGT_OBJECT_DB')
    destination_table = """{SRC_SCHEMA_NAME}_{SRC_OBJECT_NAME}""".format(SRC_SCHEMA_NAME=src_schema_name, SRC_OBJECT_NAME=src_object_name)
    column_mapping_name = 'csv_mapping_{}'.format(destination_table)

    drop_table_if_exists_command = """
    .drop table {DESTINATION_TABLE} ifexists
    """.format(DESTINATION_TABLE=destination_table, CREATE_COLUMN_LIST=obj.get('CREATE_COLUMN_LIST'))

    create_table_command = """
    .create table {DESTINATION_TABLE} ({CREATE_COLUMN_LIST})
    """.format(DESTINATION_TABLE=destination_table, CREATE_COLUMN_LIST=obj.get('CREATE_COLUMN_LIST'))
    
    check_for_mapping_command = """
    .show table {DESTINATION_TABLE} ingestion csv mapping '{COLUMN_MAPPING_NAME}' 
    """.format(DESTINATION_TABLE=destination_table, COLUMN_MAPPING_NAME=column_mapping_name)    
    
    create_mapping_command = """
    .create table {DESTINATION_TABLE} ingestion csv mapping '{COLUMN_MAPPING_NAME}' '{COLUMN_MAPPING}'
    """.format(DESTINATION_TABLE=destination_table, COLUMN_MAPPING_NAME=column_mapping_name, COLUMN_MAPPING=json.dumps(obj.get('COLUMN_MAPPING')))

    
    
    # Drop table if exists, then create.
    kusto_client.execute_mgmt(kusto_database, drop_table_if_exists_command)
    kusto_client.execute_mgmt(kusto_database, create_table_command)
    
    #NOTE: this may be backwards...
    try:
        response = kusto_client.execute_mgmt(kusto_database, check_for_mapping_command)
    except: # Because check_for_mapping_command should throw error if mapping already exists
        response = kusto_client.execute_mgmt(kusto_database, create_mapping_command)

    ingestion_client = KustoIngestClient(kcsb_ingest)

    # All ingestion properties: https://docs.microsoft.com/en-us/azure/kusto/management/data-ingestion/#ingestion-properties
    ingestion_props  = IngestionProperties(reportLevel=reportLevel ,database=kusto_database, table=destination_table, dataFormat=DataFormat.csv, mappingReference=column_mapping_name, additionalProperties={'ignoreFirstRecord': 'true'})
    blobProps = BlockBlobService.get_blob_properties(blob_service, container, file_name).properties
    file_size = blobProps.content_length
    blob_descriptor = BlobDescriptor(blob_path, file_size)  # Raw size of the data in bytes

    ingestion_client.ingest_from_blob(blob_descriptor,ingestion_properties=ingestion_props)

    print("""Queued blob '{FILE_NAME}' ({FILE_SIZE} bytes) for ingestion into ADX table '{DESTINATION_TABLE}'""".format(FILE_NAME=file_name, FILE_SIZE=file_size, DESTINATION_TABLE=destination_table))

    # query = """{} | count""".format(destination_table)

    # response = kusto_client.execute_query(kusto_database, query)

    # count_query_df = dataframe_from_result_table(response.primary_results[0])
    # print(count_query_df)
    #break

#NOTE: uncomment this to check the status message logs

qs = KustoIngestStatusQueues(ingestion_client)

MAX_BACKOFF = 180

backoff = 1
while True:
    ################### NOTICE ####################
    # in order to get success status updates,
    # make sure ingestion properties set the
    # reportLevel=ReportLevel.FailuresAndSuccesses.
    if qs.success.is_empty() and qs.failure.is_empty():
        time.sleep(backoff)
        backoff = min(backoff * 2, MAX_BACKOFF)
        print("No new messages. Backing off for {} seconds".format(backoff))
        continue

    backoff = 1

    success_messages = qs.success.pop(10)
    failure_messages = qs.failure.pop(10)

    pprint.pprint("SUCCESS : {}".format(success_messages))
    pprint.pprint("FAILURE : {}".format(failure_messages))