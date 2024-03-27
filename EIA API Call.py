# Databricks notebook source
import requests
import json
from azure.storage.blob import BlobServiceClient
import datetime

# COMMAND ----------

# # Create widgets for each variable
# dbutils.widgets.text("base_url", "https://api.eia.gov/v2/electricity/rto/fuel-type-data/data")
# dbutils.widgets.text("start", "2022-01-01T00")
# dbutils.widgets.text("end", "2022-01-31T00")
# dbutils.widgets.text("offset", "0")
# dbutils.widgets.text("api_key", "<your_api_key_here>")
# dbutils.widgets.text("connection_string", "<blob_storage_connection_string>")
# dbutils.widgets.text("container_name", "eia")

# dbutils.widgets.remove("api_key") 
# dbutils.widgets.remove("base_url") 
# dbutils.widgets.remove("connection_string") 
# dbutils.widgets.remove("container_name") 
# dbutils.widgets.remove("end")
# dbutils.widgets.remove("offset") 
# dbutils.widgets.remove("start")  

# COMMAND ----------

def write_to_blob(file, connection_string, container_name, blob_name):

    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a BlobClient object
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Upload the response JSON to Azure storage
    blob_client.upload_blob(file, overwrite=True)

# COMMAND ----------

base_url = dbutils.widgets.get('base_url')
start = dbutils.widgets.get('start')
end = dbutils.widgets.get('end')
offset = int(dbutils.widgets.get('offset'))
api_key = dbutils.widgets.get('api_key')
connection_string = dbutils.widgets.get('connection_string')
container_name = dbutils.widgets.get('container_name')

# COMMAND ----------

params = {
    'frequency': 'hourly',
    'data[0]': 'value',
    'facets[respondent][]': ['GVL', 'LDWP', 'SCL', 'TAL', 'TEC', 'TEPC'],
    'start': start,
    'end': end,
    'sort[0][column]': 'period',
    'sort[0][direction]': 'asc',
    'offset': offset,
    'api_key':  api_key# change later to kv
}

sample_response_json = requests.get(base_url, params=params).json()
total_records = int(sample_response_json['response']['total'])

while offset < total_records:
    params['offset'] = offset
    response = requests.get(base_url, params=params).json()
    file = json.dumps(response)
    current_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    blob_name = f"{start}-{end}/eia_data_{current_date}.json"
    write_to_blob(file, connection_string, container_name, blob_name)
    offset += 5000
