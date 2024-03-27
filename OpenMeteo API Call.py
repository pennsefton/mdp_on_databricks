# Databricks notebook source
import requests
import json
from azure.storage.blob import BlobServiceClient
import datetime

# COMMAND ----------

def write_to_blob(file, connection_string, container_name, blob_name):

    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get a BlobClient object
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    # Upload the response JSON to Azure storage
    blob_client.upload_blob(file, overwrite=True)

# COMMAND ----------

# dbutils.widgets.text("locations", '[{"name":"Tampa","latitude":27.9506,"longitude":-82.4572},{"name":"Gainesville","latitude":29.6516,"longitude":-82.3248},{"name":"Tucson","latitude":32.2217,"longitude":-110.9265},{"name":"Seattle","latitude":47.6062,"longitude":-122.3321},{"name":"Tallahassee","latitude":30.4383,"longitude":-84.2807},{"name":"Los Angeles","latitude":34.0522,"longitude":-118.2437}]')
# dbutils.widgets.text("container_name", "openmeteo")
# dbutils.widgets.text("start", "2022-01-01")
# dbutils.widgets.text("end", "2022-01-31")
# dbutils.widgets.text("connection_string", "<your_blob_connection_string>")
# dbutils.widgets.text("base_url", "https://archive-api.open-meteo.com/v1/archive")
 
# dbutils.widgets.remove("base_url") 
# dbutils.widgets.remove("connection_string") 
# dbutils.widgets.remove("container_name") 
# dbutils.widgets.remove("end")
# dbutils.widgets.remove("start")
# dbutils.widgets.remove("locations")

# COMMAND ----------

start = dbutils.widgets.get('start')
end = dbutils.widgets.get('end')
locations = json.loads(dbutils.widgets.get('locations'))
connection_string = dbutils.widgets.get('connection_string')
container_name = dbutils.widgets.get('container_name')
base_url = dbutils.widgets.get('base_url')

# COMMAND ----------

# Generate a parms dictionary for API call
base_url = 'https://archive-api.open-meteo.com/v1/archive'

params = {
    'start_date': start,
    'end_date': end,
    'hourly': 'temperature_2m,precipitation,cloudcover,direct_normal_irradiance,windspeed_10m',
    'temperature_unit': 'fahrenheit',
    'windspeed_unit': 'mph',
    'precipitation_unit': 'inch'
}

for location in locations:
    params['latitude'] = location['latitude']
    params['longitude'] = location['longitude']
    response = requests.get(base_url, params=params).json()
    file = json.dumps(response)
    current_date = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    location_name = location['name']
    blob_name = f"{start}-{end}/open_meteo_data_{location_name}{current_date}.json"
    write_to_blob(file, connection_string, container_name, blob_name)
