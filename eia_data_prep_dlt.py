# Databricks notebook source
import dlt

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import explode

response_df = spark.read.format('json').load('abfss://eia@managedstoragedbx.dfs.core.windows.net/2022-01-01T00-2022-01-31T00/')
responses = response_df.select(explode('response.data').alias('data_values'))

select_cols = ['fueltype','period','respondent','respondent-name','type-name','value','value-units']
df_cols = [f'data_values.{col}' for col in select_cols]

eia_df = (
            responses.select(df_cols)
)

# COMMAND ----------

json_path = "abfss://eia@managedstoragedbx.dfs.core.windows.net/2022-01-01T00-2022-01-31T00/"

@dlt.table(
    table_properties={
        "myCompanyPipeline.quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
   },
  comment="EIA Data from storage."
)
def eia_raw():
  response_df = (
                  spark.readStream.format("cloudFiles")
                                  .option("cloudFiles.format", "json")
                                  .option("cloudFiles.inferColumnTypes", "true")
                                  .load(json_path)
  )
  #response_df = spark.read.format('json').load('abfss://eia@managedstoragedbx.dfs.core.windows.net/2022-01-01T00-2022-01-31T00/')
  responses = response_df.select(explode('response.data').alias('data_values'))

  select_cols = ['fueltype','period','respondent','respondent-name','type-name','value','value-units']
  df_cols = [f'data_values.{col}' for col in select_cols]

  eia_df = (
    responses.select(df_cols)
  )

  return eia_df


# COMMAND ----------

# json_path = "abfss://openmeteo@managedstoragedbx.dfs.core.windows.net/2022-01-01-2022-01-31/"

# @dlt.table(
#     table_properties={
#         "myCompanyPipeline.quality": "bronze",
#         "pipelines.autoOptimize.managed": "true"
#    },
#   comment="openemeteo Data from storage."
# )
# def openmeteo_raw():
#   (
#     spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "json")
#         .option("cloudFiles.inferColumnTypes", "true")
#         .load(json_path)
#   )

