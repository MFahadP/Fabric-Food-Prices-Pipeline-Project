# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1010ebbb-276d-4e3b-89d4-983874225852",
# META       "default_lakehouse_name": "Food_Prices",
# META       "default_lakehouse_workspace_id": "20230ad8-1e4f-4f65-82a0-a35e6eef0951",
# META       "known_lakehouses": [
# META         {
# META           "id": "1010ebbb-276d-4e3b-89d4-983874225852"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql import functions as f
import datetime

data = [{'LandfallDate': datetime.date(2019,3,14), 'CycloneName': 'Cyclone Idai'}, {'LandfallDate': datetime.date(2019,4,25), 'CycloneName': 'Cyclone Kenneth'}, {'LandfallDate': datetime.date(2021,1,22), 'CycloneName': 'Cyclone Eloise'}]

schema = StructType([
    StructField('LandfallDate', DateType()),
    StructField('CycloneName', StringType())
])

df = spark.createDataFrame(data, schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *

# Define schema for the commodities dimension table
DeltaTable.createIfNotExists(spark) \
 .tableName('Food_Prices.fact_cyclones') \
 .addColumn('LandfallDate', DateType()) \
 .addColumn('CycloneName', StringType()) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

DeltaTable = DeltaTable.forPath(spark, 'Tables/fact_cyclones')

# Upsert operation that checks if row matches based on the Date
DeltaTable.alias('cyclones') \
 .merge(df.alias('updates'), 'cyclones.CycloneName = updates.CycloneName') \
 .whenMatchedUpdate(set = {}) \
 .whenNotMatchedInsert(values = {
    'LandfallDate': 'updates.LandfallDate',
    'CycloneName': 'updates.CycloneName'
 }) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
