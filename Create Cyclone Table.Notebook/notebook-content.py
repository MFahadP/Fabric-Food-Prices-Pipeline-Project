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

data = [{'EventDate': datetime.date(2019,3,14), 'EventName': 'Cyclone Idai'}, {'EventDate': datetime.date(2019,4,25), 'EventName': 'Cyclone Kenneth'}, {'EventDate': datetime.date(2021,1,22), 'EventName': 'Cyclone Eloise'}, {'EventDate': datetime.date(2020,4,1), 'EventName': 'First COVID-19 state of emergency'}, {'EventDate': datetime.date(2023,4,11), 'EventName': 'End of COVID-19 restrictions'}]

schema = StructType([
    StructField('EventDate', DateType()),
    StructField('EventName', StringType())
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
 .tableName('Food_Prices.fact_weather_and_health_events') \
 .addColumn('EventDate', DateType()) \
 .addColumn('EventName', StringType()) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

DeltaTable = DeltaTable.forPath(spark, 'Tables/fact_weather_and_health_events')

# Upsert operation that checks if row matches based on the Date
DeltaTable.alias('events') \
 .merge(df.alias('updates'), 'events.EventName = updates.EventName') \
 .whenMatchedUpdate(set = {}) \
 .whenNotMatchedInsert(values = {
    'EventDate': 'updates.EventDate',
    'EventName': 'updates.EventName'
 }) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
