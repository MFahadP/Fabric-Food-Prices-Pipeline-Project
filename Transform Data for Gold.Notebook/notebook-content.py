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

# MARKDOWN ********************

# ## Transform Data for Gold

# CELL ********************

df = spark.read.table('Food_Prices.food_prices_silver')

display(df.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
