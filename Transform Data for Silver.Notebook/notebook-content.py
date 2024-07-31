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

# ## Transform Data for Silver

# MARKDOWN ********************

# ##### <u>Prepare data and add to a Dataframe</u>

# CELL ********************

from pyspark.sql.types import *
from pyspark.sql.functions import *

path = 'abfss://20230ad8-1e4f-4f65-82a0-a35e6eef0951@onelake.dfs.fabric.microsoft.com/1010ebbb-276d-4e3b-89d4-983874225852/Files/Bronze/Food_Prices.csv'

# Read data from bronze CSV
df = spark.read.format('csv').load(path, header = True)

# Filter to only incude rows with individual prices.
df = df.where(df.market != 'National Average')

# List of rows to transform by format.
date_cols = ['date']
coordinate_cols = ['latitude', 'longitude']
decimal_cols = ['price', 'usdprice']

# Cast columns to correct data types
df2 = df.withColumn('date', to_date('date'))
for x in coordinate_cols:
    df2 = df2.withColumn(x, df2[x].cast(DecimalType(10,8)))
for x in decimal_cols:
    df2 = df2.withColumn(x, df2[x].cast(DecimalType(10,2)))

# Correct errors and transform strings to upper case
df2 = df2.withColumn('admin1', regexp_replace('admin1',"_"," "))
df2 = df2.withColumn('admin2', regexp_replace('admin2',"_"," "))
df2 = df2.withColumn('admin2', regexp_replace('admin2',"Angonia","Angónia"))
df2 = df2.withColumn('admin2', regexp_replace('admin2',"Alto Molocue","Alto Molócuè"))
df2 = df2.withColumn('admin2', regexp_replace('admin2',"Barue","Báruè"))
df2 = df2.withColumn('market', regexp_replace('market',"AngÃ³nia","Angónia"))
df2 = df2.withColumn('market', regexp_replace('market',"Alto MolÃ³cuÃ¨","Alto Molócuè"))
df2 = df2.withColumn('market', regexp_replace('market',"BÃ¡ruÃ¨","Báruè"))
df2 = df2.withColumn('category', initcap('category'))
df2 = df2.withColumn('priceflag', initcap('priceflag'))

# Drop unnecessary column
df2 = df2.drop('currency')

# Rename columns for clarity
df2 = df2.withColumnsRenamed({
    'date': 'Date',
    'admin1': 'Province',
    'admin2': 'City',
    'market': 'Market',
    'latitude': 'Latitude',
    'longitude': 'Longitude',
    'category': 'Category',
    'commodity': 'Commodity',
    'unit': 'Unit',
    'priceflag': 'PriceFlag',
    'pricetype': 'PriceType',
    'price': 'PriceMZN',
    'usdprice': 'PriceUSD'
})

# Remove duplicate header row
df3 = spark.createDataFrame(df2.tail(df2.count()-1), df2.schema)

# Added FileName, Created and Modified columns
df3 = df3.withColumn('FileName', input_file_name()).withColumn('Created', current_timestamp()).withColumn('Modified', current_timestamp())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### <u>Create Silver Table</u>

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *

# Define schema and create table
DeltaTable.createIfNotExists(spark) \
 .tableName("Food_Prices_Silver") \
 .addColumn("Date", DateType()) \
 .addColumn("Province", StringType()) \
 .addColumn("City", StringType()) \
 .addColumn("Market", StringType()) \
 .addColumn("Latitude", DecimalType(10,8)) \
 .addColumn("Longitude", DecimalType(10,8)) \
 .addColumn("Category", StringType()) \
 .addColumn("Commodity", StringType()) \
 .addColumn("Unit", StringType()) \
 .addColumn("PriceFlag", StringType()) \
 .addColumn("PriceType", StringType()) \
 .addColumn("PriceMZN", DecimalType(10,2)) \
 .addColumn("PriceUSD", DecimalType(10,2)) \
 .addColumn("FileName", StringType()) \
 .addColumn("Created", DateType()) \
 .addColumn("Modified", DateType()) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### <u>Load Data to Table</u>

# CELL ********************

from delta.tables import *

table = DeltaTable.forPath(spark, 'Tables/food_prices_silver')

dfUpdates = df3

# Upsert operation that checks if row matches based on the Date, Market, Commodity, Unit and PriceType columns.
table.alias('silver') \
    .merge(
        dfUpdates.alias('updates'),
        'silver.Date = updates.Date and silver.Market = updates.Market and silver.Commodity = updates.Commodity and silver.Unit = updates.Unit and silver.PriceType = updates.PriceType'
    ) \
    .whenMatchedUpdate(set = {}) \
    .whenNotMatchedInsert(values =
        {
            'Date': 'updates.Date',
            'Province': 'updates.Province',
            'City': 'updates.City',
            'Market': 'updates.Market',
            'Latitude': 'updates.Latitude',
            'Longitude': 'updates.Longitude',
            'Category': 'updates.Category',
            'Commodity': 'updates.Commodity',
            'Unit': 'updates.Unit',
            'PriceFlag': 'updates.PriceFlag',
            'PriceType': 'updates.PriceType',
            'PriceMZN': 'updates.PriceMZN',
            'PriceUSD': 'updates.PriceUSD',
            'FileName': 'updates.FileName',
            'Created': 'updates.Created',
            'Modified': 'updates.Modified'
        }
    ) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
