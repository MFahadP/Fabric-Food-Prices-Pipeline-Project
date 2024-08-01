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

# Get data from the silver table
df = spark.read.table('Food_Prices.food_prices_silver')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *

# Define schema for calendar table
DeltaTable.createIfNotExists(spark) \
 .tableName('Food_Prices.dim_calendar_gold') \
 .addColumn('Date', DateType()) \
 .addColumn('Day', IntegerType()) \
 .addColumn('Month', IntegerType()) \
 .addColumn('MonthName', StringType()) \
 .addColumn('Quarter', IntegerType()) \
 .addColumn('Year', IntegerType()) \
 .addColumn('MonthYear', StringType()) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, dayofmonth, month, year, date_format, quarter, concat

#Create dataframe for calendar dimension
dfDimCalendar_gold = df.dropDuplicates(['RecordingDate']).select(col('RecordingDate'), \
 dayofmonth('RecordingDate').alias('Day'), \
 month('RecordingDate').alias('Month'), \
 date_format('RecordingDate', 'MMMM').alias('MonthName'), \
 quarter('RecordingDate').alias('Quarter'), \
 year('RecordingDate').alias('Year'), \
 concat('Month', 'Year').alias('MonthYear') \
).orderBy('RecordingDate')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

DeltaTable = DeltaTable.forPath(spark, 'Tables/dim_calendar_gold')

dfUpdates = dfDimCalendar_gold

# Upsert operation that checks if row matches based on the Date
DeltaTable.alias('gold') \
 .merge(dfUpdates.alias('updates'), 'gold.Date = updates.RecordingDate') \
 .whenMatchedUpdate(set = {}) \
 .whenNotMatchedInsert(values = {
    'Date': 'updates.RecordingDate',
    'Day': 'updates.Day',
    'Month': 'updates.Month',
    'MonthName': 'updates.MonthName',
    'Quarter': 'updates.Quarter',
    'Year': 'updates.Year',
    'MonthYear': 'updates.MonthYear'
 }) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *

# Define schema for the markets dimension table
DeltaTable.createIfNotExists(spark) \
 .tableName('Food_Prices.dim_markets_gold') \
 .addColumn('ID', IntegerType()) \
 .addColumn('MarketName', StringType()) \
 .addColumn('MarketType', StringType()) \
 .addColumn('CityName', StringType()) \
 .addColumn('Province', StringType()) \
 .addColumn('Latitude', DecimalType(10,8)) \
 .addColumn('Longitude', DecimalType(10,8)) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id, col, when, lit, max, coalesce

#Get unique markets and their details from the silver table
dfDimMarkets = (
    df.dropDuplicates(['Market'])
    .select(
        col('Market').alias('MarketName'),
        col('Province'),
        col('City').alias('CityName'),
        col('Latitude'),
        col('Longitude'),
    )
    .withColumn(
        'MarketType',
        when(col('MarketName').contains('rural'), 'Rural').otherwise('Urban'),
    )
)

# Generate a unique ID by incrementing the last ID on the existing table
dfDimMarkets_temp = spark.read.table('Food_Prices.dim_markets_gold')
maxMarketID = dfDimMarkets_temp.select(coalesce(max(col('ID')),lit(0)).alias('maxID')).first()[0]
dfDimMarkets_gold = dfDimMarkets.join(dfDimMarkets_temp, (dfDimMarkets.MarketName == dfDimMarkets_temp.MarketName), 'left_anti')
dfDimMarkets_gold = dfDimMarkets_gold.withColumn('ID', monotonically_increasing_id() + maxMarketID + 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

DeltaTable = DeltaTable.forPath(spark, 'Tables/dim_markets_gold')

dfUpdates = dfDimMarkets_gold

# Upsert operation that checks if row matches based on the Date
DeltaTable.alias('gold') \
 .merge(dfUpdates.alias('updates'), 'gold.MarketName = updates.MarketName') \
 .whenMatchedUpdate(set = {}) \
 .whenNotMatchedInsert(values = {
    'ID': 'updates.ID',
    'MarketName': 'updates.MarketName',
    'MarketType': 'updates.MarketType',
    'CityName': 'updates.CityName',
    'Province': 'updates.Province',
    'Latitude': 'updates.Latitude',
    'Longitude': 'updates.Longitude',
 }) \
 .execute()

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
 .tableName('Food_Prices.dim_commodities_gold') \
 .addColumn('ID', IntegerType()) \
 .addColumn('CommodityName', StringType()) \
 .addColumn('CategoryName', StringType()) \
 .addColumn('Origin', StringType()) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id, col, when, lit, max, coalesce

#Get unique commodities and their details from the silver table
dfDimCommodities = (
    df.dropDuplicates(['Commodity'])
    .select(
        col('Commodity').alias('CommodityName'),
        col('Category').alias('CategoryName'),
    )
    .withColumn(
        'Origin',
        when(col('CommodityName').contains('local'), 'Local').when(col('CommodityName').contains('imported'), 'Imported').otherwise(''),
    )
)

# Generate a unique ID by incrementing the last ID on the existing table
dfDimCommodities_temp = spark.read.table('Food_Prices.dim_commodities_gold')
maxCommodityID = dfDimCommodities_temp.select(coalesce(max(col('ID')),lit(0)).alias('maxID')).first()[0]
dfDimCommodities_gold = dfDimCommodities.join(dfDimCommodities_temp, (dfDimCommodities.CommodityName == dfDimCommodities_temp.CommodityName), 'left_anti')
dfDimCommodities_gold = dfDimCommodities_gold.withColumn('ID', monotonically_increasing_id() + maxCommodityID + 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

DeltaTable = DeltaTable.forPath(spark, 'Tables/dim_commodities_gold')

dfUpdates = dfDimCommodities_gold

# Upsert operation that checks if row matches based on the Date
DeltaTable.alias('gold') \
 .merge(dfUpdates.alias('updates'), 'gold.CommodityName = updates.CommodityName') \
 .whenMatchedUpdate(set = {}) \
 .whenNotMatchedInsert(values = {
    'ID': 'updates.ID',
    'CommodityName': 'updates.CommodityName',
    'CategoryName': 'updates.CategoryName',
    'Origin': 'updates.Origin',
 }) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# Load dimensions onto dataframes
dfDimMarkets_temp = spark.read.table('Food_Prices.dim_markets_gold')
dfDimCommodities_temp = spark.read.table('Food_Prices.dim_commodities_gold')

# Join silver table data with dimensions to retreive the generated IDs, and remove the remaining dimension columns from the fact dataframe
dfFactRecordedPrices_gold = ( \
    df.alias('factPrices') \
    .join( \
        dfDimMarkets_temp.alias('dimMarkets'), \
        df.Market == dfDimMarkets_temp.MarketName, \
    ) \
    .join( \
        dfDimCommodities_temp.alias('dimCommodities'), \
        df.Commodity == dfDimCommodities_temp.CommodityName, \
    ) \
    .select( \
        col('factPrices.RecordingDate'), \
        col('factPrices.Unit'), \
        col('factPrices.PriceFlag'), \
        col('factPrices.PriceType'), \
        col('factPrices.PriceMZN'), \
        col('factPrices.PriceUSD'), \
        col('factPrices.FileName'), \
        col('factPrices.Created'), \
        col('factPrices.Modified'), \
        col('dimMarkets.ID').alias('MarketID'), \
        col('dimCommodities.ID').alias('CommodityID'), \
    ) \
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import *
from delta.tables import *

# Define schema for the fact table
DeltaTable.createIfNotExists(spark) \
 .tableName('Food_Prices.fact_recorded_prices') \
 .addColumn('RecordingDate', DateType()) \
 .addColumn('Unit', StringType()) \
 .addColumn('PriceFlag', StringType()) \
 .addColumn('PriceType', StringType()) \
 .addColumn('PriceMZN', DecimalType(10,2)) \
 .addColumn('PriceUSD', DecimalType(10,2)) \
 .addColumn('FileName', StringType()) \
 .addColumn('Created', DateType()) \
 .addColumn('Modified', DateType()) \
 .addColumn('MarketID', IntegerType()) \
 .addColumn('CommodityID', IntegerType()) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *

DeltaTable = DeltaTable.forPath(spark, 'Tables/fact_recorded_prices')

dfUpdates = dfFactRecordedPrices_gold

# Upsert operation that checks if row matches based on the Date
DeltaTable.alias('gold') \
 .merge(dfUpdates.alias('updates'), 'gold.MarketID = updates.MarketID AND gold.PriceType = updates.PriceType AND gold.CommodityID = updates.CommodityID AND gold.Unit = updates.Unit') \
 .whenMatchedUpdate(set = {}) \
 .whenNotMatchedInsert(values = {
    'RecordingDate': 'updates.RecordingDate',
    'Unit': 'updates.Unit',
    'PriceFlag': 'updates.PriceFlag',
    'PriceType': 'updates.PriceType',
    'PriceMZN': 'updates.PriceMZN',
    'PriceUSD': 'updates.PriceUSD',
    'FileName': 'updates.FileName',
    'Created': 'updates.Created',
    'Modified': 'updates.Modified',
    'MarketID': 'updates.MarketID',
    'CommodityID': 'updates.CommodityID'
    }) \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
