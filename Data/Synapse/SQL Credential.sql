CREATE DATABASE SCOPED CREDENTIAL cred_ads
WITH
IDENTITY= 'Managed Identity'

create external data source source_silver
WITH(
    location ='https://adsstoragedatalake.blob.core.windows.net/silver',
    CREDENTIAL= cred_ads
)
create external data source source_gold
WITH(
    location ='https://adsstoragedatalake.blob.core.windows.net/gold',
    CREDENTIAL= cred_ads
)




CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)





create external table gold.extsales
with
(
location = 'extsales',
DATA_SOURCE = source_gold,
FILE_FORMAT = format_parquet
)
as 
SELECT * from gold.sales



select * from gold.extsales


