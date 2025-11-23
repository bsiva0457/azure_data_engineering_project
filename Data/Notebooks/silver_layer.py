# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Script
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Loading

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.adsstoragedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adsstoragedatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adsstoragedatalake.dfs.core.windows.net", "4a983f2b-d0ca-4df0-93dc-69b94e4fa22f")
spark.conf.set("fs.azure.account.oauth2.client.secret.adsstoragedatalake.dfs.core.windows.net", "xmx8Q~.cRxe0DGjZ72UpAsaCyedpwbdZKLwN_bXy")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adsstoragedatalake.dfs.core.windows.net", "https://login.microsoftonline.com/bb536d34-6377-48a0-b7a7-1732b4312401/oauth2/token")

# COMMAND ----------

df_cal=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Calendar")


# COMMAND ----------

df_cus=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Customers")

# COMMAND ----------

df_subcat=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adsstoragedatalake.dfs.core.windows.net/Product_Subcategories")

# COMMAND ----------

df_procat=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Product_Categories")

# COMMAND ----------

df_pro=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Products")

# COMMAND ----------

df_ret=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Returns")


# COMMAND ----------

df_sales=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Sales*")



# COMMAND ----------

df_ter=spark.read.format("csv").option("header","true").option("inferSchema","true").load("abfss://bronze@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Territories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformations
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #calender

# COMMAND ----------

df_cal_trans=df_cal.withColumn('Month',month(col('Date')))\
      .withColumn('Year',year(col('Date')))
df_cal_trans.display()

     

# COMMAND ----------

df_cal_trans.write.format('parquet').mode("append").option("path","abfss://silver@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Calendar").save()

# COMMAND ----------

# MAGIC %md
# MAGIC # customers

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus=df_cus.withColumn("fullName",concat(col("prefix"),lit(' '),col("FirstName"),lit(' '),col("LastName")))
df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet').mode("append").option("path","abfss://silver@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Customers").save()

# COMMAND ----------

# MAGIC %md
# MAGIC # sub categories

# COMMAND ----------

df_subcat.display()

# COMMAND ----------

df_subcat.write.format('parquet').mode("append").option("path","abfss://silver@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Subcategories").save()

# COMMAND ----------

df_pro.display()

# COMMAND ----------

df_pro=df_pro.withColumn('productSKU',split(col("productSKU"),'-')[0])\
    .withColumn('productName',split(col('productName')," ")[0])
df_pro.display()

# COMMAND ----------

df_pro.write.format('parquet').mode("append").option("path","abfss://silver@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Products").save()

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet').mode("append").option("path","abfss://silver@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Returns").save()

# COMMAND ----------

df_ter.display()

# COMMAND ----------

df_ter.write.format('parquet').mode("append").option("path","abfss://silver@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Territories").save()

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales = df_sales.withColumn('StockDate',to_timestamp('StockDate'))

# COMMAND ----------

df_sales=df_sales.withColumn('orderNumber',regexp_replace(col('OrderNumber'),"S",'T'))

# COMMAND ----------

df_sales=df_sales.withColumn('Multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

df_sales.write.format('parquet').mode("append").option("path","abfss://silver@adsstoragedatalake.dfs.core.windows.net/AdventureWorks_Sales").save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Sales Analysis

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count('orderNumber').alias('total_order')).display()

# COMMAND ----------

df_procat.display()


# COMMAND ----------

df_ter.display()