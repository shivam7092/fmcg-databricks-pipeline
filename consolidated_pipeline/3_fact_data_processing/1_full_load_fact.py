# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-s3-sp/{data_source}'
landing_path = f"{base_path}/landing"
processed_path = f"{base_path}/processed/"

print("Base Path:", base_path)
print("Landing Path:", landing_path)
print("Processed Path:", processed_path)

# define the table
bronze_table =  f"{catalog}.{bronze_schema}.{data_source}"
silver_table = f"{catalog}.{silver_schema}.{data_source}"
gold_table = f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

# creating the orders table using the landing path

df = spark.read.options(header = True, inferSchema = True).csv(f"{landing_path}/*.csv").withColumn("read_timestamp", F.current_timestamp()).select("*", "_metadata.file_name", "_metadata.file_size")

print("Total Rows: ", df.count())
df.show(5)

# COMMAND ----------

display(df.limit(20))

# COMMAND ----------

# writing raw data to bronze table

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("append")\
    .saveAsTable(bronze_table)

# COMMAND ----------

# moving all files from landing folder to processed

files  =  dbutils.fs.ls(landing_path)

for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver

# COMMAND ----------

# loading all records from the bronze
df_orders = spark.sql(f"SELECT * FROM {bronze_table}")

display(df_orders)

# COMMAND ----------

# transforming the order quantity
# 1. Keep only rows where order_qty is present

df_orders = df_orders.filter(F.col("order_qty").isNotNull())

# 2. clean customer_id -> keep numeric, else set to 999999
df_orders = df_orders.withColumn(
    "customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"), F.col("customer_id"))
    .otherwise("999999")
    .cast("string")
)

# 3. remove weekday name from the date text
# "Tuesday, July 01, 2025" -> "July 01, 2025"
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# 4. Parse order_placement_date using multiple possible formats

df_orders = df_orders.withColumn(
    "order_placement_date", 
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# 5. Drop duplicates

df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# 6. convert product id to string
df_orders = df_orders.withColumn('product_id', F.col('product_id').cast("string"))

# COMMAND ----------

# checking what's the maximum and minimum date
df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).show()

# COMMAND ----------

display(df_orders)

# COMMAND ----------

# we need to add the product code column to our table which we did in the products table

df_products = spark.table("fmcg.silver.products")
display(df_products)

# COMMAND ----------

# to get the productcode from products table to orders table we will join the both table on product_id
df_joined = df_orders.join(df_products, on = 'product_id', how = 'inner').select(df_orders["*"], df_products["product_code"])

df_joined.show(5)

# COMMAND ----------

# performing upsert operation for the silver table
if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)

else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"), "silver_order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_id = bronze.product_id AND silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD

# COMMAND ----------

df_gold = spark.sql(f"select order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table};")

df_gold.show(10)

# COMMAND ----------

# writing it to gold table 

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge with Parent company

# COMMAND ----------

df_child =  spark.sql(f"select date, product_code, customer_code, sold_quantity from {gold_table}")

df_child.show(10)

# COMMAND ----------

df_child.count()

# COMMAND ----------

# first change the date to first day of the month 
# 2025-04-12 --> 2025-04-01

df_monthly = (
    df_child
    # 1. Get the month start date (e.g., 2025-11-30 -> 2025-11-01)
    .withColumn("month_start", F.trunc("date", "MM")) 
    
    # 2. Group at month grain by month_start + product_code + customer_code
    .groupBy("month_start", "product_code", "customer_code")
    .agg(
        F.sum("sold_quantity").alias("sold_quantity")
    )
    # 3. Rename the column back to 'date' to match the target schema
    .withColumnRenamed("month_start", "date") 
)

display(df_monthly.limit(10))

# COMMAND ----------

df_monthly.count()

# COMMAND ----------

df_monthly = (
    df_monthly.withColumn("customer_code", F.col("customer_code").cast("long"))
    .withColumn("sold_quantity", F.col("sold_quantity").cast("long"))
    )

# COMMAND ----------

# mearging with parent , using upsert logic

gold_parent_data = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_data.alias("parent_gold").merge(df_monthly.alias("child_gold"), "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

