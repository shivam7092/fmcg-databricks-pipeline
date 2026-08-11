# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

# creating widgets for our project 

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source","customers","Data Source")

# COMMAND ----------

# Retrieve widget values to dynamically reference catalog and table

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")


# COMMAND ----------

# specifying the bucket path

base_path = f's3://sportsbar-s3-sp/{data_source}/*.csv'

print(base_path)

# COMMAND ----------

# creating dataframe from our bucket path

df = (
    spark.read.format('csv')    
        .option('header', True)
        .option('inferSchema', True)
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp()) # Add lineage columns to track ingestion time and source file details
        .select("*","_metadata.file_name", "_metadata.file_size") # metadata is hidden column which spark automatically provides while reading the csv,json,parquet
    )

display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# writing raw data to bronze layer

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_duplicate = df_bronze.groupBy("customer_id").count().filter(F.col("count")>1)

display(df_duplicate)

# COMMAND ----------

print(df_bronze.count())
df_silver = df_bronze.dropDuplicates(['customer_id'])
print(df_silver.count())

# COMMAND ----------

# checking the extra space in the customer name column

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

# removing extra spaces from the customer name column

df_silver = df_silver.withColumn(
    "customer_name",
    F.trim(F.col("customer_name"))
)

# COMMAND ----------

# checking again 
display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

# checking for distinct city names

df_silver.select('city').distinct().show()

# COMMAND ----------

# typos -> correct names
city_mapping = {
    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',
    
    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad',
    
    'NewDelhi': 'New Delhi',
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi'
}

allowed = ["Bengaluru", "Hyderabad", "New Delhi"]

# Replacing the names of the cities with the valid names and allowed names

df_silver = (
    df_silver.replace(city_mapping, subset = ["city"])
        .withColumn(
            "city",
            F.when(F.col("city").isNull(), None)
            .when(F.col("city").isin(allowed), F.col("city"))
            .otherwise(None)
        )
    )


# COMMAND ----------

# checking the customer_name columns 

df_silver.select('customer_name').distinct().show(truncate = False)

# COMMAND ----------

# Title case fix of customer_name column

df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(), None)
    .otherwise(F.initcap("customer_name"))
)

df_silver.select("customer_name").distinct().show(truncate= False)

# COMMAND ----------

# handeling the NULL cities 
df_silver.filter(F.col("city").isNull()).show()

# COMMAND ----------

# Business Confirmation Note: City corrections confirmed by business team

customer_city_fix = {
    #sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",
    
    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"

}

df_city_fix = spark.createDataFrame(
    [(k, v) for k, v in customer_city_fix.items()],
    ["customer_id", "fixed_city"]
)

df_city_fix.show()

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_city_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city") # Replace null with Fixed city
    )
    .drop("fixed_city")
)

display(df_silver)

# COMMAND ----------

# checking the schema once again
df_silver.printSchema()

# COMMAND ----------

# in our parent company gold layer for customer, customer_id is string
# converting customer_id column to string

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))

df_silver.printSchema()

# COMMAND ----------

# build final customer Column: "CustomerName-City" or "CustomerName-Unknown"
# Create composite customer key and add static business attributes for downstream reporting
df_silver = (
    df_silver
    .withColumn(
        "customer",
        F.concat_ws("-", "customer_name", F.coalesce(F.col("city"), F.lit("Unknown")))
    )
    # static attributes aligned with parent data model
    .withColumn("market", F.lit("India"))
    .withColumn("platform", F.lit("Sports Bar"))
    .withColumn("channel", F.lit("Acquisition"))
)

display(df_silver.limit(10))

# COMMAND ----------

# writing data to silver table 

df_silver.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .option("mergeSchema", "true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

#take req calls only 
#'customer_id, customer_name, city, read_timestamp, file_name, file_size, customer, market, platform, channel"

df_gold = df_silver.select("customer_id", "customer_name", "city", "read_timestamp", "file_name", "file_size", "customer", "market", "platform", "channel")

# COMMAND ----------

# writing it to gold layer

df_gold.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")



# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_customers")
df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

# Upsert into Gold dimension table — update existing customers, insert new ones

delta_table.alias("target").merge(
    source = df_child_customers.alias("source"),
    condition = "target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

