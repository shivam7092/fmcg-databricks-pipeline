# Databricks notebook source
# MAGIC %md
# MAGIC ### importing required libraries    

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC #### Loading project utilites and initialize Notebook Widgets

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg","Catalog")
dbutils.widgets.text("data_source", "products", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbar-s3-sp/{data_source}/*.csv'

print(base_path)

# COMMAND ----------

df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", F.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.count()

# COMMAND ----------

df.write\
    .format('delta')\
    .option("delta.enableChangeDataFeed", "true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

df_bronze = spark.sql(f"select * from {catalog}.{bronze_schema}.{data_source}")
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Drop Duplicate

# COMMAND ----------

print(f"rows before removing duplicates: {df_bronze.count()}")

df_silver = df_bronze.dropDuplicates(['product_id'])

print(f"rows after removing duplicates: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. Title case fix

# COMMAND ----------

# findig the distinct values

df_silver.select('category').distinct().show()

# COMMAND ----------

# Title case fix for category column

df_silver = df_silver.withColumn(
    "category",
    F.when(F.col("category").isNull(), None)
    .otherwise(F.initcap("category"))
)


display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. correcting spelling mistakes

# COMMAND ----------


df_silver = (
    df_silver.withColumn(
        "product_name",
        F.regexp_replace(
            F.col("product_name"),
            r"(?i)\bprotien\b",
            "Protein"
        )
    )
    .withColumn(
        "category",
        F.regexp_replace(
            F.col("category"),
            r"(?i)\bprotien\b",
            "Protein"
        )
    )
)


df_silver.select("product_name", "category").show(truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standardizing Customer Attributes to Match Parent Company Data Model

# COMMAND ----------

### 1. adding division column
df_silver = (
    df_silver
    .withColumn(
        "division",
        F.when(F.col("category") == "Energy Bars", "Nutrition Bars")
         .when(F.col("category") == "Protein Bars", "Nutrition Bars")
         .when(F.col("category") == "Granola & Cereals",  "Breakfast Foods")
         .when(F.col("category") == "Recovery Dairy",     "Dairy & Recovery")
         .when(F.col("category") == "Healthy Snacks",     "Healthy Snacks")
         .when(F.col("category") == "Electrolyte Mix",    "Hydration & Electrolytes")
         .otherwise("Other")
    )
)

### 2. variant column
df_silver = df_silver.withColumn(
    "variant",
    F.regexp_extract("product_name", r"\(([^)]*)\)", 1)
)


# COMMAND ----------

# Invalid product_ids are replaced with a fallback value to avoid losing fact records and ensure downstream joins remain consistent

df_silver= (
    df_silver
    # 1. Generate deterministic product_code from product_name
    .withColumn(
        "product_code",
        F.sha2(F.col("product_name").cast("string"), 256)
    )
    # 2. clean product_id: keep only numeric IDs, else set to 999999
    .withColumn(
        "product_id",
        F.when(
            F.col("product_id").cast("string").rlike("^[0-9]+$"),
            F.col("product_id").cast("string")
        ).otherwise(F.lit(999999).cast("string"))
    )
    # 3. Rename product name -> product 
    .withColumnRenamed("product_name", "product")
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# reordering the columns
df_silver = df_silver.select(
    "product_code", "division","category", "product", "variant", "product_id", "read_timestamp", "file_name", "file_size"
    )

display(df_silver)

# COMMAND ----------

# writing it to silver table

df_silver.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed", "true")\
    .option("mergeSchema", "true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### GOLD

# COMMAND ----------

# selecting only required columns for the gold layer as the business requirement

df_silver = spark.sql(f"select * from {catalog}.{silver_schema}.{data_source};")
df_gold = df_silver.select("product_code", "product_id", "division", "category", "product", "variant")

df_gold.show(10)

# COMMAND ----------

# writing data back to Gold table
try:
    df_gold.write\
        .format("delta")\
        .option("delta.enableChangeDataFeed", "true")\
        .mode("overwrite")\
        .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")
except Exception as e:
    print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging data source with parent

# COMMAND ----------

# Merging the parent table (delta_table) with child table
delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_products")
df_child_products = spark.sql(f"SELECT product_code, division, category, product, variant FROM fmcg.gold.sb_dim_products;")
df_child_products.show(5)

# COMMAND ----------

# merging by using Upsert operation

delta_table.alias("target").merge(
    source= df_child_products.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "division": "source.division",
        "category": "source.category",
        "product": "source.product",
        "variant": "source.variant"
    }
).execute()


# COMMAND ----------

