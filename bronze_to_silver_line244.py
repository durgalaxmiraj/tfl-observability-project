# Databricks notebook source
from pyspark.sql.functions import col

bronze_path = "s3://tfl-line244-bronze"

# Use read_files() to leverage external location credentials
df_bronze = spark.sql(f"SELECT * FROM read_files('{bronze_path}', format => 'json')")

df_bronze.printSchema()
display(df_bronze.limit(5))


# COMMAND ----------


from pyspark.sql.functions import col, count

bronze_path = "s3://tfl-line244-bronze"
df_bronze = spark.sql(f"SELECT * FROM read_files('{bronze_path}', format => 'json')")

# Check distribution of status codes
print("=== Status Code Distribution ===")
df_bronze.groupBy("status_code").agg(count("*").alias("count")).orderBy("status_code").show()

# Look at a successful response (status 200)
print("\n=== Sample Successful Response (status_code = 200) ===")
display(df_bronze.filter(col("status_code") == 200).limit(3))

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.tfl_reliability")


# COMMAND ----------

spark.sql("SHOW SCHEMAS IN workspace").show()


# COMMAND ----------

spark.sql("SHOW EXTERNAL LOCATIONS")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.tfl_raw;
# MAGIC

# COMMAND ----------



# COMMAND ----------

display(spark.sql("SHOW EXTERNAL LOCATIONS"))

# COMMAND ----------

# DBTITLE 1,Silver Layer Transformations
from pyspark.sql.functions import (
    col, to_timestamp, current_timestamp, explode, from_json, get_json_object
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, ArrayType, TimestampType
)

bronze_path = "s3://tfl-line244-bronze"

# Expected TFL Line 244 arrivals schema (from TFL StopPoint/Arrivals API)
tfl_arrival_schema = ArrayType(StructType([
    StructField("id", StringType()),
    StructField("operationType", IntegerType()),
    StructField("vehicleId", StringType()),
    StructField("naptanId", StringType()),
    StructField("stationName", StringType()),
    StructField("lineId", StringType()),
    StructField("lineName", StringType()),
    StructField("platformName", StringType()),
    StructField("direction", StringType()),
    StructField("bearing", StringType()),
    StructField("destinationNaptanId", StringType()),
    StructField("destinationName", StringType()),
    StructField("timestamp", StringType()),
    StructField("timeToStation", IntegerType()),
    StructField("currentLocation", StringType()),
    StructField("towards", StringType()),
    StructField("expectedArrival", StringType()),
    StructField("timeToLive", StringType()),
    StructField("modeName", StringType()),
]))

# Read bronze data with rescued data to capture schema mismatches
df_bronze = spark.sql(f"SELECT * FROM read_files('{bronze_path}', format => 'json')")

# === Silver Transformation ===
# 1. Parse timestamps and classify responses
# NOTE: Successful arrival records have status_code = NULL (stored as individual JSON objects)
#       Error records have status_code = 429
df_silver_base = (
    df_bronze
    .withColumn("fetched_at_ts", to_timestamp("fetched_at"))
    .withColumn("is_error", col("status_code").isNotNull())  # errors have a status_code
    .withColumn("processed_at", current_timestamp())
)

# 2. Error tracking table (records with status_code like 429)
df_errors = (
    df_silver_base
    .filter(col("is_error"))
    .select(
        col("fetched_at_ts").alias("fetched_at"),
        col("status_code"),
        col("data.error").alias("error_message"),
        col("processed_at")
    )
)

# 3. Successful arrivals - already at top level (status_code is NULL)
df_arrivals = (
    df_silver_base
    .filter(~col("is_error"))
    .select(
        col("fetched_at_ts").alias("fetched_at"),
        col("id").alias("prediction_id"),
        col("vehicleId").alias("vehicle_id"),
        col("naptanId").alias("stop_id"),
        col("stationName").alias("stop_name"),
        col("lineId").alias("line_id"),
        col("lineName").alias("line_name"),
        col("platformName").alias("platform_name"),
        col("direction"),
        col("destinationNaptanId").alias("destination_stop_id"),
        col("destinationName").alias("destination_name"),
        to_timestamp(col("expectedArrival")).alias("expected_arrival"),
        col("timeToStation").cast("int").alias("time_to_station_sec"),
        col("currentLocation").alias("current_location"),
        col("towards"),
        col("modeName").alias("mode_name"),
        col("processed_at")
    )
)

print(f"Total bronze records: {df_bronze.count()}")
print(f"Error records (status_code NOT NULL): {df_errors.count()}")
print(f"Successful arrival records: {df_arrivals.count()}")
print("\n=== Arrivals Silver Schema ===")
df_arrivals.printSchema()
display(df_arrivals.limit(5))

# COMMAND ----------

# DBTITLE 1,Write Silver Tables to Unity Catalog
# Write error tracking table
df_errors.write.mode("overwrite").saveAsTable("tfl_line244_bronze")

# Write successful arrivals with expanded schema
# Using mergeSchema to handle evolving data structure as new API responses arrive
df_arrivals.write.mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("workspace.tfl_raw.line244_arrivals_raw")

print("Silver tables written to workspace.tfl_raw:")
print("  - line244_api_errors: tracks failed API calls")
print("  - line244_arrivals_raw: flattened TFL arrival predictions")
print("\n=== Final Table Schemas ===")
print("\n--- line244_arrivals_raw ---")
spark.table("workspace.tfl_raw.line244_arrivals_raw").printSchema()
display(spark.sql("SHOW TABLES IN workspace.tfl_raw"))

# COMMAND ----------

# DBTITLE 1,Query line244_arrivals_raw
df = spark.table("workspace.tfl_raw.line244_arrivals_raw")
print(f"Row count: {df.count()}")
df.printSchema()
display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,Data Quality - NOT NULL Checks (Silver Layer)
from pyspark.sql.functions import col, count, when

# Read silver arrivals table
df_silver = spark.table("workspace.tfl_raw.line244_arrivals_raw")

total_rows = df_silver.count()
print(f"=== Data Quality: NOT NULL Checks (Silver Layer) ===")
print(f"Total silver records: {total_rows}")

if total_rows > 0:
    # Check nulls for destination_name, direction, current_location
    null_destination = df_silver.filter(col("destination_name").isNull()).count()
    null_direction = df_silver.filter(col("direction").isNull()).count()
    null_location = df_silver.filter(col("current_location").isNull()).count()

    print(f"\n--- NOT NULL Checks ---")
    print(f"  destination_name NULLs:  {null_destination} / {total_rows}")
    print(f"  direction NULLs:         {null_direction} / {total_rows}")
    print(f"  current_location NULLs:  {null_location} / {total_rows}")

    assert null_destination == 0, (
        f"FAILED: destination_name has {null_destination} NULL values ({null_destination/total_rows*100:.1f}%)"
    )
    assert null_direction == 0, (
        f"FAILED: direction has {null_direction} NULL values ({null_direction/total_rows*100:.1f}%)"
    )
    assert null_location == 0, (
        f"FAILED: current_location has {null_location} NULL values ({null_location/total_rows*100:.1f}%)"
    )
    print("\n\u2713 All NOT NULL checks PASSED")
else:
    print("\n\u26a0 No records in silver table. Pipeline awaiting successful TFL API data.")
    print("  Checks will enforce NOT NULL for:")
    print("    - destination_name")
    print("    - direction")
    print("    - current_location")

# COMMAND ----------

# DBTITLE 1,Gold Layer - Average Daily Delay per Station
from pyspark.sql.functions import col, avg, to_date, round as spark_round, count

# Read from the silver arrivals table
df_arrivals = spark.table("workspace.tfl_raw.line244_arrivals_raw")

# Remove null values before aggregation to ensure gold layer data quality
df_arrivals_clean = (
    df_arrivals
    .filter(col("time_to_station_sec").isNotNull())
    .filter(col("stop_name").isNotNull())
    .filter(col("expected_arrival").isNotNull())
)

print(f"Silver records (total): {df_arrivals.count()}")
print(f"Silver records (after null removal): {df_arrivals_clean.count()}")

# Gold aggregation: Average daily delay (timeToStation in seconds) per station
df_gold_avg_delay = (
    df_arrivals_clean
    .withColumn("arrival_date", to_date("expected_arrival"))
    .groupBy("stop_name", "arrival_date")
    .agg(
        spark_round(avg("time_to_station_sec"), 2).alias("avg_delay_seconds"),
        spark_round(avg("time_to_station_sec") / 60, 2).alias("avg_delay_minutes"),
        count("*").alias("num_predictions")
    )
    .orderBy("arrival_date", "stop_name")
)

# Write gold table
df_gold_avg_delay.write.mode("overwrite").saveAsTable("workspace.tfl_raw.gold_avg_daily_delay")

print("Gold table created: workspace.tfl_raw.gold_avg_daily_delay")
print(f"Row count: {df_gold_avg_delay.count()}")
df_gold_avg_delay.printSchema()
display(df_gold_avg_delay.limit(20))

# COMMAND ----------

# DBTITLE 1,Average Daily Delay per Station - Visualization
from pyspark.sql.functions import col, avg, to_date, round as spark_round, count, countDistinct, min as spark_min, max as spark_max

# Read from silver table for extended aggregation
df_silver = spark.table("workspace.tfl_raw.line244_arrivals_raw")

# Filter nulls for key fields
df_clean = (
    df_silver
    .filter(col("time_to_station_sec").isNotNull())
    .filter(col("stop_name").isNotNull())
    .filter(col("expected_arrival").isNotNull())
)

# Aggregation with platform_name, expected_arrival, vehicle_id
df_agg = (
    df_clean
    .withColumn("arrival_date", to_date("expected_arrival"))
    .groupBy("stop_name", "arrival_date")
    .agg(
        spark_round(avg("time_to_station_sec"), 2).alias("avg_delay_seconds"),
        spark_round(avg("time_to_station_sec") / 60, 2).alias("avg_delay_minutes"),
        count("*").alias("num_predictions"),
        countDistinct("platform_name").alias("distinct_platforms"),
        countDistinct("vehicle_id").alias("distinct_vehicles"),
        spark_min("expected_arrival").alias("earliest_arrival"),
        spark_max("expected_arrival").alias("latest_arrival")
    )
    .orderBy("arrival_date", "stop_name")
)

print(f"Total aggregated records: {df_agg.count()}")
df_agg.printSchema()
display(df_agg)

# COMMAND ----------

# DBTITLE 1,Gold Aggregation by Location, Direction, Destination
from pyspark.sql.functions import col, avg, count, round as spark_round, countDistinct

# Read from silver table
df_silver = spark.table("workspace.tfl_raw.line244_arrivals_raw")

# Filter nulls for key fields
df_clean = (
    df_silver
    .filter(col("time_to_station_sec").isNotNull())
    .filter(col("current_location").isNotNull())
    .filter(col("towards").isNotNull())
    .filter(col("direction").isNotNull())
    .filter(col("destination_name").isNotNull())
)

# Aggregation by current_location, towards, direction, destination_name
df_agg_location = (
    df_clean
    .groupBy("current_location", "towards", "direction", "destination_name")
    .agg(
        spark_round(avg("time_to_station_sec"), 2).alias("avg_delay_seconds"),
        spark_round(avg("time_to_station_sec") / 60, 2).alias("avg_delay_minutes"),
        count("*").alias("num_predictions"),
        countDistinct("vehicle_id").alias("distinct_vehicles"),
        countDistinct("stop_name").alias("distinct_stops")
    )
    .orderBy(col("num_predictions").desc())
)

print(f"Silver records (after null removal): {df_clean.count()}")
print(f"Aggregated records: {df_agg_location.count()}")
df_agg_location.printSchema()
display(df_agg_location)

# COMMAND ----------

