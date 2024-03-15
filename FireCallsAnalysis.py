from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, to_timestamp, month, weekofyear

# Create SparkSession
spark = SparkSession.builder \
    .appName("FireCallsAnalysis") \
    .getOrCreate()

# Load data
fire_df = spark.read.csv("DataSet/sf-fire-calls.csv", header=True, inferSchema=True)

# Filter data
few_fire_df = fire_df \
    .select("IncidentNumber", "AvailableDtTm", "CallType") \
    .where(col("CallType") != "Medical Incident")

few_fire_df.show(5, truncate=False)

# Count distinct call types
distinct_call_types = fire_df \
    .select("CallType") \
    .where(col("CallType").isNotNull()) \
    .agg(countDistinct("CallType").alias("DistinctCallTypes"))

distinct_call_types.show()

# List distinct call types
distinct_call_types_list = fire_df \
    .select("CallType") \
    .where(col("CallType").isNotNull()) \
    .distinct()

distinct_call_types_list.show(10, False)

# Rename column and filter response times
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
new_fire_df \
    .select("ResponseDelayedinMins") \
    .where(col("ResponseDelayedinMins") > 5) \
    .show(5, False)

# Convert date columns to timestamp
cleaned_df = fire_df \
    .withColumn('IncidentDate', to_timestamp(col('CallDate'), 'MM/dd/yyyy')) \
    .drop('CallDate') \
    .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy")) \
    .drop("WatchDate") \
    .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a")) \
    .drop("AvailableDtTm")

cleaned_df.select("IncidentDate", "OnWatchDate", "AvailableDtTS").show(5, False)

# Most common call types
most_common_call_types = fire_df.groupBy("CallType").count().orderBy(col("count").desc()).show(1)

# Aggregations
aggregations = fire_df.agg({"NumAlarms": "sum", "Delay": "avg", "Delay": "min", "Delay": "max"}).show()

# Fire calls in 2018
fire_calls_2018 = fire_df.filter(col("CallDate").like("%2018%")).select("CallType").distinct().show()

# Months with highest calls in 2018
months_with_highest_calls = fire_df.filter(col("CallDate").like("%2018%")) \
    .withColumn("Month", month(to_timestamp(col('CallDate'), 'MM/dd/yyyy'))) \
    .groupBy("Month").count().orderBy(col("count").desc()).show(1)

# Neighborhood with most calls in 2018
neighborhood_most_calls = fire_df.filter(col("CallDate").like("%2018%")) \
    .groupBy("Neighborhood").count().orderBy(col("count").desc()).show(1)

# Neighborhoods with worst response times in 2018
worst_response_times_neighborhood = fire_df.filter(col("CallDate").like("%2018%")) \
    .groupBy("Neighborhood").avg("Delay").orderBy(col("avg(Delay)").desc()).show(1)

# Week with most calls in 2018
week_most_calls = fire_df.filter(col("CallDate").like("%2018%")) \
    .withColumn("Week", weekofyear(to_timestamp(col('CallDate'), 'MM/dd/yyyy'))) \
    .groupBy("Week").count().orderBy(col("count").desc()).show(1)

# Correlation between Neighborhood and Zipcode
correlation = fire_df.groupBy("Neighborhood", "Zipcode").count().orderBy(col("count").desc()).show()


# Write to Parquet: Parquet Files:
#
# Storing Data: Parquet files are a columnar storage format, ideal for analytical workloads with large datasets. You can use libraries like Apache Spark or PyArrow to write DataFrame objects directly to Parquet files efficiently.
# Reading Data: Similarly, you can use these libraries to read Parquet files back into DataFrame objects or other data structures, making it suitable for data analysis tasks.
# SQL Tables:
#
# Storing Data: SQL tables are structured data storage in relational databases, suitable for transactional workloads. You can use SQL queries or ORM libraries like SQLAlchemy to insert data into SQL tables.
# Reading Data: Retrieving data from SQL tables can be done using SQL queries or ORM libraries. Data fetched from SQL tables can be stored in appropriate data structures for further processing.
