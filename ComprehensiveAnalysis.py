%pyspark

#1. Identify the top 5 merchants in each city based on rating frequency, average rating, and check-in frequency.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, desc, rank
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Top 5 Merchants in Each City") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the necessary datasets from Hive
df_review = spark.table('review')
df_checkin = spark.table('checkin')
df_business = spark.table('business')  # Add this line to read the business table

# Calculate rating frequency by merchant
rating_frequency = df_review.groupBy("rev_business_id").agg(count("*").alias("rating_frequency"))

# Calculate average rating by merchant
avg_rating = df_review.groupBy("rev_business_id").agg(avg("rev_stars").alias("avg_rating"))

# Calculate check-in frequency by merchant
checkin_frequency = df_checkin.groupBy("business_id").agg(count("*").alias("checkin_frequency"))

# Join with the business table to add the year
combined_data = rating_frequency.join(avg_rating, "rev_business_id") \
    .join(checkin_frequency, rating_frequency["rev_business_id"] == checkin_frequency["business_id"], "inner") \
    .join(df_business.select("business_id", "name", "city"), rating_frequency["rev_business_id"] == df_business["business_id"], "inner") \
    .drop(checkin_frequency["business_id"]) \
    .drop(df_business["business_id"])  # Drop duplicate columns

# Define window specification for ranking within each city
window_spec = Window.partitionBy("city").orderBy(desc("rating_frequency"), desc("avg_rating"), desc("checkin_frequency"))

# Rank merchants within each city based on rating frequency, average rating, and check-in frequency
ranked_merchants = combined_data.withColumn("rank_within_city", rank().over(window_spec))

# Filter to get only top 5 merchants in each city
top_merchants_in_each_city = ranked_merchants.filter(col("rank_within_city") <= 5)

# Show the top 5 merchants in each city based on rating frequency, average rating, and check-in frequency
z.show(top_merchants_in_each_city)

# Stop the SparkSession
spark.stop()
