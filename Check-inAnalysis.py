%pyspark

#1. Count the yearly check-in frequency.

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Yearly Check-in Frequency") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the check-in dataset from the Hive table
checkin_df = spark.table('checkin')

# Extract the year from the 'checkin_dates' column
checkin_df = checkin_df.withColumn("year", year("checkin_dates"))

# Group by year and count the number of check-ins
yearly_checkin_frequency = checkin_df.groupBy("year").agg(count("*").alias("checkin_count"))

# Show the results
z.show(yearly_checkin_frequency.orderBy("year"))



%pyspark

#2. Analyze the check-in frequency for each hour of the day.
from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, count

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Hourly Check-in Frequency") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the check-in dataset from the Hive table
checkin_df = spark.table('checkin')

# Extract the hour from the 'checkin_dates' column
checkin_df = checkin_df.withColumn("hour_of_day", hour("checkin_dates"))

# Group by hour of the day and count the number of check-ins
hourly_checkin_frequency = checkin_df.groupBy("hour_of_day").agg(count("*").alias("checkin_count"))

# Show the results
z.show(hourly_checkin_frequency.orderBy("hour_of_day"))


%pyspark

#3. Identify the cities where check-ins are most frequent.

from pyspark import HiveContext
from pyspark.sql.functions import count, col

# Create a HiveContext
hc = HiveContext(sc)

# Load the 'checkin' dataset
checkin_df = hc.table("checkin")

# Load the 'business' dataset
business_df = hc.table("business")

# Join the two datasets on the 'business_id' column
joined_df = checkin_df.join(business_df, checkin_df.business_id == business_df.business_id)

# Group by city and count the number of check-ins
city_checkin_frequency = joined_df.groupBy("city").agg(count("*").alias("checkin_count")).orderBy(col("checkin_count").desc())

# Show the top 20 cities with the most frequent check-ins
z.show(city_checkin_frequency.limit(20))



%pyspark

#4. Show the top 20 merchants with the most frequent check-ins

from pyspark import HiveContext
from pyspark.sql.functions import count, col

# Create a HiveContext
hc = HiveContext(sc)

# Load the 'checkin' dataset
checkin_df = hc.table("checkin")

# Load the 'business' dataset
business_df = hc.table("business")

# Join the two datasets on the 'business_id' column
joined_df = checkin_df.join(business_df, checkin_df.business_id == business_df.business_id)

# Group by merchant name and count the number of check-ins
merchant_checkin_frequency = joined_df.groupBy("name").agg(count("*").alias("checkin_count")).orderBy(col("checkin_count").desc())

# Set limit to 20 for displaying top merchants
top_merchants = merchant_checkin_frequency.limit(20)

# Show the top 20 merchants with the most frequent check-ins
z.show(top_merchants)



