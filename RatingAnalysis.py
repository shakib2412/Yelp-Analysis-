%pyspark

# 1. Analyze the distribution of ratings (1-5)

# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

# Create SparkSession
spark = SparkSession.builder \
    .appName("Rating Distribution Analysis") \
    .getOrCreate()

# Read the 'review' table from Hive
review_df = spark.sql("SELECT * FROM review")

# Calculate the distribution of ratings
rating_distribution = review_df.groupBy('rev_stars') \
    .agg(count('*').alias('count')) \
    .orderBy('rev_stars')

# Show the distribution of ratings
z.show(rating_distribution)


%pyspark

# 2. Show the frequency of ratings on each day of the week

# Import necessary functions
from pyspark.sql.functions import date_format, count

# Calculate the frequency of ratings on each day of the week
ratings_by_day = review_df.groupBy(date_format('rev_date', 'E').alias('day_of_week')) \
                           .agg(count('rev_stars').alias('rating_count')) \
                           .orderBy('day_of_week')


z.show(ratings_by_day)


%pyspark

# 3. Identify the top 5 merchants with the most 5-star ratings.
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Create SparkSession
spark = SparkSession.builder \
    .appName("Top 5 Merchants with Most 5-Star Ratings") \
    .enableHiveSupport() \
    .getOrCreate()

# Read the 'review' table from Hive
review_df = spark.sql("SELECT * FROM review")

# Read the 'business' table from Hive
business_df = spark.sql("SELECT * FROM business")

# Join review and business tables on the merchant ID
joined_df = review_df.join(business_df, review_df.rev_business_id == business_df.business_id)

# Filter for 5-star ratings
five_star_merchants = joined_df.filter(joined_df['stars'] == 5) \
                               .groupBy('business_id', 'name') \
                               .agg(count('*').alias('five_star_count')) \
                               .orderBy('five_star_count', ascending=False) \
                               .limit(5)

# Show the top 5 merchants with the most 5-star ratings
z.show(five_star_merchants)


