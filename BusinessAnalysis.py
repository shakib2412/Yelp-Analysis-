%pyspark

#1. Identify the most common merchants in the United States (Top 20).

from pyspark import HiveContext
from pyspark.sql.functions import count, col

hc = HiveContext(sc)
df = hc.table('business')

result = df.groupBy('name')\
    .agg(count('name').alias('cnt'))\
    .orderBy(col('cnt').desc())\
    .limit(20)

z.show(result)


%pyspark

#2. Find the top 10 cities with the most merchants in the United States.

from pyspark.sql import HiveContext
from pyspark.sql.functions import count, col
sc = spark.sparkContext
hc = HiveContext(sc)
df = hc.table('business')

# Perform aggregation by city
result = df.groupBy('city') \
            .agg(count('name').alias('merchant_count')) \
            .orderBy(col('merchant_count').desc()) \
            .limit(10)

z.show(result)


%pyspark

#3. Identify the top 5 states with the most merchants in the United States.

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
sc = spark.sparkContext
hc = HiveContext(sc)
df = hc.table('business')
# Perform aggregation by state
result = df.groupBy('state') \
                      .agg(count('name').alias('merchant_count')) \
                      .orderBy(col('merchant_count').desc()) \
                      .limit(10)

z.show(result)


%pyspark

#4. Find the most common merchants in the United States and display their average ratings (Top 20).

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, avg
sc = spark.sparkContext
hc = HiveContext(sc)
df = hc.table('business')
result = df.groupBy('name') \
                      .agg(count('name').alias('merchant_count'), avg('stars').alias('avg_rating')) \
                      .orderBy(col('merchant_count').desc()) \
                      .limit(20)

z.show(result)


%pyspark

#5. Analyze and list the top 10 cities with the highest average ratings.

from pyspark import HiveContext
from pyspark.sql.functions import count, col, countDistinct

hc = HiveContext(sc)
df = hc.table('business')
result = df.filter(df['is_open'] == 1)\
    .groupBy('city')\
    .agg((count('review_count') / countDistinct('name')).alias('avg_review_count_per_merchant'))\
    .orderBy(col('avg_review_count_per_merchant').desc())\
    .limit(15)

z.show(result)


%pyspark

#6. Count the number of categories.

from pyspark import HiveContext
from pyspark.sql.functions import explode, split, count, desc

hc = HiveContext(sc)
df = hc.table('business')

category_counts = df.select(explode(split('categories', ',')).alias('category')) \
    .groupBy('category') \
    .agg(count('*').alias('count')) \
    .orderBy(desc('count')) \
    .limit(20)

z.show(category_counts)


%pyspark

#7. Identify and list the top 10 categories with the highest frequency.

from pyspark import HiveContext
from pyspark.sql.functions import count, col, split, explode

hc = HiveContext(sc)
df = hc.table('business')

result = df.select(explode(split('categories', ', ')).alias('category'))\
    .groupBy('category') \
    .agg(count('category').alias('cnt'))\
    .orderBy(col('cnt').desc())\
    .limit(20)

z.show(result)


%pyspark

#8. List the top 20 merchants with the most five-star reviews.

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
sc = spark.sparkContext
hc = HiveContext(sc)
df = hc.table('business')


# Filter businesses with five-star reviews
five_star_businesses = df.filter(df.stars == 5)

result = five_star_businesses.groupBy('name') \
                              .agg(sum('review_count').alias('five_star_reviews')) \
                              .orderBy(col('five_star_reviews').desc()) \
                              .limit(20)

z.show(result)


%pyspark

#9. Summarize the types and quantities of restaurants for different cuisines (Chinese, American, Mexican).

from pyspark.sql import HiveContext
from pyspark.sql.functions import col, when

# Create HiveContext
hc = HiveContext(sc)

# Load the 'business' table
business_df = hc.table('business')

# Define the cuisine types
cuisines = ['Chinese', 'American', 'Mexican']

# Create columns for each cuisine type indicating whether the business belongs to that cuisine
business_with_cuisines = business_df.withColumn('Chinese', when(col('categories').contains('Chinese'), 1).otherwise(0)) \
                                   .withColumn('American', when(col('categories').contains('American'), 1).otherwise(0)) \
                                   .withColumn('Mexican', when(col('categories').contains('Mexican'), 1).otherwise(0))

# Summarize the types and quantities of restaurants for different cuisines
cuisine_summary = business_with_cuisines.selectExpr('sum(Chinese) as Chinese',
                                                    'sum(American) as American',
                                                    'sum(Mexican) as Mexican')

z.show(cuisine_summary)


%pyspark

#10. Analyze the review counts for restaurants of different cuisines.

from pyspark import HiveContext
from pyspark.sql.functions import col

hc = HiveContext(sc)
df = hc.table('business')

# Filter restaurants with specified cuisines
cuisine_types = ['Chinese', 'American', 'Mexican']
restaurant_review_counts = df.select('categories', 'review_count') \
    .filter(df['categories'].like('%Chinese%') | df['categories'].like('%American%') | df['categories'].like('%Mexican%')) \
    .orderBy(col('review_count').desc()) \
    .limit(20)

z.show(restaurant_review_counts)



%pyspark

#12

#Determine the average latitude and longitude coordinates for businesses in each state:
from pyspark import HiveContext
from pyspark.sql.functions import count, col
hc = HiveContext(sc)
df = hc.table('business')
result = df.groupBy('state')\
    .agg(avg('latitude').alias('avg_latitude'), avg('longitude').alias('avg_longitude'))

z.show(result)


%pyspark

#11. Explore the distribution of ratings for restaurants of different cuisines.

from pyspark import HiveContext
from pyspark.sql.functions import col

hc = HiveContext(sc)
df = hc.table('business')

# Filter restaurants with specified cuisines
cuisine_types = ['Chinese', 'American', 'Mexican']
restaurant_ratings = df.select('categories', 'stars') \
    .filter(df['categories'].like('%Chinese%') | df['categories'].like('%American%') | df['categories'].like('%Mexican%')) \
    .orderBy(col('stars').desc()) \
    .limit(20)

z.show(restaurant_ratings)


%pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count

# Create SparkSession
spark = SparkSession.builder \
    .appName("Top 5 Cities with Most Restaurants") \
    .getOrCreate()

# Read the 'business' table from Hive
business_df = spark.sql("SELECT * FROM business")

# Filter businesses that are categorized as restaurants
restaurant_df = business_df.filter(col("categories").like("%Restaurant%"))

# Count the number of restaurants in each city
restaurant_count_by_city = restaurant_df.groupBy("city").agg(count("*").alias("restaurant_count"))

# Find the top 5 cities with the most restaurants
top_5_cities_with_restaurants = restaurant_count_by_city.orderBy(col("restaurant_count").desc()).limit(5)

# Show the results
z.show(top_5_cities_with_restaurants)



%pyspark
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Merchants in Nashville and Tucson") \
    .enableHiveSupport() \
    .getOrCreate()

# Read the 'business' table from Hive
business_df = spark.table('business')

# Filter businesses in Nashville and Tucson
nashville_tucson_merchants = business_df.filter((business_df['city'] == 'Nashville') | (business_df['city'] == 'Tucson'))

# Select only the names of the merchants
merchant_names = nashville_tucson_merchants.select('name')

# Show the results
z.show(merchant_names)



