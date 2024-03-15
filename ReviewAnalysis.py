%pyspark
#1. Count the yearly number of reviews.

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count

# Create a SparkSession
spark = SparkSession.builder \
    .appName("YourApp") \
    .getOrCreate()

# Load the review dataset from Hive
df = spark.table('review')

# Calculate the yearly review count based on the 'rev_date' column
yearly_review_count = df.withColumn("year", year(df["rev_date"])) \
    .groupBy("year") \
    .agg(count("review_id").alias("review_count")) \
    .orderBy("year", ascending=False)  # Sort by year in descending order

# Show the results

z.show(yearly_review_count)



%pyspark

#2. Summarize the count of helpful, funny, and cool reviews each year.

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, sum

# Create SparkSession
spark = SparkSession.builder \
    .appName("Summarize Reviews by Year") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the Hive table 'review'
df = spark.table('review')

# Extract the year from the 'rev_date' column
df = df.withColumn("year", year(df["rev_date"]))

# Group by year and summarize the counts of helpful, funny, and cool reviews
summary_by_year = df.groupBy("year") \
    .agg(sum("rev_useful").alias("total_helpful_reviews"),
         sum("rev_funny").alias("total_funny_reviews"),
         sum("rev_cool").alias("total_cool_reviews")) \
    .orderBy("year", ascending=False)  # Sort by year in descending order

# Show the results
z.show(summary_by_year)

# Stop SparkSession
spark.stop()


%pyspark

# 3. Create a ranking of users based on their total reviews each year.
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, year, desc, dense_rank
from pyspark.sql import Window
from pyspark.sql import HiveContext

# Create a SparkSession
spark = SparkSession.builder \
    .appName("User_Reviews_Ranking") \
    .enableHiveSupport() \
    .getOrCreate()

# Create a HiveContext
hc = HiveContext(spark.sparkContext)

# Load the review dataset from Hive
df = hc.table('review')

# Extract the year from the 'rev_date' column
df = df.withColumn("year", year(df["rev_date"]))

# Group by user_id and year, and count the number of reviews
user_yearly_reviews = df.groupBy("rev_user_id", "year") \
    .agg(countDistinct("review_id").alias("total_reviews"))

# Define a window specification to rank users within each year based on total reviews
window_spec = Window.partitionBy("year").orderBy(desc("total_reviews"))

# Add a rank column based on total_reviews within each year
user_ranking = user_yearly_reviews.withColumn("rank", dense_rank().over(window_spec))

# Select the top reviewer (first row) for each year
top_reviewers_by_year = user_ranking.filter("rank == 1")

# Show the results
z.show(top_reviewers_by_year.orderBy("year"))


%pyspark

#4. Extract the top 5 common words from reviews.
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, regexp_replace
from pyspark.sql.functions import desc

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Top_5_Common_Words") \
    .enableHiveSupport() \
    .getOrCreate()

# Load the review dataset from Hive
df = spark.table('review')

# Tokenize the reviews into individual words
words_df = df.select(explode(split(lower(regexp_replace("rev_text", "[^a-zA-Z\\s]", "")), "\\s+")).alias("word"))

# Count the occurrences of each word
word_count_df = words_df.groupBy("word").count()

# Sort the words by their frequency in descending order and show the top 5
top_words_df = word_count_df.orderBy(desc("count")).limit(5)

# Show the top 5 most common words
z.show(top_words_df)



