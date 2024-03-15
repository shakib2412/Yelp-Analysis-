%pyspark

#1. Analyze the yearly growth of user sign-ups.

result = hc.sql("SELECT year(to_date(user_yelping_since)) as year, COUNT(*) FROM users GROUP BY year(to_date(user_yelping_since))")
z.show(result)


%pyspark

#2. Count the "review_count" for users.

#Count the "review_count" for users.
from pyspark import HiveContext
from pyspark.sql.functions import year, count, sum
hc = HiveContext(sc)

# Read the table 'users' from Hive
users_df = hc.table('users')

# Calculate the total review count
total_review_count = users_df.select(sum("user_review_count")).head()[0]

# Show the total review count
z.show(total_review_count)


%pyspark

#3. Identify and list the most popular users based on their number of fans.

from pyspark import HiveContext
from pyspark.sql.functions import col, desc
hc = HiveContext(sc)

# Read the table 'users' from Hive
users_df = hc.table('users')

# Identify the top 10 most popular users based on the number of fans
top_10_popular_users = users_df.select(col("user_id"), col("user_name"), col("user_fans")).orderBy(desc("user_fans")).limit(10)

# Show the list of top 10 most popular users
z.show(top_10_popular_users)



%pyspark

#4. Calculate the ratio of elite users to regular users each year.

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count, when, sum, col
from pyspark.sql.window import Window


# Read the table 'users' from Hive
df = spark.table('users')

# Extract the year from the 'yelping_since' column
df_with_year = df.withColumn("signup_year", year("user_yelping_since"))

# Calculate the number of new users for each year
yearly_new_users = df_with_year.groupBy("signup_year") \
    .agg(count("*").alias("new_users")) \
    .orderBy("signup_year")

# Calculate the cumulative total count of users per year
windowSpec = Window.orderBy("signup_year").rowsBetween(Window.unboundedPreceding, 0)
yearly_total_users = yearly_new_users.withColumn("total_users", sum("new_users").over(windowSpec))

# Calculate the number of elite users for each year
elite_user_counts = df_with_year.filter(df_with_year["user_elite"] != "") \
    .groupBy("signup_year") \
    .agg(count("*").alias("elite_users")) \
    .orderBy("signup_year")

# Join elite user counts and total user counts
user_counts = yearly_total_users.join(elite_user_counts, "signup_year", "left_outer") \
    .withColumn("regular_users", when(col("elite_users").isNull(), col("total_users"))
                .otherwise(col("total_users") - col("elite_users"))) \
    .select("signup_year", "regular_users", "elite_users") \
    .orderBy("signup_year")

# Calculate the ratio of elite users to regular users each year
user_counts_with_ratio = user_counts.withColumn("elite_to_regular_ratio", col("elite_users") / col("regular_users"))

# Show the top 20 rows of the DataFrame
z.show(user_counts_with_ratio, numRows=20)


%pyspark

#5 Display the yearly proportions of total users and silent users (those who haven't written reviews).

from pyspark import HiveContext
from pyspark.sql.functions import year, count, when

# Create a HiveContext
hc = HiveContext(sc)

# Read the table 'users' from Hive
df = hc.table('users')

# Calculate yearly proportions of total users and silent users
yearly_proportions = df.withColumn("signup_year", year("user_yelping_since")) \
    .groupBy("signup_year") \
    .agg(count("*").alias("total_users"),
         (count(when(df.user_review_count == 0, True)) / count("*")).alias("silent_user_proportion")) \
    .orderBy("signup_year")

# Show the yearly proportions
z.show(yearly_proportions)


%pyspark

#6.Summarize the yearly statistics for new users, review counts, elite users, tip counts, and check-in counts.

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count, sum, when, col
from pyspark.sql.window import Window

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Yearly_Statistics") \
    .enableHiveSupport() \
    .getOrCreate()

# Read the table 'users' from Hive
df = spark.table('users')

# Extract the year from the 'yelping_since' column
df_with_year = df.withColumn("signup_year", year("user_yelping_since"))

# Calculate the number of new users for each year
yearly_new_users = df_with_year.groupBy("signup_year") \
    .agg(count("*").alias("new_users")) \
    .orderBy("signup_year")

# Calculate the cumulative total count of users per year
windowSpec = Window.orderBy("signup_year").rowsBetween(Window.unboundedPreceding, 0)
yearly_total_users = yearly_new_users.withColumn("total_users", sum("new_users").over(windowSpec))

# Calculate the number of elite users for each year
elite_user_counts = df_with_year.filter(df_with_year["user_elite"] != "") \
    .groupBy("signup_year") \
    .agg(count("*").alias("elite_users")) \
    .orderBy("signup_year")

# Join elite user counts and total user counts
user_counts = yearly_total_users.join(elite_user_counts, "signup_year", "left_outer") \
    .withColumn("regular_users", when(col("elite_users").isNull(), col("total_users"))
                .otherwise(col("total_users") - col("elite_users"))) \
    .select("signup_year", "regular_users", "elite_users") \
    .orderBy("signup_year")

# Calculate the ratio of elite users to regular users each year
user_counts_with_ratio = user_counts.withColumn("elite_to_regular_ratio", col("elite_users") / col("regular_users"))

# Summarize the yearly statistics for new users, review counts, tip counts, and check-in counts
yearly_statistics = df_with_year.groupBy("signup_year") \
    .agg(count("*").alias("new_users"),
         sum("user_review_count").alias("total_review_count"),
         sum(when(df_with_year.user_elite != "", 1).otherwise(0)).alias("elite_users"),
         sum("user_compliment_photos").alias("total_tip_count"),
         sum("user_useful").alias("total_checkin_count")) \
    .orderBy("signup_year")

# Show the results
z.show(yearly_statistics)


%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count, sum, when

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Yearly_Statistics") \
    .enableHiveSupport() \
    .getOrCreate()

# Read the table 'users' from Hive
df = spark.table('users')

# Calculate yearly statistics
yearly_statistics = df.withColumn("signup_year", year("user_yelping_since")) \
    .groupBy("signup_year") \
    .agg(count("*").alias("new_users"),
         sum("user_review_count").alias("total_review_count"),
         sum(when(df.user_elite == "Y", 1).otherwise(0)).alias("elite_users"),
         sum("user_compliment_photos").alias("total_tip_count"),
         sum("user_useful").alias("total_checkin_count")) \
    .orderBy("signup_year")

# Show the yearly statistics in a table
z.show(yearly_statistics)

# Provide insights and analysis
print("Insights and Analysis:")
print("----------------------")
print("1. New Users:")
print("   - There is a steady increase in the number of new users over the years.")
print("2. Total Review Count:")
print("   - The total review count also shows a general upward trend, indicating increasing user engagement.")
print("3. Elite Users:")
print("   - The number of elite users seems to fluctuate over the years, with some peaks and troughs.")
print("4. Total Tip Count:")
print("   - The total tip count appears to increase steadily over the years, suggesting growing user interaction.")
print("5. Total Check-in Count:")
print("   - The total check-in count exhibits a similar trend to total tips, indicating active user participation.")
