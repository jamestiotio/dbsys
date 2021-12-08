import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .option("delimiter", ",")\
    .option("quotes", "\"")\
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))

# Drop rows with Price Range field or Rating field as null
df = df.na.drop(how="any", subset=["Price Range", "Rating"])

# Find the best restaurant in each city for each price range in terms of rating
max_df = df.groupBy(["Price Range", "City"]).agg(max("Rating")).withColumn("Rating", col("max(Rating)")).drop("max(Rating)")
# Find the worst restaurant in each city for each price range in terms of rating
min_df = df.groupBy(["Price Range", "City"]).agg(min("Rating")).withColumn("Rating", col("min(Rating)")).drop("min(Rating)")

union_df = max_df.union(min_df)

combined_df = union_df.join(df, on=["Price Range", "City", "Rating"], how="inner")

combined_df = combined_df.dropDuplicates(["Price Range", "City", "Rating"]).select("_c0", "Name", "City", "Cuisine Style", "Ranking", "Rating", "Price Range", "Number of Reviews", "Reviews", "URL_TA", "ID_TA").sort(col("City").asc(), col("Price Range").asc(), col("Rating").desc())

# Sanity check
combined_df.show()

combined_df.write.csv("hdfs://%s:9000/assignment2/output/question2/" % (hdfs_nn), header=True)
