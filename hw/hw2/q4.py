import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_replace, explode, count, col, trim

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

df = df.withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\[", ""))
df = df.withColumn("Cuisine Style", regexp_replace("Cuisine Style", "\\]", ""))
df = df.withColumn("Cuisine Style", split(col("Cuisine Style"), ", "))

df_exploded = (
    df.withColumn("Cuisine Style", explode("Cuisine Style"))
    .withColumn("Cuisine Style", regexp_replace("Cuisine Style", "'", ""))
    .withColumn("Cuisine Style", trim(col("Cuisine Style")))
)

new_df = df_exploded.select("City", "Cuisine Style")
new_df = (
    new_df.groupBy("City", "Cuisine Style")
    .agg(count("*").alias("count"))
    .sort(col("City").asc(), col("count").desc())
)
new_df = new_df.select(
    col("City").alias("City"),
    col("Cuisine Style").alias("Cuisine"),
    col("count").alias("count"),
)

# Sanity check
new_df.show()

new_df.write.csv(
    "hdfs://%s:9000/assignment2/output/question4/" % (hdfs_nn), header=True
)
