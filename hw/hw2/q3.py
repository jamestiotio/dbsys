import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    split,
    regexp_replace,
    explode,
    count,
    col,
    arrays_zip,
    trim,
)

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

split_cols = split(df["Reviews"], "\\], \\[")

df = df.withColumn("review", split_cols.getItem(0)).withColumn(
    "date", split_cols.getItem(1)
)

# This should allow commas in review texts
df = df.withColumn("review", split(col("review"), "\\', \\'")).withColumn(
    "date", split(col("date"), "\\', \\'")
)

new_df = (
    df.withColumn("new", arrays_zip("review", "date"))
    .withColumn("new", explode("new"))
    .select("ID_TA", col("new.review").alias("review"), col("new.date").alias("date"))
)

new_df = new_df.withColumn("review", regexp_replace("review", "'", "")).withColumn(
    "date", regexp_replace("date", "'", "")
)

new_df = new_df.withColumn("review", regexp_replace("review", "\\[", "")).withColumn(
    "date", regexp_replace("date", "\\]", "")
)

# Remove useless leading and trailing whitespace characters
new_df = new_df.withColumn("review", trim(new_df.review)).withColumn(
    "date", trim(new_df.date)
)

# Sanity check
new_df.show()

new_df.write.csv(
    "hdfs://%s:9000/assignment2/output/question3/" % (hdfs_nn),
    header=True,
    emptyValue="",
)
