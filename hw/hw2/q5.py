import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.sql.functions import from_json, col, explode, array, array_sort, count

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

json_schema = ArrayType(StructType([StructField("name", StringType(), False)]))

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))
)

df = df.drop("crew")

df = df.withColumn(
    "actor1", explode(from_json(col("cast"), json_schema).getField("name"))
)
df = df.withColumn(
    "actor2", explode(from_json(col("cast"), json_schema).getField("name"))
)

df = df.select("movie_id", "title", "actor1", "actor2")

# An actor/actress cannot co-cast with themselves... (philosophical identity question that's best not answered here hahaha!)
df = df.filter(col("actor1") != col("actor2"))

# Sort the cast pairing arrays to make it easier to compare and count (pure string comparisons are generally cheaper than array-of-strings comparisons)
# This is super hacky and scuffed, but it works and it doesn't require any more memory than the default Hadoop and Spark settings!
df = df.withColumn("cast_pair", array(col("actor1"), col("actor2"))).withColumn(
    "cast_pair", array_sort(col("cast_pair")).cast("string")
)

# Remove duplicate cast pair repetitions within the same movies to avoid overcounting
df = df.dropDuplicates(["movie_id", "title", "cast_pair"]).sort(col("cast_pair").asc())

df_counts = (
    df.groupBy("cast_pair")
    .agg(count("*").alias("count"))
    .filter(col("count") >= 2)
    .sort(col("cast_pair").asc())
)

final_df = (
    df_counts.join(df, ["cast_pair"], "inner")
    .drop("cast_pair", "count")
    .sort(col("movie_id").asc())
)

# Sanity check
final_df.show()

final_df.write.option("header", True).mode("overwrite").parquet(
    "hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn)
)
