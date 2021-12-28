import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, length, count, udf, lit
from pyspark.sql.types import BooleanType

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

# This returns true if the review is not empty
# "[ [  ], [  ] ]" is actually considered as not empty, since such reviews might have been written in other languages/unencodable characters
@udf(returnType=BooleanType())
def is_not_empty_reviews(col1):
    if col1:
        return bool(eval(col1))
    else:
        return bool(col1)


df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .option("quotes", '"')
    .csv("hdfs://%s:9000/assignment2/part1/input/" % (hdfs_nn))
)

# Remove rows with no reviews or rating < 1.0
df = df.filter(is_not_empty_reviews(col("Reviews"))).filter(
    (col("Rating") >= 1.0) & (col("Rating").isNotNull())
)

# Sanity checks (we can do the opposite to ensure that only rows with no reviews or rating < 1.0 are removed)
# df.groupBy(col("Reviews")).agg(count("Reviews")).sort(desc("count(Reviews)")).show()
# print(df.count())
df.show()

df.write.csv("hdfs://%s:9000/assignment2/output/question1/" % (hdfs_nn), header=True)
