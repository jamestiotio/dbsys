import sys 
from pyspark.sql import SparkSession
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW

df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .option("delimiter", ",")\
    .option("quotes", "\"")\
    .parquet("hdfs://%s:9000/assignment2/part2/input/" % (hdfs_nn))

df.show()

# df.write.parquet("hdfs://%s:9000/assignment2/output/question5/" % (hdfs_nn), header=True)
