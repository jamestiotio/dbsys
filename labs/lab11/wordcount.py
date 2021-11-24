import sys
from pyspark import SparkContext, SparkConf, SparkSession

"""
sparkSession = SparkSession.builder.getOrCreate()
sc = sparkSession.SparkContext
"""

conf = SparkConf().setAppName("Wordcount Application")
sc = SparkContext(conf=conf)

name_node = "localhost"  # fixme

text_file = sc.textFile("hdfs://{}:9000/input/".format(name_node))
counts = (
    text_file.flatMap(lambda line: line.split(" "))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
)
counts.saveAsTextFile("hdfs://{}:9000/output/".format(name_node))
sc.stop()

"""
# Reduce the number of data to be shuffled
words = sc.textFile(...).map(lambda l: l.strip().split())
wordPairsRDD = words.map(lambda w: (w, 1))
wordCountsWithReduce = wordPairsRDD.reduceByKey(lambda x,y: x+y).collect()
wordCountsWithGroup = wordPairsRDD.groupByKey().map(lambda t: (t[0], sum(t[1]))).collect()
"""