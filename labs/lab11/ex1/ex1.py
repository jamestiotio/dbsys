# The `pyspark` shell command can be used to run an interactive shell for debugging purposes (preferably only on smaller datasets)
import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Ex1")
sc = SparkContext(conf=conf)


def foreach(record):
    cols = record.split(",")
    if len(cols) > 1:
        extra_col = str(cols[0].split(cols[1]))
        cols.append(extra_col)
    return ",".join(cols)


text_file = sc.textFile("hdfs://localhost:9000/input/")
output = text_file.map(foreach)
output.saveAsTextFile("hdfs://localhost:9000/output/")
sc.stop()
