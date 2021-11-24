import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Ex2")
sc = SparkContext(conf=conf)


def ip_address(row):
    return (row[2], 1)


def splitline(line):
    return line.split(",")


def count(x, y):
    return x + y


def to_csv(line):
    return ",".join([str(line[0]), str(line[1])])


text_file = sc.textFile("hdfs://localhost:9000/input/")
output = text_file.map(splitline).map(ip_address).reduceByKey(count).map(to_csv)
output.saveAsTextFile("hdfs://localhost:9000/output/")
sc.stop()
