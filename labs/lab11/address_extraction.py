import sys, re
from pyspark import SparkContext, SparkConf

pat = re.compile("ˆ(.*) ([A-Za-z]{2}) ([0-9]{5})(-[0-9]{4})?$")


def is_addr(line):
    return not (pat.match(line.strip()) is None)


def get_zip(addr):
    return re.search("([0-9]{5})(-[0-9]{4})?", addr).group(1)


def get_state(addr):
    return re.search("([A-Za-z]{2})", addr).group(1)


conf = SparkConf().setAppName("ETL (Extract) Example")
hdfs_nn = "127.0.0.1"
input = sc.textFile("hdfs://%s:9000/data/extract/" % hdfs_nn)
addrs = input.filter(is_addr)
addrs.persist()  # cache the intermediate results
zipcodes = addrs.map(get_zip).distinct()
states = addrs.map(get_state).distinct()
zipcodes.saveAsTextFile("hdfs://%s:9000/output/zipcodes" % hdfs_nn)
states.saveAsTextFile("hdfs://%s:9000/output/states" % hdfs_nn)