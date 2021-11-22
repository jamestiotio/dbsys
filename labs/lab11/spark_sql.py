"""
This code is equivalent to:

select dept, avg(age) from input
group by dept;
"""

import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Wordcount Application")
sc = SparkContext(conf=conf)

# Assuming input.csv has two columns, dept and age
data = sc.textFile("input.csv")
rdd1 = data.map(lambda x: x.split(","))
rdd2 = rdd1.map(lambda x: (x[0], (x[1], 1)))
rdd3 = rdd2.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
res = rdd3.map(lambda x: (x[0], x[1][0] / x[1][1]))
print(list(res.collect()))
