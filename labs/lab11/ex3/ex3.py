import sys
from pyspark import SparkContext, SparkConf
from mapreduce import *

conf = SparkConf().setAppName("Ex3")
sc = SparkContext(conf=conf)

text_file = sc.textFile("hdfs://localhost:9000/input/")
cache_input = text_file.map(lambda record: record.split(",")).cache()
aggregated = cache_input.map(lambda cols: (cols[2], 1)).reduceByKey(lambda a, b: a + b)
left = cache_input.map(lambda cols: (cols[2], cols))
output = left.join(aggregated).map(lambda p: ",".join(map(str, p[1][0] + [p[1][1]])))
output.saveAsTextFile("hdfs://localhost:9000/output/")
sc.stop()

"""
# Map-Side Join Implementation

def get_ip_address(row):
    return (row[2], 1)


def splitline(line):
    return line.split(",")


def count(x, y):
    return x + y


def to_csv(line):
    return ",".join([str(line[0]), str(line[1])])


def join(table1, table2, col_sel1, col_sel2):
    return filter(
        # Extra checking
        lambda l: len(l) > 0,
        flatMap(
            lambda r1: list(
                # Append the two rows together if they have matching values in the specified columns
                map(lambda r2: r1 + r2 if col_sel1(r1) == col_sel2(r2) else [], table2)
            ),
            table1,
        ),
    )


text_file = sc.textFile("hdfs://localhost:9000/input/")
first_table = text_file.map(splitline)
second_table = text_file.map(splitline).map(get_ip_address).reduceByKey(count)
output = join(first_table, second_table, lambda x: x[2], lambda y: y[0]).map(to_csv)
output.saveAsTextFile("hdfs://localhost:9000/output/")
sc.stop()
"""

"""
# Reduce-Side Join Implementation

def foreach_group(p):
    lefts = []
    rights = [] # Though there should be only one right
    for i in p[1]:
        if isinstance(i, list) and len(i) == 4:
            lefts.append(i)
        else:
            rights.append(i)
    cartesian = [list(a) + [str(b)] for a in lefts for b in rights]
    return cartesian

text_file = sc.textFile("hdfs://localhost:9000/input/")
cache_input = text_file.map(lambda record: record.split(",")).cache()
aggregated = cache_input.map(lambda cols: (cols[2], 1)).reduceByKey(lambda a, b: a + b)
left = cache_input.map(lambda cols: (cols[2], cols))
output = left.union(aggregated).groupByKey().flatMap(foreach_group).map(lambda l:",".join(l))
output.saveAsTextFile("hdfs://localhost:9000/output/")
sc.stop()
"""
