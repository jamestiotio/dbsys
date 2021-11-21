from mapreduce import *


def read_db(filename):
    db = []
    with open(filename, "r") as f:
        for l in f:
            db.append(l)
    f.close()
    return db


price_db = read_db("./data/price.csv")
priceTable = list(map(lambda ln: ln.strip("\n").split(","), price_db))

stock_db = read_db("./data/stock.csv")
stockTable = list(map(lambda ln: ln.strip("\n").split(","), stock_db))


def productID(cols):
    return cols[0]


def supplierID(cols):
    return cols[1]


def price(cols):
    return cols[2]


# Projection
print(list(map(supplierID, priceTable)))


# Selection
print(list(filter(lambda x: price(x) > 100, priceTable)))


# Join
def join(table1, table2, col_sel1, col_sel2):
    return filter(
        lambda l: len(l) > 0,
        flatMap(
            lambda r1: list(
                map(lambda r2: r1 + r2 if col_sel1(r1) == col_sel2(r2) else [], table2)
            ),
            table1,
        ),
    )


print(join(priceTable, stockTable, productID, productID))

# map side join
# it works when one table is small enough to fit into the RAM of a single datanode.

# reduce side join, e.g. use hadoop with Hive
# it works even when both tables are too large to fit into the RAM of a single datenode.
# Hint: it is very similar to the distinct question we saw during the quiz 3. you need to union the two tables before hand.

# t1: [("k1", 1), ("k2", 2)]
# t2: [("k1", "a")]
# tu: [("k1", 1), ("k2", 2), ("k1", "a")]


# What about Spark?
# 1. sort both tables by the keys to be joined, so that, the co-partitions (i.e. the partitions that hold same subset of the keys)
#    will be on the same workernode
# 2. merge join within each worker node.

# more exercise on map reduce
# a) how to implement other SQL query in mapreduce ?
# b) take some data from kaggle and data transformation using RDD?