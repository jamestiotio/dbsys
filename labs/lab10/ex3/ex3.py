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
    return int(cols[0])


def supplierID(cols):
    return int(cols[1])


def price(cols):
    return float(cols[2])


# Projection
print(list(map(supplierID, priceTable)))


# Selection
print(list(filter(lambda x: price(x) > 100, priceTable)))


# Join
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


print(join(priceTable, stockTable, productID, productID))


# Map-Side Join
# It works when one table is small enough to fit into the RAM of a single datanode.

# Reduce-Side Join, e.g., use Hadoop with Hive
# It works even when both tables are too large to fit into the RAM of a single datenode.
# Hint: It is very similar to the distinct question we saw during the quiz 3. You need to union the two tables beforehand.

# t1: [("k1", 1), ("k2", 2)]
# t2: [("k1", "a")]
# tu: [("k1", 1), ("k2", 2), ("k1", "a")]
# Apply a groupByKey / shuffle to tu
# [("k1", [1, "a"]), ("k2", [2])]
# Apply filter
# [("k1", [1, "a"])] would be the result of our successful join
# If t1: [("k1", 1), ("k2", 2), ("k1", 2)], then we do groupByKey / shuffle first, then before/after applying the filter, need to distribute (Cartesian product)

# What about Spark?
# 1. Sort both tables by the keys to be joined, so that, the co-partitions (i.e., the partitions that hold same subset of the keys)
#    will be on the same worker node.
# 2. Merge join within each worker node.

# More exercises on Map-Reduce:
# a) How to implement other SQL query in MapReduce?
# b) Take some data from Kaggle and data transformation using RDD?