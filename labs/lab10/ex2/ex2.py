from mapreduce import *


def read_db(filename):
    db = []
    with open(filename, "r") as f:
        for l in f:
            db.append(l)
    f.close()
    return db


test_db = read_db("./data/price.csv")


def preprocess(line):
    cols = line.split(",")
    supplier = cols[1].strip()
    price = cols[2].strip()
    return (int(supplier), float(price))


def aggregate(p):
    supplier = p[0]
    prices = p[1]
    # We have a shuffle step, so we can use this
    return (supplier, sum(prices) / len(prices))


m_out = map(preprocess, test_db)
# reduceByKey2 allows us to get len(prices) and other different types of aggregation without doing any kind of awkward/costly joining (which is the case if we use reduceByKey).
# The result should contain a list of suppliers,
# with the average sale price for all items by this supplier.
result = reduceByKey2(aggregate, m_out)


for supplier, avg_price in result:
    print(supplier, avg_price)

# For aggregation that can be defined using a commutative and associative binary operation, we should use reduceByKey2, e.g., calculating median.
