from mapreduce import *


def read_db(filename):
    db = []
    with open(filename, "r") as f:
        for l in f:
            db.append(l)
    f.close()
    return db


test_db = read_db("./data/price.csv")
priceTable = map(lambda ln: ln.strip().split(","), test_db)


def supplierID(cols):
    return cols[1]


def price(cols):
    return cols[2]


# Projection
print(list(map(supplierID, priceTable)))


# Selection
print(list(filter(lambda x: price(x) > 100, priceTable)))

# Join