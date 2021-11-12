from mapreduce import *

def read_db(filename):
    db = []
    with open(filename, 'r') as f:
        for l in f:
            db.append(l)
    f.close()
    return db
            
test_db = read_db("./data/price.csv")

# TODO: FIXME
# the result should contain a list of suppliers, 
# with the average sale price for all items by this supplier.
result = []

for supplier,avg_price in result:
    print(supplier, avg_price)