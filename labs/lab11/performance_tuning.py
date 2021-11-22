# Input: CSV with columns pid, sid, price

# Compare this (more costly/expensive using reduceByKey - 3 shuffle operations)
total_price = rdd.map(lambda t: (t[1], t[2])).reduceByKey(lambda x, y: x + y)
num_items = rdd.map(lambda t: (t[1], t[2])).reduceByKey(lambda x, y: 1 + y, 0)
total_price.join(num_items).map(lambda t: (t[0], t[1] / t[2]))

# With this (less costly using reduceByKey2 - 1 shuffle operation)
rdd.map(lambda t: (t[1], t[2])).groupByKey().map(
    lambda t: (t[0], sum(t[1]) / len(t[1]))
)
