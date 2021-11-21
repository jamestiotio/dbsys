def mapper(key, text, writer):
    cols = text.split(",")
    supplier = cols[1]
    price = cols[2]
    writer.emit(supplier, price)


def reducer(supplier, prices, writer):
    prices = list(map(float, (prices)))
    avg = str(sum(prices) / len(prices))
    writer.emit(supplier, avg)