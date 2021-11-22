from mapreduce import *
import sys


infile = open(sys.argv[1], "r")
lines = []
for line in infile:
    lines.append(line.strip())

ws = flatMap(lambda line: line.split(" "), lines)
w1s = map(lambda w: (w, 1), ws)
res = reduceByKey(lambda x, y: x + y, w1s, 0)

with open(sys.argv[2], "w") as out:
    for w, c in res:
        out.write(w + "\t" + str(c) + "\n")
