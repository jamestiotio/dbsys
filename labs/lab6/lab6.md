# SUTD ISTD/CSD 2021 50.043 Database and Big Data Systems Lab 6 Exercises

> James Raphael Tiovalen / 1004555

## Exercise 1

1. Total cost = 50 + (50 _ 50) _ 100 = 250050 I/Os
2. Total cost = 50 + 100 \* 3 = 350 I/Os

## Exercise 2

1. Total number of passes = 1 + log<sub>3</sub>(27) = 1 + 3 = 4 passes
2. Total cost = 2 _ 108 _ 4 = 864 I/Os
3. Minimum number of buffer frames = 11

## Exercise 3

Total cost = 2 _ 186 _ (1 + 0) = 372 I/Os

Since number of buffer frames >> number of pages, we can simply do an in-memory sort (instead of executing an external sort).

## Exercise 4

1. Total cost = (20000 / 25) + (45000 / 30) + 2 _ (45000 / 30) _ (1 + 2) = 800 + 1500 + 9000 = 11300 I/Os
2. Total cost = 800 + (20000 \* k) I/Os. This is huge! The index join algorithm should then be only used when one of the relations is very small and there is an index on the join attribute(s) in the other (larger) relation.
