# SUTD ISTD/CSD 2021 50.043 Database Systems Lab 8 Exercises

> James Raphael Tiovalen / 1004555

## Exercise 1

Only expression 1 is equivalent to the specified algebra expression.

## Exercise 2

1. Assuming uniformity, number of requested tuples = (1 / V(A, X)) * N(X) = 200 / 100 = 2.
2. Assuming uniformity, number of requested tuples in X ⋈ Y = N(Y) * N(X) / V(B, X) = 1000 * 200 / 20 = 10000.
3. Assuming independence between attributes A and B, and since relation Y does not have attribute A, V(A, X ⋈ Y) = V(A, X) = 100.

## Exercise 3

0. All the names of the sailors with rating >= 5 and who reserved the boat with bid = 100.
1. Cost = 500 + (500 * 80) * 1000 = 40000500 I/Os.
2. Cost = 500 + 0.6 * (500 * 80) * 1000 = 24000500 I/Os.
3. Cost = 500 + 0.6 * (500 * 80) * 1000 = 24000500 I/Os.
4. Cost = 1000 + 0.01 * (1000 * 100) * 500 = 501000 I/Os.
5. Cost = 500 + 1000 + (0.01 * 1000) + 0.6 * (500 * 80) * (0.01 * 1000) = 241510 I/Os.
6. Cost = 500 + 1000 + (0.01 * 1000) + (0.01 * 1000) * ceil(500 * 80 * 0.6 /(80 * (5-2))) = 2510 I/Os.
7. Cost = ...
8. Cost = ...

