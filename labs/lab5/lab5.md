# SUTD ISTD/CSD 2021 50.043 Database Systems Lab 5 Exercises

> James Raphael Tiovalen / 1004555

## Exercise 1

1. Hit rate: 3 / 7. Final buffer state contains the items: D, E, F, G.
2. Hit rate: 1 / 7.
3. MRU would be better than LRU when there are repeating sequences of data access patterns (e.g., A B C D E A B C D E A B C D E...) or when the most recently accessed data is not going to be used/accessed any more any time soon (such as the case for bus/subway train arrivals, etc.).

## Exercise 2

After inserting 13, 15, 18, 25, 4:

![Lab 5 Exercise 2 B+ Tree Post-Insert](./lab5_ex2_b+tree_insert.png)

After deleting 4, 25, 18, 15, 13:

![Lab 5 Exercise 2 B+ Tree Post-Delete](./lab5_ex2_b+tree_delete.png)

As shown, insert operations followed by symmetrically mirrored delete operations in the reverse order may result in an overall significant change in the structure/shape of the resulting final B+ tree as compared to the original B+ tree (even if the actual set of elements/keys remaining in the leaf nodes are no different from the original B+ tree). In other words, the order of insertion and deletion matters, which might be undesirable in some circumstances.

## Exercise 3

![Lab 5 Exercise 3 Hash Table](./lab5_ex3_hash_table.png)
