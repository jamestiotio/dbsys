# SUTD ISTD/CSD 2021 50.043 Database and Big Data Systems Lab 9 Exercises

> James Raphael Tiovalen / 1004555

## Exercise 1

1. Log content:
   1. <T1, BEGIN>
   2. <T1, A, 0>
   3. <T1, B, 0>
   4. <T1, COMMIT>
   5. <T2, BEGIN>
   6. <T2, A, 10>
   7. <T2, C, 0>
   8. <T2, A, 40>
   9. <T2, COMMIT>
   10. <T3, BEGIN>
   11. <T3, B, 20>
2. Values of A, B, and C respectively are:
   - 50, 20, 30
   - 10, 20, 0

## Exercise 2

1. Log content:

   1. <T1, BEGIN>
   2. <T1, A, 10>
   3. <T1, B, 20>
   4. <T1, COMMIT>
   5. <T2, BEGIN>
   6. <T2, A, 40>
   7. <T2, C, 30>
   8. <T2, A, 50>
   9. <T2, COMMIT>
   10. <T3, BEGIN>
   11. <T3, B, 75>

2. Values of A, B, and C respectively are:

   - A can be either 0, 10, 40, or 50; B can be either 0 or 20; C can be either 0 or 30
   - 0, 0, 0

   This will not affect the eventual correctness of the values of A, B, and C after the system recovery, as the database state will eventually become the state expected after all committed transactions are executed. To indicate whether the values have been written to disk or not so as to not waste effort in redoing anything, we can possibly add or introduce another type/kind of log of this format: <Ti, END>.

3. Values of A, B, and C respectively are:

   - 50, 20, 30
   - 10, 20, 0

## Exercise 3

Values of A, B, and C respectively are:

- A can be either 0 or 10; B can be either 0 or 20; C is 0
- 10, 20, 0

## Exercise 4

Find the last finished/completed checkpoint (specified by line 6). Since T2 and T3 are committed after the checkpoint was completed, we start redoing from line 3 onwards:

- Set B = 10 (line 5)
- Set C = 15 (line 7)
- Set D = 20 (line 9)

## Exercise 5

Yes, the specified execution order is conflict serializable. One possible conflict-equivalent order of transactions is: T3 -> T1 -> T2.

## Exercise 6

No, the specified execution order is not conflict serializable. The R(A) operation in T2 conflicts with the W(A) operation in T1, while the R(B) operation in T1 conflicts with the W(B) operation in T2. If the order of transactions contain T1 -> T2, it violates the given constrained order of the R(A) operation in T2 being executed before the W(A) operation in T1, whereas if the order of transactions contain T2 -> T1, it violates the given constrained order of the R(B) operation in T1 being executed before the W(B) operation in T2. Hence, we cannot derive a conflict-equivalent serial order of transactions from the execution order given in the schedule without violating the original conflict order. Therefore, it is not conflict serializable.

## Exercise 7

Yes, the schedule is possible under 2PL. T1 would need to acquire the lock for A before its R(A) operation. Since T1 would need to release the lock for A after its W(A) operation to be used by T2, T1 would also need to acquire the lock for B at any time before that. After T1's W(A) operation, T1 would release the lock for A, which would then be acquired by T2. Just immediately after T1's W(B) operation, T1 would release the lock for B to be used by T2. T2 can then release the lock for B whenever after its W(B) operation and release the lock for A after its W(A) operation.
