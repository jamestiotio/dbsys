# SUTD ISTD/CSD 2021 50.043 Database and Big Data Systems Lab 10 Extra Exercises

> James Raphael Tiovalen / 1004555

## Exercise 1

1. Consider HDFS append operation, it doesn’t provide correctness! Give an example of how incorrect append could happen.

The HDFS cluster could have crashed, and during the recovery phase/process, the majority of the replicas of the data block have not been updated to the latest version.

Another case would be when writing to the data node, the data block being written, and the file consists multiple data blocks. When writing the block, after it has been fully written to node 2, for some reason node 3 dies. At this point, node 1 and 2 has fully written. When someone reads the file, the moment it navigates to the block, for some reason node 3 might come back alive/online, and at this point, node 3 has not been updated to have the same data as node 1 and 2. And the read could be exposed to inconsistencies if it prefers to read from node 3. Before the main node could detect the data inconsistency and kick in the "replication recovery process", a read could have been done, which might contain the incorrect un-appended data.

2. Why do you think it’s difficult to guarantee correctness for append?

To guarantee correctness for append, we need to block intermittent reads that could cause inconsistency. Blocking reads before writing would be expensive, since the reading process would take a much longer time.

## Exercise 2

In a Hadoop setup, the erasure coding configuration is RS(12,6).
1. What is the storage overhead? 6 / 12 = 50%.
2. What is the storage efficiency? 12 / 18 = 66.7%.
3. What is the fault tolerance level? We can afford to lose 6 out of 18 cells per codeword.
4. What is the dimension of the Generator Matrix? 18 by 12.

## Exercise 3

Suppose you are engaged by a client to setup a HDFS for data computation. Here are the user requirements:

- Existing active data size 5TB
- Estimated year-over-year data growth rate 80% 
- 50% buffer space for intermediate/temp data file
- HDFS replication factor 3


1. What is the projected disk space requirement for HDFS in 3 years time? 5 * 3 * (1.8)^3 * 1.5 = 131.22 TB
2. What is the projected disk space requirement for HDFS in 3 years time, if we replace RF=3 by RS(10,4)? 5 * ((10 + 4) / 10) * (1.8)^3 * 1.5 = 61.236 TB

