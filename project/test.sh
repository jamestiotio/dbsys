#!/bin/bash
# Created by James Raphael Tiovalen (2021)

printf "Running all unit tests...\n\n"

for i in TupleDescTest TupleTest CatalogTest HeapPageIdTest RecordIdTest HeapPageReadTest HeapFileReadTest PredicateTest JoinPredicateTest FilterTest JoinTest IntegerAggregatorTest StringAggregatorTest AggregateTest HeapPageWriteTest HeapFileWriteTest BufferPoolWriteTest InsertTest LockingTest TransactionTest DeadlockTest BTreeFileReadTest BTreeFileInsertTest BTreeFileDeleteTest BTreeNextKeyLockingTest BTreeDeadlockTest
do
    for j in {1..10..1}
    do
        ant runtest -Dtest=$i
    done
done

printf "\nRunning all system tests...\n\n"

for i in ScanTest FilterTest JoinTest AggregateTest InsertTest DeleteTest EvictionTest AbortEvictionTest TransactionTest BTreeScanTest BTreeFileInsertTest BTreeFileDeleteTest BTreeTest
do
    for j in {1..10..1}
    do
        ant runsystest -Dtest=$i
    done
done
