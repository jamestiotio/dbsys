#!/bin/bash
# Created by James Raphael Tiovalen (2021)

printf "Running all unit tests...\n\n"

for i in TupleDescTest TupleTest CatalogTest HeapPageIdTest RecordIdTest HeapPageReadTest HeapFileReadTest PredicateTest JoinPredicateTest FilterTest JoinTest IntegerAggregatorTest StringAggregatorTest AggregateTest HeapPageWriteTest HeapFileWriteTest BufferPoolWriteTest InsertTest
do
    for j in {1..5..1}
    do
        ant runtest -Dtest=$i
    done
done

printf "\nRunning all system tests...\n\n"

for i in ScanTest FilterTest JoinTest AggregateTest InsertTest DeleteTest EvictionTest
do
    for j in {1..5..1}
    do
        ant runsystest -Dtest=$i
    done
done