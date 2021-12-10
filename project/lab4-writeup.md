# SUTD 2021 50.043 SimpleDB Project Part 4 Writeup Report Document

> James Raphael Tiovalen / 1004555

## Implementation Description

For part 4 of the project, I also simply implemented the specified skeleton methods/functions and classes. I also followed the implementation guide as laid out by the exercises closely, and hence, there are very few deviations from the intended path.

Several design decisions that I have made include:

- Changing all of the `HeapFile` casting occurrences in `BufferPool` to the more general `DbFile` interface instead so as to support both `HeapFile` and `BTreeFile`.
- Following all of the given comments of suggested implementations in each specified method closely.
- Using a `switch`-`case` statement clause separation in the `findLeafPage` method.

One main challenge that I have faced was:

- Getting to pass the `BTreeTest` system test consistently, even if the code passed all of the other unit tests and system tests. The sheer scale of the test can sometimes cause the test to timeout in 10 minutes, especially on repeated runs of the system test, even when there are seemingly no deadlocks at all (as indicated by smoothly and flawlessly passing the `TransactionTest` system test and the `BTreeDeadlockTest` unit test).

The original provided API of the project was not changed.

All of the requested elements/parts for part 4 are completed. The code should pass all of the required unit tests and system tests for the entire project so far, including the optional extra credit `BTreeTest` system test.

Part 4 of the project is surprisingly much easier/simpler as compared to part 3. Perhaps this is because part 4 is less "open-ended" as compared to part 3, due to the helpful step-by-step guiding comments in each method to be implemented.
