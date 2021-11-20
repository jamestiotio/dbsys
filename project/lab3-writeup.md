# SUTD 2021 50.043 SimpleDB Project Part 3 Writeup Report Document

> James Raphael Tiovalen / 1004555

## Implementation Description

For part 3 of the project, I also simply implemented most of the skeleton methods/functions and classes specified by the handout. I also followed the implementation guide as laid out by the exercises closely, and hence, there are very few deviations from the intended path.

Several design decisions that I have made include:

- Implementing a `LockManager` class, as specified by the handout, which is used by the `BufferPool` class.
- Implementing strict two-phase locking at page granularity.
- Implementing a NO STEAL/FORCE buffer management policy.
- Implementing a deadlock detection method by building and using a wait-for graph and then detecting cycles in said dependency graph.

Several challenges that I have faced include:

- Implementing certain methods in the `LockManager` class with the specific Java oddities/eccentricities. Some methods exhibit weird behavior such as deadlocking if the method is `synchronized` for some reason. Race conditions and various `ConcurrentModificationException`-related errors had to be taken care of as well during development.

I did not change the provided API at all. All the custom, additional classes are designed around the current API to support it, instead of subverting, modifying, or redirecting it.

There are no missing or incomplete elements of my code, assuming that all that is being considered is for part 3. Any other extra additional methods will be implemented in future parts, since this provides ease of debugging and separation between different functionalities as the project timeline progresses.
