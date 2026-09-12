# Appendix A: Concurrency II

*The detailed tutorial companion to Chapter 13.*

## Core Idea
The mechanics behind Chapter 13's recommendations: why a single `++` has thousands of execution paths, what actually causes deadlock (and the four ways to break it), when nonblocking beats locking, and how to test code that fails once in a million runs.

## Framework: The Four Conditions for Deadlock

**All four must hold. Break any one and deadlock is impossible.**

| Condition | Definition |
|---|---|
| **Mutual Exclusion** | Multiple threads need the same resources, which **cannot be used simultaneously** and are **limited in number** — database connections, a file open for write, a record lock, a semaphore |
| **Lock & Wait** | Once a thread acquires a resource, it does not release it until it has acquired *all* the others it needs and finished its work |
| **No Preemption** | One thread cannot take resources from another; the only way to get one is for the holder to release it |
| **Circular Wait** | *"The deadly embrace."* T1 holds R1 and needs R2; T2 holds R2 and needs R1 |

### The Canonical Deadlock Scenario

A web app with two finite pools (10 each): database connections and MQ connections to a master repository. Two operations acquire them in **opposite order**:
- **Create** — acquire master repository, *then* database
- **Update** — acquire database, *then* master repository

1. Ten users call `create`; all ten database connections are taken, each thread interrupted before acquiring the master repository.
2. Ten users call `update`; all ten master repository connections are taken, each interrupted before acquiring a database connection.
3. The ten "create" threads wait for the master repository; the ten "update" threads wait for the database.
4. **Deadlock. The system never recovers.**

*"This might sound like an unlikely situation, but who wants a system that freezes solid every other week?… This is the kind of problem that happens in the field, then takes weeks to solve."*

**The trap that follows**: adding debug statements changes the timing enough that the deadlock reappears somewhere else months later. *"The debugging code 'fixes' the problem so it remains in the system."*

### Reference Table: Breaking Each Condition

| Strategy | How | Cost |
|---|---|---|
| **Break Mutual Exclusion** | Use simultaneously-usable resources (`AtomicInteger`); increase resource count to ≥ competing threads; check all resources are free before seizing any | Most resources are genuinely limited, and *"it's not uncommon for the identity of the second resource to be predicated on the results of operating on the first"* |
| **Break Lock & Wait** | **Refuse to wait.** Check each resource before seizing; if any is busy, release everything and start over | **Starvation** (a thread with a rare combination never acquires all it needs → low CPU utilization) and **livelock** (threads in lockstep acquiring and releasing forever → high, useless CPU utilization). *"As inefficient as this strategy sounds, it's better than nothing… it can almost always be implemented if all else fails"* |
| **Break Preemption** | Let threads take resources from others via a request mechanism: when a resource is busy, ask the owner to release it; if the owner is itself waiting, it releases everything and restarts | Fewer restarts than the above, but *"managing all those requests can be tricky"* |
| **Break Circular Wait** | **The most common approach.** Agree on a **global ordering of resources** and have all threads allocate in that order. *"For most systems it requires no more than a simple convention agreed to by all parties"* | Acquisition order may not match use order, so resources stay locked longer than necessary. And if the second resource's ID comes from operating on the first, **ordering is not feasible** |

*"So there are many ways to avoid deadlock. Some lead to starvation, whereas others make heavy use of the CPU and reduce responsiveness. **TANSTAAFL!**"* (There Ain't No Such Thing As A Free Lunch.)

> *"Isolating the thread-related part of your solution to allow for tuning and experimentation is a powerful way to gain the insights needed to determine the best strategies."*

## Framework: Nonblocking Solutions (CAS)

```java
// Blocking — synchronized always acquires a lock, even when no other thread
// is trying to update the same value. Intrinsic locks have improved across
// versions but "they are still costly."
public class ObjectWithValue {
    private int value;
    public void synchronized incrementValue() { ++value; }
    public int getValue() { return value; }
}

// Nonblocking — Java 5's AtomicBoolean, AtomicInteger, AtomicReference (and more).
public class ObjectWithValue {
    private AtomicInteger value = new AtomicInteger(0);
    public void incrementValue() { value.incrementAndGet(); }
    public int getValue() { return value.get(); }
}
```

**The performance claim, stated precisely**: *"the performance of this class will nearly always beat the previous version. In some cases it will only be slightly faster, but the cases where it will be slower are virtually nonexistent."*

**Why** — modern processors provide **Compare and Swap (CAS)**:

| Approach | Database analogy | Assumption |
|---|---|---|
| `synchronized` | **Pessimistic locking** | Contention is likely; always pay for the lock |
| CAS / `Atomic*` | **Optimistic locking** | Threads *generally* don't modify the same value often enough to collide — detect collisions and retry |

*"This detection is almost always less costly than acquiring a lock, even in moderate to high contention situations."*

The CAS operation is atomic; logically:

```java
int variableBeingSet;

void simulateNonBlockingSet(int newValue) {
    int currentValue;
    do {
        currentValue = variableBeingSet;
    } while (currentValue != compareAndSwap(currentValue, newValue));
}

int synchronized compareAndSwap(int currentValue, int newValue) {
    if (variableBeingSet == currentValue) {
        variableBeingSet = newValue;
        return currentValue;
    }
    return variableBeingSet;
}
```

CAS verifies the variable still holds its last known value; if another thread got in the way, the change is not made, the attempt sees this, and **retries**.

## Framework: The Executor Framework

*"If you are creating threads and are not using a thread pool or are using a hand-written one, you should consider using the `Executor`. It will make your code cleaner, easier to follow, and smaller."*

What it provides: thread pooling, automatic resizing, thread recreation, and **futures**. Works with `Runnable` and with **`Callable`** — *"a `Callable` looks like a `Runnable`, but it can return a result, which is a common need in multithreaded solutions."*

```java
public String processRequest(String message) throws Exception {
    Callable<String> makeExternalCall = new Callable<String>() {
        public String call() throws Exception {
            String result = "";
            // make external request
            return result;
        }
    };

    Future<String> result = executorService.submit(makeExternalCall);
    String partialResult = doSomeLocalProcessing();   // proceed while it runs
    return result.get() + partialResult;              // blocks until the future completes
}
```

## Key Concepts
- **Non-thread-safe classes** — a named list including `SimpleDateFormat`; do not assume library classes are safe.
- **Dependencies Between Methods Can Break Concurrent Code** — the appendix develops Ch 13's three remedies: **Tolerate the Failure**, **Client-Based Locking**, **Server-Based Locking**.
- **Increasing Throughput** — single-thread vs. multithread calculation of throughput, showing why enlarging critical sections to reduce their count backfires.
- **Possible Paths of Execution** — the derivation behind Ch 13's 12,870 (`int`) and 2,704,156 (`long`) figures, including how the JIT compiler and the Java memory model's notion of atomicity determine what counts as a path.

## Worked Example: Testing `ClassWithThreadingProblem`

```java
public class ClassWithThreadingProblem {
    int nextId;

    public int takeNextId() {
        return nextId++;
    }
}
```

*"How can we write a test to demonstrate the following code is broken?"* — this is the appendix's central testing exercise, and it is genuinely hard: the failing interleaving is one of thousands, so a naive test passes forever while the code is wrong. The chapter develops the test and then covers **Tool Support for Testing Thread-Based Code** (IBM's ConTest and similar), which systematically perturbs orderings rather than hoping to hit one by luck.

## Key Takeaways
1. Deadlock needs all four conditions — mutual exclusion, lock & wait, no preemption, circular wait. **Break exactly one and it cannot happen.**
2. Breaking circular wait via a global resource ordering is usually the cheapest fix: it's a convention, not a mechanism.
3. Every deadlock remedy costs something — starvation, livelock, CPU burn, or longer-held locks. TANSTAAFL.
4. Prefer `Atomic*` classes over `synchronized` for simple shared counters and flags: CAS is optimistic locking and virtually never slower.
5. Use the `Executor` framework instead of hand-rolled thread pools; use `Callable`/`Future` when you need results back.
6. Debug output changes timing — a deadlock that "disappears" when you add logging has not been fixed.
7. Isolate the threaded portion of the design so you can tune and experiment; that isolation is what makes strategy selection empirical rather than guesswork.

## Connects To
- **Ch 13**: this appendix is the detailed tutorial for every recommendation there — the Client/Server example (p. 317), Possible Paths of Execution (p. 321), Digging Deeper (p. 323), Dependencies Between Methods (p. 329), Increasing Throughput (p. 333).
- **Ch 9**: testing code whose failures are rare requires exactly the pluggability F.I.R.S.T. and TDD produce.
- **Ch 11**: `Executor` and thread pools are construction concerns that belong on the `main` side of the line.
- **Lea99**: *Concurrent Programming in Java* — the origin of `java.util.concurrent`.
