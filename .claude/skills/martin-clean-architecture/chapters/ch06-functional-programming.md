# Chapter 6: Functional Programming

*Part II: Starting with the Bricks: Programming Paradigms*

## Core Idea
**"Variables in functional languages do not vary."** This matters architecturally for one reason: **all race conditions, deadlock conditions, and concurrent update problems are due to mutable variables.**

## Framework: The Architectural Argument for Immutability

> *"Why would an architect be concerned with the mutability of variables? The answer is absurdly simple: **All race conditions, deadlock conditions, and concurrent update problems are due to mutable variables.** You cannot have a race condition or a concurrent update problem if no variable is ever updated. You cannot have deadlocks without mutable locks."*

> *"All the problems that we face in concurrent applications — all the problems we face in applications that require multiple threads, and multiple processors — **cannot happen if there are no mutable variables**."*

**Is immutability practicable?** *"The answer to that question is affirmative, **if you have infinite storage and infinite processor speed**. Lacking those infinite resources, the answer is a bit more nuanced. Yes, immutability can be practicable, **if certain compromises are made**."* The chapter then presents the two compromises.

## Worked Example: Squares of the First 25 Integers

```java
// Java — uses a mutable variable: i, the loop control variable.
public class Squint {
    public static void main(String args[]) {
        for (int i = 0; i < 25; i++)
            System.out.println(i*i);
    }
}
```

```clojure
;; Clojure — no mutable variable exists. x is initialized, never modified.
(println (take 25 (map (fn [x] (* x x)) (range))))

;; Reformatted, reading innermost-outward:
(println                        ; ___________________ Print
  (take 25                      ; _________________ the first 25
    (map (fn [x] (* x x))       ; __ squares
      (range))))                ; ___________ of Integers
```

- `(range)` returns a **never-ending** list of integers starting at 0.
- `map` applies the anonymous squaring function to each element → a never-ending list of squares.
- `take` returns a new list of only the first 25.
- `println` prints it.

*"If you find yourself terrified by the concept of never-ending lists, don't worry. Only the first 25 elements of those never-ending lists are actually created. That's because **no element of a never-ending list is evaluated until it is accessed**."*

**The point of the comparison is not syntax** — it is that the Java version mutates `i` and the Clojure version mutates nothing.

## Framework: Compromise 1 — Segregation of Mutability

**The structure**: partition the application (or its services) into **immutable** and **mutable** components. Immutable components do their work purely functionally; they communicate with one or more components that permit state mutation.

**The protection**: since mutating state exposes those components to all concurrency problems, *"it is common practice to use some kind of **transactional memory** to protect the mutable variables from concurrent updates and race conditions."*

> *"Transactional memory simply treats variables in memory the same way a database treats records on disk. It protects those variables with a transaction- or retry-based scheme."*

```clojure
(def counter (atom 0))   ; initialize counter to 0
(swap! counter inc)      ; safely increment counter
```

**How `swap!` works** — a traditional **compare and swap** algorithm:
1. The value of `counter` is read and passed to `inc`.
2. When `inc` returns, `counter` is **locked** and compared to the value that was passed in.
3. **Same** → the new value is stored and the lock released.
4. **Different** → the lock is released and **the strategy is retried from the beginning**.

**The honest limit**: *"The atom facility is adequate for simple applications. Unfortunately, it cannot completely safeguard against concurrent updates and deadlocks **when multiple dependent variables come into play**. In those instances, more elaborate facilities can be used."*

> **The architect's directive**: *"Architects would be wise to push **as much processing as possible into the immutable components**, and to drive as much code as possible **out of** those components that must allow mutation."*

## Framework: Compromise 2 — Event Sourcing

**The premise**: *"The limits of storage and processing power have been rapidly receding from view… The more memory we have, and the faster our machines are, the less we need mutable state."*

**The banking example:**
- Conventional: store account balances; **mutate** them on deposit and withdrawal.
- Event sourced: **store only the transactions.** To learn a balance, add up all transactions for that account from the beginning of time. **This scheme requires no mutable variables.**

**The obvious objection, answered:** *"Obviously, this approach sounds absurd. Over time, the number of transactions would grow without bound… To make this scheme work **forever**, we would need infinite storage and infinite processing power. **But perhaps we don't have to make the scheme work forever.** And perhaps we have enough storage and enough processing power to make the scheme work for the reasonable lifetime of the application."*

**The shortcut**: compute and save state every midnight; then only transactions since midnight need replaying.

**The consequences:**

| Property | Result |
|---|---|
| Nothing is ever deleted or updated | Applications are **not CRUD — they are just CR** |
| No updates or deletions in the store | **There cannot be any concurrent update issues** |
| Enough storage + processor power | Applications become **entirely immutable, and therefore entirely functional** |

> *"If this still sounds absurd, it might help if you remembered that **this is precisely the way your source code control system works**."*

## Part II Conclusion (stated here)

> - *Structured programming is discipline imposed upon **direct transfer of control**.*
> - *Object-oriented programming is discipline imposed upon **indirect transfer of control**.*
> - *Functional programming is discipline imposed upon **variable assignment**.*

> *"Each of these three paradigms has **taken something away** from us… None of them has added to our power or our capabilities. What we have learned over the last half-century is **what not to do**."*

**The unwelcome fact**: *"Software is not a rapidly advancing technology. The rules of software are the same today as they were in 1946, when Alan Turing wrote the very first code that would execute in an electronic computer. The tools have changed, and the hardware has changed, but **the essence of software remains the same**."*

> **"Software — the stuff of computer programs — is composed of sequence, selection, iteration, and indirection. Nothing more. Nothing less."**

## Mental Models
- **Treat concurrency bugs as assignment bugs.** Every race, deadlock, and lost update traces back to a variable that was allowed to change. That reframes the fix from "add locks" to "remove mutation."
- **Draw a mutability boundary the way you'd draw a security boundary.** Know exactly which components may mutate, and shrink that set deliberately.
- **Trade storage for correctness when the numbers allow it.** Event sourcing is not an exotic pattern — it's the same bargain your VCS already makes.
- **"Forever" is the wrong requirement.** A scheme only has to survive the reasonable lifetime of the application.

## Key Takeaways
1. Functional programming's discipline is on assignment: variables do not vary.
2. Every concurrency problem — races, deadlocks, concurrent updates — requires mutable state to exist at all.
3. Immutability is practicable with compromises, not with infinite resources.
4. Segregate mutable from immutable components, and protect the mutable ones with transactional memory (compare-and-swap).
5. Push as much processing as possible into immutable components; drive code out of the mutable ones.
6. Event sourcing stores transactions instead of state: CR instead of CRUD, and therefore no concurrent update issues.
7. Software is sequence, selection, iteration, and indirection — the essence hasn't changed since 1946.

## Connects To
- **Ch 3**: "discipline upon assignment," developed here.
- **Ch 5**: indirection (polymorphism) is the fourth element in the closing summary — the two chapters together define the whole toolkit.
- **Ch 16 (Independence)**: decoupling for concurrency is one of the axes an architecture must keep open.
- **Clean Code Ch 13 / Appendix A**: races, deadlock, and CAS — the same mechanisms at the code level.
- **Alonzo Church, λ-calculus (1930s)**; **Greg Young** (credited for event sourcing).
