# Chapter 4: Structured Programming

*Part II: Starting with the Bricks: Programming Paradigms*

## Core Idea
Dijkstra sought **mathematical proof** of program correctness; he never got it. What structured programming actually gave us is **falsifiability** — the ability to decompose a program into small units that tests can try to prove *incorrect*. **Software is a science, not a mathematics.**

## Frameworks Introduced

- **The Böhm–Jacopini Result + Dijkstra's Discovery** — the founding coincidence.
  - Dijkstra found that certain uses of `goto` *"prevent modules from being decomposed recursively into smaller and smaller units, thereby preventing use of the divide-and-conquer approach necessary for reasonable proofs."*
  - The "good" uses of `goto` corresponded to **simple selection and iteration** — `if/then/else` and `do/while`. Modules using only those could be recursively subdivided into **provable units**.
  - Böhm and Jacopini had proved two years earlier that **all programs can be constructed from just three structures: sequence, selection, and iteration.**
  - > *"The very control structures that made a module provable were the same minimum set of control structures from which all programs can be built. Thus structured programming was born."*

- **The Three Proof Techniques** Dijkstra developed:

| Structure | Proof method |
|---|---|
| **Sequence** | **Enumeration** — mathematically trace inputs to outputs; no different from a normal mathematical proof |
| **Selection** | **Reapplication of enumeration** — enumerate each path; if both produce appropriate results, the proof is solid |
| **Iteration** | **Induction** — prove case 1 by enumeration, then prove N ⟹ N+1 by enumeration, plus starting and ending criteria |

*"Such proofs were laborious and complex — but they were proofs."*

- **Falsifiability as the Replacement for Proof** — the chapter's real thesis.
  - **Mathematics** is *"the discipline of proving provable statements true."*
  - **Science** is *"the discipline of proving provable statements false."* Scientific laws *"are falsifiable but not provable"* — F = ma cannot be proven, only demonstrated and not-yet-refuted. *"And yet we bet our lives on these laws every day."*
  - Dijkstra: **"Testing shows the presence, not the absence, of bugs."**
  - > *"Software development is not a mathematical endeavor, even though it seems to manipulate mathematical constructs. Rather, software is like a science. We show correctness by failing to prove incorrectness, despite our best efforts."*

- **The Critical Dependency** — why structure still matters even though the proofs never came:
  > *"Such proofs of incorrectness can be applied only to **provable programs**. A program that is not provable — due to unrestrained use of `goto`, for example — cannot be deemed correct no matter how many tests are applied to it."*

  Structured programming forces recursive decomposition into small provable functions; **tests then attack those functions**; functions that survive are *"correct enough for our purposes."*

## Worked Example: The Goto War and How It Ended

- **1968** — Dijkstra's letter to the editor of *CACM*, published in March: **"Go To Statement Considered Harmful."**
- *"And the programming world caught fire."* No Internet, so the flaming happened via letters to journal editors — *"Some were intensely negative; others voiced strong support."*
- **The battle lasted about a decade**, then petered out. **"The reason was simple: Dijkstra had won."**
- The mechanism of victory was not persuasion but language evolution: *"As computer languages evolved, the `goto` statement moved ever rearward, until it all but disappeared."*
- > *"Nowadays we are all structured programmers, **though not necessarily by choice**. It's just that our languages don't give us the option to use undisciplined direct transfer of control."*
- **On the modern objections**: named `break`s in Java and exceptions are sometimes called `goto` analogs. *"In fact, these structures are not the utterly unrestricted transfers of control that older languages like Fortran or COBOL once had."* Even languages retaining the `goto` keyword typically restrict the target to the current function's scope.

## Key Concepts
- **Functional decomposition** — take a large-scale problem statement, decompose into high-level functions, each into lower-level functions, *ad infinitum*, each expressible in the restricted control structures. This foundation produced **structured analysis and structured design**, popularized in the late 1970s–1980s by Ed Yourdon, Larry Constantine, Tom DeMarco, and Meilir Page-Jones.
- **No formal proofs** — *"The Euclidean hierarchy of theorems was never built… Dijkstra's dream faded and died. Few of today's programmers believe that formal proofs are an appropriate way to produce high-quality software."*
- **Provable unit** — a module decomposable recursively because it uses only sequence, selection, and iteration.
- **Not all statements are provable** — *"This is a lie"* is neither true nor false.

## Mental Models
- **Treat every module as a hypothesis.** You never confirm it; you fail to refute it, then ship it as "correct enough for our purposes."
- **Testability is not a nicety — it's a precondition for any correctness claim.** An untestable module cannot be deemed correct by any amount of testing, because there is nothing to falsify.
- **Scale the discipline up.** *"Software architects strive to define modules, components, and services that are easily falsifiable (testable). To do so, they employ restrictive disciplines similar to structured programming, albeit at a much higher level."*

## Key Takeaways
1. Structured programming = sequence + selection + iteration; unrestrained `goto` destroys recursive decomposition.
2. Dijkstra's proof program failed, but the structure it required survived and is now enforced by our languages.
3. Software is falsifiable, not provable — testing shows presence, never absence, of bugs.
4. Only provable (decomposable, testable) programs can be deemed correct at all; testing an untestable structure proves nothing.
5. Functional decomposition remains a best practice at the architectural level for exactly this reason.
6. The architect's job is to define components that are *easily falsifiable* — testability is an architectural property.

## Connects To
- **Ch 3**: "discipline on direct transfer of control," stated in full here.
- **Ch 28 (The Test Boundary)**: tests as a first-class architectural component follow from falsifiability.
- **Ch 22**: the Clean Architecture's concentric structure is a restrictive discipline at a much higher level.
- **Clean Code Ch 9**: the Three Laws of TDD are the practical form of "fail to prove incorrect."
- **Böhm & Jacopini (1966)**; **Dijkstra, "Go To Statement Considered Harmful," *CACM*, March 1968**.
