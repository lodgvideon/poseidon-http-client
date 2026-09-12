# Chapter 3: Paradigm Overview

*Part II: Starting with the Bricks: Programming Paradigms*

## Core Idea
Each of the three paradigms **removes** a capability from the programmer — none adds one. They tell us **what not to do**, and together they remove `goto`, function pointers, and assignment.

## Framework: The Three Paradigms, One Sentence Each

| Paradigm | Discovered | By | The discipline |
|---|---|---|---|
| **Structured programming** | 1968 (adopted first) | Edsger Wybe Dijkstra | *"imposes discipline on **direct transfer of control**"* |
| **Object-oriented programming** | 1966 | Ole Johan Dahl, Kristen Nygaard | *"imposes discipline on **indirect transfer of control**"* |
| **Functional programming** | 1936 (invented first, adopted last) | Alonzo Church (λ-calculus) → LISP, John McCarthy, 1958 | *"imposes discipline upon **assignment**"* |

**How OO was discovered**: Dahl and Nygaard noticed the ALGOL function call stack frame could be moved to a **heap**, letting local variables outlive the function's return. *"The function became a constructor for a class, the local variables became instance variables, and the nested functions became methods. This led inevitably to the discovery of polymorphism through the disciplined use of function pointers."*

**What FP rests on**: λ-calculus's foundational notion of **immutability** — symbol values do not change — which effectively means a functional language has no assignment statement. (Most do provide some means to alter a variable, *"but only under very strict discipline."*)

## Framework: Food for Thought — The Subtractive Argument

> *"Each of the paradigms **removes** capabilities from the programmer. None of them **adds** new capabilities. Each imposes some kind of extra discipline that is **negative in its intent**. The paradigms tell us what **not** to do, more than they tell us what to do."*

**The closure argument** — why there will be no fourth paradigm:
- The three together remove **`goto` statements, function pointers, and assignment**. *"Is there anything left to take away? Probably not."*
- Corroborating evidence: **all three were discovered within the ten years between 1958 and 1968.** *"In the many decades that have followed, no new paradigms have been added."*

## Mental Models
- **Read a paradigm as a constraint, not a feature set.** The question is never "what can I now do?" but "what am I now prevented from doing, and what does that buy?"
- **Map each paradigm to the architectural concern it serves** — this is the chapter's payoff:

| Paradigm | Architectural use | Big concern of architecture |
|---|---|---|
| **Polymorphism** (OO) | *"the mechanism to cross architectural boundaries"* | **Separation of components** |
| **Functional** | *"impose discipline on the location of and access to data"* | **Data management** |
| **Structured** | *"the algorithmic foundation of our modules"* | **Function** |

*"Notice how well those three align with the three big concerns of architecture."*

## Key Takeaways
1. Three paradigms, three subtractions: direct transfer of control, indirect transfer of control, assignment.
2. Paradigms constrain; they never empower. Fifty years of progress is a list of things not to do.
3. All three were found in a single decade (1958–1968), and nothing has been added since — expect no fourth.
4. Each paradigm maps onto exactly one of architecture's three big concerns.
5. Polymorphism is the boundary-crossing mechanism — this is why OO matters architecturally, not because it "models the real world."

## Connects To
- **Ch 4**: structured programming, and why falsifiability (not proof) is what it bought us.
- **Ch 5**: OO as absolute control over source code dependencies via polymorphism.
- **Ch 6**: immutability, and why concurrency problems are assignment problems.
- **Ch 17–22**: crossing boundaries with polymorphism is the practical use of this chapter's claim.
