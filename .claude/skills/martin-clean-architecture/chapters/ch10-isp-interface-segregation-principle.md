# Chapter 10: ISP — The Interface Segregation Principle

*Part III: Design Principles*

## Core Idea
**"Depending on something that carries baggage that you don't need can cause you troubles that you didn't expect."** Avoid depending on things you don't use — at the class level *and* at the architectural level.

## Framework: The Diagram the Principle Is Named After

**The problem** (Fig 10.1): a class `OPS` exposes `op1`, `op2`, `op3`. `User1` uses only `op1`, `User2` only `op2`, `User3` only `op3`.

> *"In that case, the source code of `User1` will **inadvertently depend on `op2` and `op3`, even though it doesn't call them**. This dependence means that a change to the source code of `op2` in `OPS` will force `User1` to be recompiled and redeployed, even though nothing that it cared about has actually changed."*

**The fix** (Fig 10.2): segregate the operations into interfaces — `U1Ops`, `U2Ops`, `U3Ops`.

> *"The source code of `User1` will depend on `U1Ops`, and `op1`, but **will not depend on `OPS`**. Thus a change to `OPS` that `User1` does not care about will not cause `User1` to be recompiled and redeployed."*

## Framework: ISP and Language — the honest caveat

*"Clearly, the previously given description depends critically on language type."*

| Language type | Behavior | Consequence |
|---|---|---|
| **Statically typed** (Java) | Forces declarations that users must `import`, `use`, or otherwise include | *"It is these included declarations in source code that create the source code dependencies that force recompilation and redeployment"* |
| **Dynamically typed** (Ruby, Python) | Declarations don't exist in source; they are **inferred at runtime** | *"There are no source code dependencies to force recompilation and redeployment"* |

> *"This is the primary reason that **dynamically typed languages create systems that are more flexible and less tightly coupled** than statically typed languages."*

*"This fact could lead you to conclude that the ISP is a language issue, rather than an architecture issue."* — and the next section is the rebuttal.

## Framework: ISP and Architecture — the deeper concern

> *"If you take a step back and look at the root motivations of the ISP, you can see a deeper concern lurking there. **In general, it is harmful to depend on modules that contain more than you need.** This is obviously true for source code dependencies that can force unnecessary recompilation and redeployment — but it is also true at a much higher, architectural level."*

## Worked Example: The Framework That Dragged In a Database

**The setup** (Fig 10.3): an architect building system **S** wants to include framework **F**. The authors of F bound it to a particular database **D**.

**The dependency chain**: **S → F → D**

Now suppose **D contains features that F does not use**, and therefore that **S does not care about**. Two distinct failure modes follow:

| Failure mode | What happens |
|---|---|
| **Deployment coupling** | *"Changes to those features within D may well force the redeployment of F and, therefore, the redeployment of S"* |
| **Failure propagation** | **"Even worse, a failure of one of the features within D may cause failures in F and S."** |

The second is the sharper point: you inherit not just the *rebuild* burden of unused features but their *runtime failures*.

## Mental Models
- **Count what a dependency drags in, not just what it offers.** Evaluating a framework means evaluating everything the framework is bound to — transitively.
- **Unused features are not free.** They cost you rebuilds, redeployments, and — critically — availability, because their failures reach you.
- **Fat interfaces are a redeployment amplifier.** Every client of a wide interface is coupled to every change in it, however irrelevant.
- **Treat the "it's just a language issue" objection as a scale error.** The mechanism differs by language; the underlying harm (depending on more than you need) is universal.

## Anti-patterns
- **The god interface** — one class exposing every operation any client might want, so every client depends on all of them.
- **Adopting a framework without tracing its bindings** — S depends on D's failure modes without ever mentioning D.
- **Concluding ISP is irrelevant in dynamic languages** — the source-code mechanism vanishes; the architectural harm does not.

## Key Takeaways
1. Segregate wide interfaces so each client depends only on the operations it actually calls.
2. In statically typed languages, an unused method is still a source dependency forcing recompile and redeploy.
3. Dynamic typing removes that specific mechanism — which is a real flexibility advantage, honestly stated.
4. The principle survives the language difference: depending on more than you need is harmful at every level.
5. A framework's bindings become your bindings: F's database becomes S's database.
6. Unused transitive dependencies can propagate **failures**, not just rebuilds.

## Connects To
- **Ch 13 (Component Cohesion)**: the **Common Reuse Principle** is this idea at component scale — *"Don't force users of a component to depend on things they don't need."*
- **Ch 8 (OCP)**: "software entities should not depend on things they don't directly use" appears there as the reason for Information Hiding.
- **Ch 11 (DIP)**: depending on narrow abstractions instead of wide concretions.
- **Ch 32 (Frameworks Are Details)**: the framework-coupling problem, developed fully.
