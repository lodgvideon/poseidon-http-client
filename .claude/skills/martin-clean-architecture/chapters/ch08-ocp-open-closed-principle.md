# Chapter 8: OCP — The Open-Closed Principle

*Part III: Design Principles*

## Core Idea
**"A software artifact should be open for extension but closed for modification."** (Bertrand Meyer, 1988.) At the architectural level this becomes a **hierarchy of protection**: components are arranged so that higher-level ones are shielded from changes in lower-level ones.

## Framework: OCP as the Reason Architecture Exists

> *"This, of course, is the most fundamental reason that we study software architecture. Clearly, if simple extensions to the requirements force massive changes to the software, then the architects of that software system have engaged in a **spectacular failure**."*

**The two-step recipe**, using the other principles:
1. **Separate the things that change for different reasons** → SRP
2. **Organize the dependencies between those things properly** → DIP

## Worked Example: The Financial Summary Thought Experiment

**The setup**: a system displays a financial summary on a web page — scrollable, negative numbers in red.

**The new requirement**: the same information as a report printed on a black-and-white printer — properly paginated, with page headers, page footers, column labels, and **negative numbers in parentheses**.

> *"Clearly, some new code must be written. But **how much old code will have to change**? A good software architecture would reduce the amount of changed code to the barest minimum. **Ideally, zero.**"*

**Step 1 — apply SRP** (Fig 8.1). The essential insight: generating the report involves **two separate responsibilities**:
- the **calculation** of the reported data
- the **presentation** of that data into web- and printer-friendly form

**Step 2 — partition into components** (Fig 8.2), four of them:

| Position | Component |
|---|---|
| Upper left | **Controller** |
| Upper right | **Interactor** |
| Lower right | **Database** |
| Lower left | **Presenters and Views** (four components) |

Diagram conventions worth keeping: `<I>` marks interfaces, `<DS>` marks data structures; **open** arrowheads are *using* relationships, **closed** arrowheads are *implements/inheritance*.

**Two things to notice:**
1. **All the dependencies are source code dependencies.** *"An arrow pointing from class A to class B means that the source code of class A mentions the name of class B, but class B mentions nothing about class A."* `FinancialDataMapper` knows about `FinancialDataGateway` through an *implements* relationship, but `FinancialGateway` knows nothing about `FinancialDataMapper`.
2. **Each double line is crossed in one direction only** — all component relationships are unidirectional (Fig 8.3), *"and these arrows point toward the components that we want to protect from change."*

> **"If component A should be protected from changes in component B, then component B should depend on component A."**

## Framework: The Hierarchy of Protection

What gets protected from what, and why:

| Component | Protected from | Level | Why |
|---|---|---|---|
| **Interactor** | *Everything* — Database, Controller, Presenters, Views | **Highest** | *"Because it contains the business rules… the highest-level policies of the application"* |
| **Controller** | Presenters, Views | High-mid | Peripheral to the Interactor, but **central to** Presenters and Views |
| **Presenters** | Views | Low-mid | Peripheral to the Controller, but **central to** the Views |
| **Views** | — | **Lowest** | *"Among the lowest-level concepts, so they are the least protected"* |

*"All the other components are dealing with peripheral concerns. The Interactor deals with the central concern."*

> **"This is how the OCP works at the architectural level. Architects separate functionality based on how, why, and when it changes, and then organize that separated functionality into a hierarchy of components. Higher-level components in that hierarchy are protected from the changes made to lower-level components."**

## Two Supporting Mechanisms

**Directional Control** — *"If you recoiled in horror from the class design shown earlier, look again. Much of the complexity in that diagram was intended to make sure that the dependencies between the components pointed in the correct direction."*
- `FinancialDataGateway` exists between `FinancialReportGenerator` and `FinancialDataMapper` **to invert** a dependency that would otherwise point from the Interactor to the Database.
- Same for `FinancialReportPresenter` and the two `View` interfaces.

**Information Hiding** — `FinancialReportRequester` serves a *different* purpose: *"to protect the `FinancialReportController` from knowing too much about the internals of the Interactor. If that interface were not there, then the Controller would have **transitive dependencies** on the `FinancialEntities`."*

> *"Transitive dependencies are a violation of the general principle that **software entities should not depend on things they don't directly use**."*

So protection runs **both ways**: *"even though our first priority is to protect the Interactor from changes to the Controller, we also want to protect the Controller from changes to the Interactor by hiding the internals of the Interactor."*

## Mental Models
- **Draw the arrow toward what you want to keep still.** Deciding "which way does this dependency point?" is the same question as "which of these two do I want to protect?"
- **Level = distance from the central concern.** Business rules are highest because everything else is peripheral to them; the UI is lowest because everything is peripheral to it.
- **Interfaces in the diagram serve two distinct jobs** — inverting direction (Directional Control) and limiting knowledge (Information Hiding). Don't conflate them; a design can get direction right and still leak transitive dependencies.
- **Complexity in a dependency diagram is usually purposeful.** Before simplifying an interface away, ask which inversion or which hiding it was performing.

## Key Takeaways
1. OCP is the fundamental reason architecture exists: extensions should not force massive change.
2. Achieve it by separating what changes for different reasons (SRP), then pointing dependencies correctly (DIP).
3. **B depends on A** is how you protect **A** from **B**.
4. Component relationships must be unidirectional; every double line crossed in one direction only.
5. The Interactor — business rules — sits at the top of the protection hierarchy and is protected from everything.
6. Insert interfaces both to invert direction *and* to hide internals; transitive dependencies violate "don't depend on what you don't use."
7. Ideally, a new delivery mechanism (printer report) requires **zero** changes to existing code.

## Connects To
- **Ch 5**: dependency inversion is the mechanism this chapter deploys structurally.
- **Ch 10 (ISP)** and **Ch 13 (Common Reuse Principle)**: "don't depend on things you don't use," recurring.
- **Ch 11 (DIP)**: the curved line separating abstract from concrete becomes this chapter's component boundary.
- **Ch 19 (Policy and Level)**: "level" is defined precisely there; used informally here.
- **Ch 22 (The Clean Architecture)**: Controller / Interactor / Presenter / View is exactly the cast of characters.
- **Bertrand Meyer, *Object Oriented Software Construction*, 1988, p. 23.**
