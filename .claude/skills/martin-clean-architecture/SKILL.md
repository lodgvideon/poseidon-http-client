---
name: martin-clean-architecture
description: "Knowledge base from \"Clean Architecture: A Craftsman's Guide to Software Structure and Design\" by Robert C. Martin. Use when applying Uncle Bob's frameworks for SOLID principles, component design, architectural boundaries, the Dependency Rule, use cases and entities, decoupling modes, or treating databases/web/frameworks as details — while designing systems, reviewing architecture, or referencing the book."
---

<!-- argument-hint: [topic, principle name, or chapter number] -->

# Clean Architecture: A Craftsman's Guide to Software Structure and Design
**Author**: Robert C. Martin (with James Grenning and Simon Brown) | **Pages**: 429 | **Chapters**: 34 + appendix | **Generated**: 2026-08-21

## How to Use This Skill

- **Without arguments** — load the core frameworks below
- **With a topic** — ask about `SOLID`, `boundaries`, `component cohesion`, `micro-services`, `testability`; I find and read the relevant chapter
- **With a chapter** — ask for `ch22`; I load that file
- **Browse** — ask "what chapters do you have?" for the full index

For fast decisions — the metrics, decision trees, and smell table — go straight to [cheatsheet.md](cheatsheet.md).

**Companion skill**: `martin-clean-code` covers the same author at the code level (names, functions, comments, tests). This skill covers structure: boundaries, components, and dependencies.

---

## Core Frameworks & Mental Models

### The goal, stated once
> **"The goal of software architecture is to minimize the human resources required to build and maintain the required system."**

Measure design quality by whether effort per release is **flat or rising**. And: *"The only way to go fast, is to go well."*

### Two values (Ch 2)
Software provides **behavior** (urgent, often unimportant) and **structure** (important, never urgent). Structure wins, provable by extremes: a program that works but can't change becomes **useless**; one that's broken but changeable stays **useful**.

**The key diagnostic — scope vs. shape**: *"The difficulty in making a change should be proportional only to the **scope** of the change, and not to the **shape** of the change."* When similar-sized requests cost progressively more, the architecture prefers a shape your requirements don't have. **Architectures should be as shape-agnostic as practical.**

You are a stakeholder. *"It is the responsibility of the software development team to assert the importance of architecture over the urgency of features."*

### The three paradigms (Ch 3–6)
Each **removes** a capability; none adds one.
- **Structured** → discipline on **direct transfer of control** (`goto`). Bought us **falsifiability**: software is a science — we show correctness by failing to prove incorrectness. *Only testable structures can be deemed correct at all.*
- **Object-oriented** → discipline on **indirect transfer of control** (function pointers). To an architect, **OO is absolute control over the direction of every source code dependency** — encapsulation earns no points, inheritance half a point.
- **Functional** → discipline on **assignment**. *"All race conditions, deadlock conditions, and concurrent update problems are due to mutable variables."*

### SOLID (Ch 7–11) — mid-level structure
| Principle | The actual statement |
|---|---|
| **SRP** | *"A module should be responsible to one, and only one, **actor**."* **Not** "do one thing." Symptoms of violation: accidental duplication and merge collisions |
| **OCP** | Open for extension, closed for modification. At architectural scale: **if A must be protected from B, make B depend on A** |
| **LSP** | Substitution must leave behavior unchanged. Violations are paid for in **permanent extra mechanism** |
| **ISP** | Don't depend on things you don't use — unused dependencies propagate **rebuilds and failures** |
| **DIP** | Depend on abstractions, not **volatile** concretions. Violations are inevitable — **gather them into `Main`** |

### Components (Ch 12–14)
**Cohesion — three principles in tension**: **REP** (granule of reuse = granule of release) and **CCP** (gather what changes together = SRP for components) push components **larger**; **CRP** (don't force unneeded dependencies = ISP for components) pushes them **smaller**. Early projects favor CCP (developability); mature ones slide toward REP.

**Coupling**: **ADP** — no cycles (break them with DIP or a new shared component). **SDP** — depend toward stability, where stability = *"not easily moved"* = incoming dependencies, **not** change frequency. **SAP** — be as abstract as you are stable. **SDP + SAP = DIP for components.**

Metrics: `I = Fan-out/(Fan-in+Fan-out)`, `A = Na/Nc`, `D = |A+I−1|`. Avoid the **Zone of Pain** (stable + concrete + volatile — database schemas) and the **Zone of Uselessness** (abstract + unwanted).

**Component structure cannot be designed top-down** — it evolves, and it is a map of **buildability and maintainability**, not of function.

### The Dependency Rule (Ch 22) — the center of the book
> **"Source code dependencies must point only inward, toward higher-level policies."**

No inner circle may **name** anything in an outer one — including data formats. Four circles (schematic, not mandatory): **Entities** → **Use Cases** → **Interface Adapters** (all MVC, all SQL) → **Frameworks & Drivers** (glue only).

Cross boundaries with **output ports**: the use case calls an interface declared *inside*; the outer class implements it. Only **simple data structures** cross — never Entity objects, never database rows.

### Policy, level, and details (Ch 15, 19)
**Level = distance from the inputs and outputs.** Higher-level policies change less often, for more important reasons; lower-level ones change often, urgently, for trivial reasons. **Couple source dependencies to level, not to data flow.**

**Policy** holds the business rules — the true value. **Details** are IO devices, databases, web systems, frameworks, protocols. *"A good architect **maximizes the number of decisions not made**."* If someone already chose the database or framework, **pretend they haven't**.

### Business rules (Ch 20)
**Entities** hold Critical Business Rules — those that would make money *"even if executed manually, by a clerk with an abacus."* **Use cases** hold application-specific rules and *"control the dance of the Entities."* Entities are **higher level** because they're general and far from IO. Use cases depend on Entities, never the reverse. Both take dependency-free request/response structures.

### Independence and decoupling modes (Ch 16)
Decouple **horizontally** into layers and **vertically** into use cases. Three modes — **source**, **deployment**, **service** — and the optimal one changes over a project's life. *"Push the decoupling to the point where a service **could** be formed… but leave the components in the same address space as long as possible."* A good architecture supports growing into services **and sliding back to a monolith**.

**Distinguish true from accidental duplication**: if two similar sections change at different rates for different reasons, **leave them apart.**

### Details, named (Ch 30–32)
- **The database is a detail.** The *data model* is architecturally significant; the database is *"a big bucket of bits."* Ask: if there were no disk, how would you structure this? That's your model.
- **The web is a detail.** One swing of a pendulum oscillating since the 1960s. Abstract the **transaction** (input complete → use case → output), not the chatty GUI dance.
- **Frameworks are details.** *"Don't marry the framework!"* The relationship is asymmetric — you commit, the author doesn't. Derive proxies; confine DI to `Main`.

### Testability is structural (Ch 21, 28)
Tests are the **outermost circle** — nothing depends on them. If the architecture is really about use cases, you can **unit-test every use case with no web server, no database, no framework**. Fragile tests make the system **rigid**; build a **testing API** that decouples test *structure* from application *structure*.

---

## Chapter Index

| # | Title | Key Frameworks |
|---|-------|----------------|
| [ch01](chapters/ch01-what-is-design-and-architecture.md) | What Is Design and Architecture? | Goal of architecture, Tortoise & Hare, "go fast = go well" |
| [ch02](chapters/ch02-a-tale-of-two-values.md) | A Tale of Two Values | Behavior vs. structure, scope vs. shape, Eisenhower matrix |
| [ch03](chapters/ch03-paradigm-overview.md) | Paradigm Overview | Three paradigms as subtractions |
| [ch04](chapters/ch04-structured-programming.md) | Structured Programming | Falsifiability, functional decomposition |
| [ch05](chapters/ch05-object-oriented-programming.md) | Object-Oriented Programming | OO = dependency control; plugin architecture |
| [ch06](chapters/ch06-functional-programming.md) | Functional Programming | Immutability, segregation of mutability, event sourcing |
| [ch07](chapters/ch07-srp-single-responsibility-principle.md) | SRP | Actors, accidental duplication, merges |
| [ch08](chapters/ch08-ocp-open-closed-principle.md) | OCP | Hierarchy of protection, directional control |
| [ch09](chapters/ch09-lsp-liskov-substitution-principle.md) | LSP | Square/Rectangle, taxi dispatch |
| [ch10](chapters/ch10-isp-interface-segregation-principle.md) | ISP | Fat interfaces, transitive framework dependencies |
| [ch11](chapters/ch11-dip-dependency-inversion-principle.md) | DIP | Stable abstractions, Abstract Factory, `Main` |
| [ch12](chapters/ch12-components.md) | Components | Units of deployment; 50 years to plugins |
| [ch13](chapters/ch13-component-cohesion.md) | Component Cohesion | REP, CCP, CRP, the tension diagram |
| [ch14](chapters/ch14-component-coupling.md) | Component Coupling | ADP, SDP, SAP, I/A/D metrics, Main Sequence |
| [ch15](chapters/ch15-what-is-architecture.md) | What Is Architecture? | Four life-cycle concerns, keeping options open |
| [ch16](chapters/ch16-independence.md) | Independence | Decoupling layers/use cases, three modes, duplication |
| [ch17](chapters/ch17-boundaries-drawing-lines.md) | Boundaries: Drawing Lines | Premature decisions, FitNesse, plugin argument |
| [ch18](chapters/ch18-boundary-anatomy.md) | Boundary Anatomy | Four boundary strengths and their costs |
| [ch19](chapters/ch19-policy-and-level.md) | Policy and Level | Level = distance from IO |
| [ch20](chapters/ch20-business-rules.md) | Business Rules | Entities, use cases, request/response models |
| [ch21](chapters/ch21-screaming-architecture.md) | Screaming Architecture | Frameworks as tools, testable architectures |
| [ch22](chapters/ch22-the-clean-architecture.md) | **The Clean Architecture** | **The Dependency Rule**, four circles, output ports |
| [ch23](chapters/ch23-presenters-and-humble-objects.md) | Presenters and Humble Objects | Humble Object, gateways, data mappers |
| [ch24](chapters/ch24-partial-boundaries.md) | Partial Boundaries | Skip-the-last-step, Strategy, Facade |
| [ch25](chapters/ch25-layers-and-boundaries.md) | Layers and Boundaries | Hunt the Wumpus, API ownership, the watchful eye |
| [ch26](chapters/ch26-the-main-component.md) | The Main Component | `Main` as the dirtiest plugin |
| [ch27](chapters/ch27-services-great-and-small.md) | Services: Great and Small | Decoupling fallacy, the Kitty Problem |
| [ch28](chapters/ch28-the-test-boundary.md) | The Test Boundary | Fragile Tests Problem, testing API, structural coupling |
| [ch29](chapters/ch29-clean-embedded-architecture.md) | Clean Embedded Architecture | Firmware redefined, HAL/PAL/OSAL |
| [ch30](chapters/ch30-the-database-is-a-detail.md) | The Database Is a Detail | Data model vs. database; the RDBMS anecdote |
| [ch31](chapters/ch31-the-web-is-a-detail.md) | The Web Is a Detail | The endless pendulum; abstracting the transaction |
| [ch32](chapters/ch32-frameworks-are-details.md) | Frameworks Are Details | Asymmetric marriage, four risks, proxies |
| [ch33](chapters/ch33-case-study-video-sales.md) | Case Study: Video Sales | Actors → use cases → components |
| [ch34](chapters/ch34-the-missing-chapter.md) | The Missing Chapter *(Simon Brown)* | Package by layer/feature/component, compiler enforcement |
| [ch35](chapters/ch35-appendix-a-architecture-archaeology.md) | Appendix A: Architecture Archaeology | 45 years of projects; reusable frameworks |

## Topic Index

- **Actors** → ch07, ch33
- **ADP / dependency cycles** → ch14
- **Boundaries (drawing / anatomy / partial)** → ch17, ch18, ch24, ch25
- **Component cohesion (REP/CCP/CRP)** → ch13
- **Component coupling (SDP/SAP)** → ch14
- **Conway's law** → ch16, ch07
- **Database** → ch30, ch17, ch22, ch23
- **Decoupling modes** → ch16, ch18, ch34
- **Dependency Rule** → ch22, ch11, ch19
- **DIP** → ch11, ch05, ch14
- **Duplication (true vs. accidental)** → ch16
- **Embedded / firmware / HAL** → ch29
- **Entities** → ch20, ch22
- **Event sourcing / immutability** → ch06
- **Frameworks** → ch32, ch21, ch10
- **Humble Object / Presenters** → ch23, ch22
- **ISP** → ch10, ch13
- **Level / policy vs. detail** → ch19, ch15
- **LSP** → ch09
- **`Main`** → ch26, ch11, ch32
- **Metrics (I, A, D, Main Sequence)** → ch14
- **Micro-services** → ch27, ch16, ch25, ch34
- **OCP** → ch08, ch14, ch27
- **Package organization / access modifiers** → ch34
- **Paradigms** → ch03, ch04, ch05, ch06
- **Plugin architecture** → ch17, ch05, ch12, ch26
- **Rewrite / Grand Redesign** → ch01, ch35
- **Screaming architecture** → ch21
- **SRP** → ch07, ch13, ch17, ch33
- **Testability / tests** → ch28, ch21, ch23, ch04
- **Use cases** → ch20, ch16, ch33, ch31
- **Web / GUI / IO** → ch31, ch17, ch21

## Supporting Files

- [glossary.md](glossary.md) — every key term with a definition and chapter reference
- [patterns.md](patterns.md) — techniques as *when to use → how → trade-offs*
- [cheatsheet.md](cheatsheet.md) — metrics, decision trees, trade-off matrices, smell table

---

## Scope & Limits

Covers the book's content only. Examples are Java/C/C++ (2017); the principles transfer, but specific technology commentary (Java 9 modules as "new," EJB, Spring idioms) is dated. This is a book about **structure** — for code-level craft (naming, function size, comments, unit tests) use the companion `martin-clean-code` skill. Appendix B (the index) was not summarized; Appendix A is at ch35.
