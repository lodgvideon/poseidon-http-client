---
name: martin-clean-code
description: "Knowledge base from \"Clean Code: A Handbook of Agile Software Craftsmanship\" by Robert C. Martin. Use when applying Uncle Bob's frameworks for naming, function design, comments, formatting, error handling, unit tests, class design, concurrency, refactoring, and code smells — while writing or reviewing code, studying the book, or referencing its concepts."
---

<!-- argument-hint: [topic, framework name, or chapter number] -->

# Clean Code: A Handbook of Agile Software Craftsmanship
**Author**: Robert C. Martin (with Tim Ottinger, Michael Feathers, James Grenning, Jeff Langr, Brett L. Schuchert, Dean Wampler) | **Pages**: 462 | **Chapters**: 17 + appendices | **Generated**: 2026-08-21

## How to Use This Skill

- **Without arguments** — load the core frameworks below for reference
- **With a topic** — ask about `naming`, `function arguments`, `error handling`, `deadlock`, `code smells`; I find and read the relevant chapter
- **With a chapter** — ask for `ch03`; I load that specific chapter file
- **Browse** — ask "what chapters do you have?" for the full index

When you ask about a topic not covered below, I read the relevant chapter file before answering. For fast decisions — thresholds, decision trees, smell recognition — go straight to [cheatsheet.md](cheatsheet.md).

---

## Core Frameworks & Mental Models

### The governing claim
**The only way to go fast is to keep the code clean.** Speed and cleanliness are the same variable, not a trade-off. Bad code slows a team asymptotically toward zero, and adding staff accelerates the rot because newcomers can't tell design-honoring changes from design-thwarting ones.

- **The Boy Scout Rule** — *"Leave the campground cleaner than you found it."* One small improvement per check-in: rename one variable, split one long function, delete one duplication.
- **LeBlanc's Law** — *"Later equals never."* If you won't clean it now, you've decided to keep it.
- **The 10:1 read/write ratio** — you read code far more than you write it. Optimize for reading *even when it makes writing harder*; you cannot write code you cannot read around.
- **Code-sense** — recognizing bad code and knowing how to write good code are different skills. The second must be deliberately acquired.

### Names (Ch 2)
- **A name must answer why it exists, what it does, how it is used.** If it needs a comment, the name failed.
- **Name length tracks scope size** [N5]. Single letters only inside short methods — never `l` or `O`.
- **Make distinctions mean something.** `Info`, `Data`, `Object`, `a1/a2` satisfy the compiler, not the reader.
- **Classes are nouns, methods are verbs.** Never `Manager`, `Processor`, `Super` — they signal aggregated responsibilities.
- **One word per concept, and never one word for two concepts** (don't pun: `add` ≠ `insert`).
- **Prefer creating a class over prefixing variables** when you need context (`Address`, not `addrState`).
- **Rename fearlessly** — IDEs make it cheap.

### Functions (Ch 3)
- **Small, then smaller.** Blocks inside `if`/`while` are one line — a call. **Indent level ≤ 2.**
- **Do One Thing**, testable three ways: the **TO-paragraph test** (every step exactly one level below the name), the **extraction test** (can you extract a function whose name isn't a restatement?), and the **sections test** (a function divisible into sections does many things).
- **The Stepdown Rule** — order functions so the module reads top-down, each introducing the next.
- **Arguments**: 0 ideal → 1 → 2 → avoid 3 → never more. **Flag arguments and output arguments are defects, not styles.**
- **Command Query Separation** — do something *or* answer something, never both.
- **Prefer exceptions to error codes**; extract try/catch bodies; error handling is one thing.
- **DRY** — *"Duplication may be the root of all evil in software."*

### Comments (Ch 4)
**Comments are always failures** — they compensate for an inability to express intent in code, and they rot because code moves while comments can't follow. **Inaccurate comments are worse than none**; the code is the only source of truth. The short list of good comments: legal, informative, intent, clarification, warning of consequences, TODO, amplification, public-API Javadoc. Everything else — redundant, mandated, journal, noise, bylines, commented-out code, HTML — **delete**.

### Objects vs. Data Structures (Ch 6)
**Data/Object Anti-Symmetry**: objects hide data and expose behavior; data structures expose data and have no behavior. *"Procedural code makes it easy to add new functions without changing existing data structures. OO code makes it easy to add new classes without changing existing functions"* — and each is hard where the other is easy. **Choose by the axis of expected change.** *"The idea that everything is an object is a myth."* Never build hybrids.

### Tests (Ch 9)
- **Three Laws of TDD** — no production code without a failing test; no more test than suffices to fail (not compiling *is* failing); no more production code than suffices to pass. Cycle length: ~30 seconds.
- **Test code is as important as production code.** Dirty tests get abandoned; then change becomes frightening; then the production code rots.
- **Tests enable the -ilities, because tests enable change.**
- **F.I.R.S.T.** — Fast, Independent, Repeatable, Self-Validating, Timely.
- **One concept per test** beats dogmatic one-assert-per-test.

### Classes & Systems (Ch 10–11)
- **SRP** — one, and only one, reason to change. Measure class size in **responsibilities**, not lines. The 25-word test: describe it without "and"/"or"/"but".
- **OCP** — open for extension, closed for modification.
- **DIP** — depend on abstractions; decoupling *is* testability.
- **Separate construction from use** — wire in `main` or a container; every dependency arrow points away from `main`.
- **Postpone decisions until the last responsible moment** — *"a premature decision is a decision made with suboptimal knowledge."*

### Emergent Design (Ch 12) — Kent Beck's four rules, in priority order
1. **Runs all the tests** 2. **Contains no duplication** 3. **Expresses the intent of the programmer** 4. **Minimizes the number of classes and methods**

Rule 1 does the heavy lifting: testability forces small classes (SRP) and loose coupling (DIP). **Writing tests leads to better designs.**

### How clean code actually gets written (Ch 14)
**"To write clean code, you must first write dirty code and then clean it."** Nobody produces it in one pass. Write it under test, then refine: split functions, rename, remove duplication, reorder, extract classes — keeping tests green throughout. **Recognize the moment before a mess becomes unfixable and stop adding features then.** For legacy code, the order is **first make it work** (cover it, measure coverage, fix bugs), **then make it right**.

---

## Chapter Index

| # | Title | Key Frameworks |
|---|-------|----------------|
| [ch01](chapters/ch01-clean-code.md) | Clean Code | Boy Scout Rule, LeBlanc's Law, Beck's Rules of Simple Code, Grand Redesign |
| [ch02](chapters/ch02-meaningful-names.md) | Meaningful Names | Intention-revealing names, searchable names, avoid encodings, one word per concept |
| [ch03](chapters/ch03-functions.md) | Functions | Do One Thing, Stepdown Rule, TO-paragraph test, argument arity, Command Query Separation |
| [ch04](chapters/ch04-comments.md) | Comments | Comments are failures, explain yourself in code, the good/bad comment catalog |
| [ch05](chapters/ch05-formatting.md) | Formatting | Newspaper Metaphor, vertical openness/density/distance, Team Rules |
| [ch06](chapters/ch06-objects-and-data-structures.md) | Objects and Data Structures | Data/Object Anti-Symmetry, Law of Demeter, train wrecks, DTO/Active Record |
| [ch07](chapters/ch07-error-handling.md) | Error Handling | Exceptions over error codes, try-catch-finally first, Special Case Pattern, don't return/pass null |
| [ch08](chapters/ch08-boundaries.md) | Boundaries | Learning tests, wrap third-party APIs, the interface you wish you had, Adapter |
| [ch09](chapters/ch09-unit-tests.md) | Unit Tests | Three Laws of TDD, F.I.R.S.T., Build-Operate-Check, domain-specific testing language |
| [ch10](chapters/ch10-classes.md) | Classes | SRP, cohesion, 25-word test, OCP, DIP, organizing for change |
| [ch11](chapters/ch11-systems.md) | Systems | Separate construction from use, DI, cross-cutting concerns, POJOs, DSLs, anti-BDUF |
| [ch12](chapters/ch12-emergence.md) | Emergence | Four Rules of Simple Design, Template Method, reuse in the small |
| [ch13](chapters/ch13-concurrency.md) | Concurrency | SRP for threads, limit data scope, Producer-Consumer / Readers-Writers / Dining Philosophers |
| [ch14](chapters/ch14-successive-refinement.md) | Successive Refinement | Write dirty then clean, incrementalism, the rule of three places |
| [ch15](chapters/ch15-junit-internals.md) | JUnit Internals | Live refactoring with heuristic tags, exposing temporal coupling |
| [ch16](chapters/ch16-refactoring-serialdate.md) | Refactoring SerialDate | First make it work then make it right, professional review, coverage as an oracle |
| [ch17](chapters/ch17-smells-and-heuristics.md) | Smells and Heuristics | **The 66-item catalog**: C1–C5, E1–E2, F1–F4, G1–G36, J1–J3, N1–N7, T1–T9 |
| [ch18](chapters/ch18-appendix-a-concurrency-ii.md) | Appendix A: Concurrency II | Four deadlock conditions and how to break them, CAS, Executor framework |

## Topic Index

- **Abstraction levels** → ch03, ch06, ch17 [G6, G34]
- **Argument objects / arity** → ch03, ch17 [F1]
- **Boundaries / third-party code** → ch08, ch07, ch11
- **Boy Scout Rule** → ch01, ch15, ch16
- **Class size / God classes** → ch10, ch05
- **Code smells (full catalog)** → ch17
- **Cohesion** → ch10, ch12
- **Command Query Separation** → ch03
- **Comments** → ch04, ch17 [C1–C5]
- **Concurrency / threads** → ch13, ch18
- **Deadlock / livelock / starvation** → ch18, ch13
- **Dependency Injection** → ch11, ch10
- **DIP** → ch10, ch11, ch12
- **DRY / duplication** → ch03, ch12, ch17 [G5]
- **Error handling / exceptions** → ch07, ch03
- **Feature Envy** → ch06, ch17 [G14]
- **Flag / selector arguments** → ch03, ch17 [F3, G15]
- **Formatting / line length / file size** → ch05
- **Function length** → ch03, ch05
- **Law of Demeter / train wrecks** → ch06, ch17 [G36]
- **Learning tests** → ch08
- **Magic numbers** → ch02, ch17 [G25]
- **Naming** → ch02, ch17 [N1–N7]
- **Null (returning / passing)** → ch07
- **OCP** → ch03, ch10, ch12
- **Objects vs. data structures** → ch06
- **Polymorphism over switch** → ch03, ch06, ch17 [G23]
- **Refactoring (worked examples)** → ch14, ch15, ch16
- **Simple Design (four rules)** → ch12, ch01
- **Special Case Pattern** → ch07
- **SRP** → ch10, ch13, ch11
- **Stepdown Rule** → ch03, ch05
- **Switch statements** → ch03, ch17 [G23]
- **TDD (Three Laws)** → ch09, ch14
- **Temporal coupling** → ch03, ch15, ch17 [G31]
- **Template Method** → ch09, ch12
- **Test coverage** → ch16, ch17 [T2, T8]
- **Testing heuristics** → ch09, ch17 [T1–T9]
- **Vertical distance / ordering** → ch05, ch17 [G10]

## Supporting Files

- [glossary.md](glossary.md) — every key term with a one-line definition and chapter reference
- [patterns.md](patterns.md) — all techniques as *when to use → how → trade-offs*
- [cheatsheet.md](cheatsheet.md) — thresholds, decision rules, decision trees, and smell recognition

---

## Scope & Limits

This skill covers the book's content only. Examples are Java (2008); the principles transfer, but library advice (`java.util.concurrent`, Java 5 enums, checked exceptions, wildcard imports) is dated — check current language idioms before applying [J1–J3] literally. For applying these rules in a specific codebase, combine with project-specific tooling and conventions; **team conventions beat book conventions** (Ch 5, "Team Rules"). Appendix B (the full `SerialDate` source listing) and Appendix C (the heuristics cross-reference) were not summarized; the heuristics themselves are in ch17.
