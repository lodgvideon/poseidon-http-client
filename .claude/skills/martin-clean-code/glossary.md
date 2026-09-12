# Glossary

**Abstract Factory** — GoF pattern used to bury a `switch` so it appears once, creates polymorphic objects, and stays hidden behind inheritance. (Ch 3, Ch 11)

**Active Record** — a DTO with navigational methods like `save` and `find`, usually a direct translation of a database table. Keep it a data structure; put business rules in separate objects. (Ch 6)

**Adapter** — GoF pattern bridging your ideal interface to a real API you don't control; gives one place to change when the API evolves. (Ch 8)

**Aspect** — a modular construct specifying which points in a system have their behavior modified in a consistent way, applied noninvasively. (Ch 11)

**BDUF (Big Design Up Front)** — designing everything before implementing anything. Harmful: it resists adaptation and biases all later thinking. Distinct from ordinary up-front design. (Ch 11)

**Bean** — private variables with getters and setters. *"The quasi-encapsulation of beans seems to make some OO purists feel better but usually provides no other benefit."* (Ch 6)

**Bound Resources** — resources of fixed size or number used concurrently: database connections, fixed-size buffers. (Ch 13)

**Boy Scout Rule** — *"Leave the campground cleaner than you found it."* One small improvement per check-in. (Ch 1, Ch 15, Ch 16)

**Build-Operate-Check** — the three-part structure a clean test should make visible. (Ch 9)

**CAS (Compare and Swap)** — atomic processor operation underlying `Atomic*` classes; optimistic locking, versus `synchronized`'s pessimistic locking. (Appendix A)

**Circular Wait** — "the deadly embrace"; the deadlock condition most commonly broken, via a global resource ordering. (Appendix A)

**Clean code** — code that reads like well-written prose, does one thing well, has no duplication, and looks like someone cared. (Ch 1)

**Code-sense** — the acquired aesthetic that lets a programmer see the sequence of behavior-preserving transformations out of a mess, not merely recognize the mess. (Ch 1)

**Command Query Separation** — a function either *does* something or *answers* something, never both. (Ch 3)

**Conceptual Affinity** — code that belongs near other code because it shares a naming scheme or performs variations of one task, independent of call relationships. (Ch 5)

**Cross-Cutting Concern** — a concern (persistence, security, transactions) that cuts across natural object boundaries; the motivation for AOP. (Ch 11)

**Data/Object Anti-Symmetry** — objects hide data and expose behavior; data structures do the reverse. What is easy for one is hard for the other. (Ch 6)

**Dead Code** — code that isn't executed: impossible `if` branches, `catch` blocks for exceptions never thrown, uncalled utilities. [G9] (Ch 17)

**Deadlock** — two or more threads each holding a resource the other needs. Requires all four conditions: mutual exclusion, lock & wait, no preemption, circular wait. (Ch 13, Appendix A)

**Dependency Injection (DI)** — Inversion of Control applied to dependency management; the class is completely passive about resolving its dependencies. (Ch 11)

**DIP (Dependency Inversion Principle)** — depend on abstractions, not concrete details. (Ch 10)

**DRY (Don't Repeat Yourself)** — Hunt & Thomas; Beck's "Once, and only once." *"Duplication may be the root of all evil in software."* (Ch 3, Ch 12, [G5])

**DSL (Domain-Specific Language)** — a small language or API letting code read like prose a domain expert would write; minimizes the communication gap. (Ch 11)

**DTO (Data Transfer Object)** — a class with public variables and no functions; the quintessential data structure. (Ch 6)

**Feature Envy** — a method more interested in another class's data than its own. Sometimes a necessary evil. [G14] (Ch 6, Ch 17)

**F.I.R.S.T.** — Fast, Independent, Repeatable, Self-Validating, Timely. (Ch 9)

**Hybrid** — half object, half data structure. *"The worst of both worlds."* (Ch 6)

**Implicity** — (coined in Ch 2) the degree to which context is *not* explicit in the code itself. The enemy is implicity, not complexity.

**Learning Test** — a test written to explore a third-party API before integrating it. Free, and with positive ROI on every upgrade. (Ch 8)

**LeBlanc's Law** — *"Later equals never."* (Ch 1)

**Law of Demeter** — a method should only call methods of its own class, objects it creates, its arguments, and its instance variables. *"Talk to friends, not to strangers."* Applies to objects, not data structures. (Ch 6, [G36])

**Livelock** — threads in lockstep, each finding another "in the way," progressing nowhere at high CPU cost. (Ch 13)

**Magic Number** — any token whose value is not self-describing — including strings like `"John Doe"`. [G25] (Ch 17)

**Mental Mapping** — forcing readers to translate your name into the concept they already hold. (Ch 2)

**Mutual Exclusion** — only one thread may access a shared resource at a time. (Ch 13)

**Newspaper Metaphor** — a source file should read like a newspaper article: explanatory name, high-level concepts first, increasing detail downward. (Ch 5)

**Noise words** — `Info`, `Data`, `Object`, `Variable`, `Table`, `an`, `the` — words that differentiate names without differentiating meaning. (Ch 2)

**OCP (Open-Closed Principle)** — open for extension, closed for modification. (Ch 3, Ch 10)

**ONE SWITCH rule** — no more than one switch statement per type of selection, and its cases must create the polymorphic objects replacing all others. [G23] (Ch 17)

**POJO (Plain Old Java Object)** — a domain object with no dependency on enterprise frameworks; conceptually simpler and easier to test drive. (Ch 11, Ch 13)

**Principle of Least Surprise** — implement the behavior another programmer could reasonably expect; put code where a reader would naturally look for it. [G2], [G17] (Ch 17)

**Producer-Consumer** — one of the three fundamental concurrency models; producers and consumers signal each other across a bounded queue. (Ch 13)

**Readers-Writers** — concurrency model balancing throughput against staleness; naive priorities cause starvation on one side or throughput collapse on the other. (Ch 13)

**Rough draft** — working but unrefined code. Legitimate as a stage, professional suicide as a destination. (Ch 14)

**Selector Argument** — any argument (boolean, enum, int) used to select behavior. *"A lazy way to avoid splitting a large function."* [G15] (Ch 17)

**Special Case Pattern** — (Fowler) a class or object that encapsulates exceptional behavior so client code never handles it. (Ch 7)

**SRP (Single Responsibility Principle)** — one, and only one, reason to change. *"Often the most abused class design principle."* (Ch 10, Ch 13)

**Starvation** — a thread prohibited from proceeding for an excessively long time or forever. (Ch 13)

**Stepdown Rule** — every function is followed by those at the next level of abstraction, so the program reads top-down as a set of TO paragraphs. (Ch 3)

**Structure over Convention** — enforce design decisions with structures that compel compliance rather than naming conventions that merely request it. [G27] (Ch 17)

**Temporal Coupling** — an ordering requirement between calls. Necessary sometimes; hiding it is the defect. Expose it via a bucket brigade. [G31] (Ch 3, Ch 15, Ch 17)

**Template Method** — GoF pattern removing higher-level duplication where an algorithm differs by one step. (Ch 9, Ch 12)

**Three Laws of TDD** — no production code without a failing test; no more test than suffices to fail; no more production code than suffices to pass. (Ch 9)

**TO paragraph** — describing a function as *"TO \<Name\>, we do X, then Y"* to test whether it does one thing. From LOGO's `TO` keyword. (Ch 3)

**Train Wreck** — a chain of calls resembling coupled train cars: `a.getB().getC().doSomething()`. [G36] (Ch 6)

**Ubiquitous Language** — (Evans, *DDD*) a team's shared standard vocabulary, used extensively in the code. [N3] (Ch 17)

**Vertical Density / Openness** — related lines kept adjacent; separate thoughts separated by blank lines. (Ch 5)

**Wading** — the felt experience of slogging through bad code hunting for a clue. (Ch 1)
