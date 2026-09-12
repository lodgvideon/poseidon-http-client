# Glossary

**Abstract component** — a component containing nothing but interfaces, no executable code. *"A very common, and necessary, tactic when using statically typed languages like Java and C#"*; ideal dependency targets because they're maximally stable. (Ch 14)

**Abstract use case** — a use case that sets a general policy which other use cases flesh out; e.g. `View Catalog` inherited by `View Catalog as Viewer`/`as Purchaser`. (Ch 33)

**Abstractness (A)** — `A = Na / Nc`: abstract classes and interfaces divided by total classes in a component. Range [0, 1]. (Ch 14)

**ADP (Acyclic Dependencies Principle)** — *"Allow no cycles in the component dependency graph."* (Ch 14)

**App-titude test** — the low bar of "getting the app to work." Passing it is not architecture. (Ch 29)

**Architecture** — the shape given to a system: its division into components, their arrangement, and how they communicate. (Ch 15)

**Central Transform** — Page-Jones's name for the highest-level component, farthest from inputs and outputs. (Ch 19, Ch 25)

**CCP (Common Closure Principle)** — gather classes that change for the same reasons at the same times; separate those that don't. **SRP for components.** (Ch 13)

**Component** — *"the units of deployment… the smallest entities that can be deployed as part of a system"* — jars, gems, DLLs. Simon Brown's alternative: *"a grouping of related functionality behind a nice clean interface, which resides inside an execution environment."* (Ch 12, Ch 34)

**CRP (Common Reuse Principle)** — *"Don't force users of a component to depend on things they don't need."* **ISP for components.** (Ch 13)

**Critical Business Data** — data that would exist even if the system were not automated. (Ch 20)

**Critical Business Rules** — rules that make or save money *"irrespective of whether they were implemented on a computer."* (Ch 20)

**Decoupling mode** — source level, deployment level, or service level. Which is optimal changes over a project's life. (Ch 16)

**Dependency Rule** — **"Source code dependencies must point only inward, toward higher-level policies."** No inner circle may name anything in an outer circle. (Ch 22)

**Details** — everything needed to let humans, systems, and programmers communicate with the policy, but that doesn't affect the policy's behavior: IO devices, databases, web systems, servers, frameworks, protocols. (Ch 15)

**DIP (Dependency Inversion Principle)** — *"the most flexible systems are those in which source code dependencies refer only to abstractions, not to concretions"* — specifically, **volatile** concretions. (Ch 11)

**Distance (D)** — `D = |A + I − 1|`. 0 = on the Main Sequence; 1 = as far as possible. (Ch 14)

**Entity** — *(Jacobson)* an object embodying Critical Business Rules operating on Critical Business Data. *"Pure business and nothing else."* (Ch 20, Ch 22)

**Event sourcing** — store transactions, not state; derive state by replaying them. Makes applications **CR instead of CRUD**, eliminating concurrent update issues. (Ch 6)

**Fan-in / Fan-out** — incoming / outgoing dependencies of a component. *(Formerly Afferent/Efferent coupling, Ca/Ce.)* (Ch 14)

**Firmware** — **not** code stored in ROM. *"It is firmware because of what it depends on and how hard it is to change as hardware evolves."* (Ch 29)

**Fragile Tests Problem** — one change breaking hundreds or thousands of coupled tests, which then makes the system rigid because developers avoid the change. (Ch 28)

**HAL (Hardware Abstraction Layer)** — the boundary between software and firmware; its API is *"tailored to the software's needs"* — `Indicate_LowBattery()`, not `Led_TurnOn(5)`. (Ch 29)

**Humble Object** — the hard-to-test half of a split, *"stripped down to its barest essence."* Views, gateway implementations, and data mappers are humble objects. (Ch 23)

**Instability (I)** — `I = Fan-out / (Fan-in + Fan-out)`. 0 = maximally stable (responsible and independent); 1 = maximally unstable (irresponsible and dependent). (Ch 14)

**Interface Adapters** — the circle containing MVC, all SQL, and every conversion between inner and outer formats. (Ch 22)

**ISP (Interface Segregation Principle)** — avoid depending on things you don't use. (Ch 10)

**Kitty Problem** — a cross-cutting feature that forces changes in *every* service of a functional decomposition. (Ch 27)

**Level** — *"the distance from the inputs and outputs."* Farther from IO = higher level. (Ch 19)

**LSP (Liskov Substitution Principle)** — substituting a subtype must leave every program's behavior unchanged; extended to interfaces, duck typing, and REST services. (Ch 9)

**Main** — *"the ultimate detail — the lowest-level policy"*; creates factories and global facilities, then hands control inward. *"Think of `Main` as the dirtiest of all the dirty components."* (Ch 26)

**Main Sequence** — the line from (1, 0) to (0, 1) on the A/I graph, maximally distant from both zones of exclusion. (Ch 14)

**Morning after syndrome** — arriving to find your work broken because someone changed what you depend on. What ADP prevents. (Ch 14)

**OCP (Open-Closed Principle)** — *(Meyer, 1988)* *"A software artifact should be open for extension but closed for modification."* (Ch 8)

**ONE SWITCH rule** — at most one switch per selection type, creating polymorphic objects, hidden behind inheritance. (Clean Code [G23]; applied Ch 3, Ch 27)

**OSAL (Operating System Abstraction Layer)** — isolates software from the OS so a port means writing a new OSAL, not editing complex code. (Ch 29)

**Package by component** — bundling all responsibilities for one coarse-grained component into one package, exposing a single public interface. (Ch 34)

**Package by feature / by layer** — vertical vs. horizontal slicing. Brown: *"In my opinion, both are suboptimal."* (Ch 34)

**PAL (Processor Abstraction Layer)** — confines vendor C extensions and register access, making firmware above it testable off-target. (Ch 29)

**Partial boundary** — a placeholder for a future full boundary: skip-the-last-step, Strategy, or Facade. (Ch 24)

**Périphérique anti-pattern** — a single "infrastructure" source tree letting adapters call each other **around** the domain, like Paris's ring road. (Ch 34)

**Policy** — *"all the business rules and procedures. The policy is where the true value of the system lives."* (Ch 15)

**Ports and Adapters / Hexagonal Architecture** — *(Cockburn)* an "inside" of domain concepts and an "outside" of infrastructure; the outside depends on the inside. (Ch 22, Ch 34)

**Relaxed layered architecture** — layers skipping their adjacent neighbors (controller → repository). Can silently bypass authorization. (Ch 34)

**REP (Reuse/Release Equivalence Principle)** — *"The granule of reuse is the granule of release."* (Ch 13)

**SAP (Stable Abstractions Principle)** — *"A component should be as abstract as it is stable."* SDP + SAP = **DIP for components**. (Ch 14)

**SDP (Stable Dependencies Principle)** — *"Depend in the direction of stability"*; I metrics decrease along dependencies. (Ch 14)

**Screaming architecture** — a top-level structure that announces the domain ("HOME," "LIBRARY," "Health Care System"), not the framework. (Ch 21)

**Shape (vs. scope)** — change difficulty should be proportional to a request's **scope**, never to its **shape**. (Ch 2)

**SRP (Single Responsibility Principle)** — *"A module should be responsible to one, and only one, **actor**."* Not "do one thing." (Ch 7)

**Stability** — *"not easily moved"*; the work required to change something, driven by incoming dependencies — **not** change frequency. (Ch 14)

**Structural coupling** — a test class per production class, a test method per production method. *"One of the strongest, and most insidious, forms of test coupling."* (Ch 28)

**Target-hardware bottleneck** — when code can only be tested on the target, because it was structured without abstraction layers. (Ch 29)

**Testing API** — an API with *"superpowers"* to bypass security, skip expensive resources, and force testable states — decoupling test **structure** from application **structure**. (Ch 28)

**Use case** — *"a description of the way that an automated system is used"*: input, processing steps, output. *"Use cases control the dance of the Entities."* (Ch 20)

**Zone of Pain** — near (0, 0): stable **and** concrete. Harmful only when **volatile** — database schemas live here. (Ch 14)

**Zone of Uselessness** — near (1, 1): abstract with no dependents. *"A kind of detritus."* (Ch 14)
