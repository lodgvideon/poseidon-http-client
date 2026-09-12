# Patterns & Techniques

Concrete, repeatable techniques from the book. Format: when to use → how → trade-offs.

---

## Invert a Dependency with an Interface
**When to use**: any time a source dependency points the wrong way — high-level code naming low-level code.
**How**: define the interface **in the caller's (higher-level) component**, implement it in the callee's. Control still flows outward; the source dependency now points inward.
**Trade-offs**: one more type. This is the single most-used technique in the book — everything else is a variation on it.

## Abstract Factory for Volatile Creation
**When to use**: high-level policy must *create* a volatile concrete object without naming it.
**How**: `Application` uses `Service`; to build one it calls `makeSvc` on a `ServiceFactory` interface; `ServiceFactoryImpl` instantiates `ConcreteImpl` and returns it as a `Service`. The curved line between them is the architectural boundary.
**Trade-offs**: an extra interface plus implementation; the only way to keep `new` out of policy code.

## Gather DIP Violations into Main
**When to use**: always — *"DIP violations cannot be entirely removed."*
**How**: put all construction, wiring, factories, global facilities, and configuration strings in `Main`. Let the DI framework inject **into `Main` only**; distribute normally from there.
**Trade-offs**: `Main` is deliberately dirty. Accept it, and keep multiple `Main`s (Dev/Test/Prod, per country, per customer) as plugins.

## Output Port (Crossing Against Control Flow)
**When to use**: an inner-circle use case must invoke an outer-circle presenter or gateway.
**How**: the use case calls an interface **declared in the inner circle** ("use case output port"); the outer-circle class implements it.
**Trade-offs**: an interface per crossing. This is what makes the Dependency Rule survivable when control flows outward.

## Humble Object Split
**When to use**: any behavior that is hard to test — GUIs, database access, service I/O.
**How**: split into a **humble** part reduced to trivial data movement, and a **testable** part holding all logic. View/Presenter, gateway/interactor, data mapper/gateway.
**Trade-offs**: two classes instead of one; the split usually *is* an architectural boundary, so you gain a line you wanted anyway.

## Presenter → ViewModel
**When to use**: whenever the UI must display business data.
**How**: the Presenter converts `Date`, `Currency`, and other business types into **Strings, booleans, and enums** in a ViewModel — including button names and gray-out flags. The View only moves data to the screen.
**Trade-offs**: an extra data structure and a copy. Buys a View trivial enough to be untested.

## Database Gateway
**When to use**: any persistence access from use cases.
**How**: a polymorphic interface with a method per operation, named in domain terms — `getLastNamesOfUsersWhoLoggedInAfter(Date)`. Implementations (the humble objects) live in the database layer and contain **all** the SQL.
**Trade-offs**: no ad-hoc queries from the interactor; in exchange, interactors are testable with stubs and the database becomes swappable.

## Simple Data Structures Across Boundaries
**When to use**: every boundary crossing.
**How**: pass structs, DTOs, function arguments, or hashmaps — *"always in the form that is most convenient for the inner circle."*
**Trade-offs**: copying. **Never** pass Entity objects or database row structures inward; that forces an inner circle to know an outer one.

## Request/Response Models
**When to use**: every use case's input and output.
**How**: plain structures with no dependencies — never `HttpRequest`/`HttpResponse`, never references to Entities.
**Trade-offs**: duplication that *looks* accidental. It isn't: *"over time they will change for very different reasons."*

## Break a Dependency Cycle
**When to use**: any cycle in the component dependency graph (symptom: a unit test drags in half the system).
**How**: two options — (1) **apply DIP**: put an interface in the upstream component and inherit it downstream; (2) **extract a new component** both parties depend on.
**Trade-offs**: option 2 grows the component count and makes the structure "jitter" — monitor continuously.

## Measure the Component Structure
**When to use**: periodically, and when a component feels wrong.
**How**: compute `I = Fan-out/(Fan-in+Fan-out)`, `A = Na/Nc`, `D = |A+I−1|`. Plot D per component; set a control threshold (e.g. 0.1); watch D over releases.
**Trade-offs**: *"A metric is not a god; it is merely a measurement against an arbitrary standard."*

## Partial Boundary — Skip the Last Step
**When to use**: you want a boundary's option without multi-component administration.
**How**: build the reciprocal interfaces and I/O structures fully, then compile and deploy as one component.
**Trade-offs**: full code cost, no release-management cost. **Decays** if the split never happens (FitNesse's web/wiki).

## Partial Boundary — Strategy or Facade
**When to use**: cheaper placeholders.
**How**: **Strategy** — one-directional interface, dependency inversion in one direction only. **Facade** — a class listing services, no inversion at all.
**Trade-offs**: Strategy's separation is prevented from decaying only by discipline; Facade gives clients transitive dependencies that force recompilation.

## The Testing API
**When to use**: any system whose business rules are currently verified through the GUI.
**How**: build an API with superpowers — bypass security, skip databases, force testable states. Route all tests through it so test *structure* is decoupled from application *structure*.
**Trade-offs**: keep the dangerous parts in a separately deployable component so they never ship to production.

## Enforce Boundaries with the Compiler
**When to use**: monolithic applications in languages with package-level access control.
**How**: expose exactly **one public type per component** (`OrdersComponent`); mark everything else package protected. In .NET use `internal` with one assembly per component.
**Trade-offs**: more thought per declaration. Replaces discipline and post-compile static analysis with compile-time enforcement.

## Component-Based Services
**When to use**: services that must absorb cross-cutting features without full redeployment.
**How**: ship a service as abstract classes in jar files; deliver each new feature as another jar whose classes extend them; deploy by **adding jars to the load path**.
**Trade-offs**: requires designing the service internals to the Dependency Rule — but makes feature addition OCP-compliant.

## Segregate Mutability
**When to use**: any system facing concurrency.
**How**: split into immutable components and a small mutable core; protect the mutable state with transactional memory (compare-and-swap, e.g. Clojure's `atom`/`swap!`). *"Push as much processing as possible into the immutable components."*
**Trade-offs**: `atom`-style facilities *"cannot completely safeguard against concurrent updates and deadlocks when multiple dependent variables come into play."*

## Event Sourcing
**When to use**: when storage and CPU are cheap relative to the correctness benefit.
**How**: store transactions, not state; replay to derive state; snapshot periodically (e.g. nightly).
**Trade-offs**: unbounded growth without snapshots. Buys **CR instead of CRUD** — and therefore no concurrent update issues at all.

## Hardware / OS / Processor Abstraction Layers
**When to use**: embedded systems, and anywhere platform details are spreading.
**How**: **HAL** tailored to the application's vocabulary (`Indicate_LowBattery()`, name/value pairs — not GPIO bits or flash bytes); **PAL** confining vendor C extensions and register access; **OSAL** wrapping RTOS calls.
**Trade-offs**: layers to write and maintain. Buys off-target testability, which removes the target-hardware bottleneck.

## Replace Mass Conditional Compilation
**When to use**: `#ifdef BOARD_V2` appearing more than a handful of times.
**How**: make hardware type a detail hidden under the HAL; select implementations with the **linker or runtime binding**.
**Trade-offs**: an abstraction layer instead of a build flag. *"If I see `#ifdef BOARD_V2` once, it's not really a problem. Six thousand times is an extreme problem."*

## Bolt the Required Technology on the Side
**When to use**: a database, framework, or standard is demanded for non-engineering reasons.
**How**: satisfy the requirement at the boundary with a **narrow and safe data access channel**; keep the core's own structures intact.
**Trade-offs**: some duplication of storage. The alternative — arguing on engineering merit alone — is what Martin did, and he lost.

## Defer the Detail with a Stub Ladder
**When to use**: at project start, for database, web server, and framework decisions.
**How**: put an interface between the policy and the detail, then climb: **Mock → In-memory → File system → real technology**, moving only when features demand it. (FitNesse: `MockWikiPage` → `InMemoryPage` → `FileSystemWikiPage`; MySQL deferred *"into nonexistence."*)
**Trade-offs**: none meaningful — and 18 months of fast tests along the way.

## Derive Proxies Instead of Marrying a Framework
**When to use**: a framework demands you inherit from its base classes in your business objects.
**How**: say no. Derive **proxies** in a plugin component; keep Entities free of framework types and annotations.
**Trade-offs**: proxy boilerplate. *"Perhaps you can find a way to get the milk without buying the cow."*
