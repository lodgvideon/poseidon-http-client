# Cheatsheet — Uncle Bob's Architectural Decisions

## The One Rule

> **Source code dependencies must point only inward, toward higher-level policies.**
> No inner circle may **name** anything in an outer circle — functions, classes, variables, or **data formats**.

## Formulas & Thresholds

| Metric | Formula | Read it as |
|---|---|---|
| **I** (Instability) | `Fan-out / (Fan-in + Fan-out)` | 0 = stable (responsible, independent); 1 = unstable (irresponsible, dependent) |
| **A** (Abstractness) | `Na / Nc` | 0 = no abstract types; 1 = nothing but abstract types |
| **D** (Distance) | `\|A + I − 1\|` | 0 = on the Main Sequence; investigate anything **not near 0** |
| **SDP check** | I must **decrease** along each dependency | An arrow pointing "up" (to lower I) is a violation |
| **Control limit** | D ≈ **0.1**, or 1 std deviation from the mean | Worth investigating |
| **Level** | Distance from inputs/outputs | Farther = higher = more protected |

## Decision Rules

- **If A must be protected from changes in B → make B depend on A.** That's the whole direction question.
- **If the flow of control must go outward → use an output port** (interface in the inner circle, implemented outside).
- **If you're about to pass a database row or an Entity across a boundary → stop.** Convert to a structure convenient for the *inner* circle.
- **If a use case reveals the delivery mechanism → the boundary has leaked.** You shouldn't be able to tell web from console from thick client.
- **If inner code formats a `Date` or `Currency` for display → move it to the Presenter.**
- **If you need an `if` to distinguish two implementations → they aren't substitutable** (LSP violation), and you will pay for it in permanent mechanism.
- **If a change serving one actor can break another actor → SRP violation.** Split by actor.
- **If two methods are deduplicated across actor boundaries → expect the accidental-duplication bug.**
- **If a unit test drags in half the system → look for a cycle**, not a bad test.
- **If a "flexible" component became hard to change → someone hung a dependency on it.** Break it with DIP.
- **If the same `#ifdef` appears thousands of times → you're missing an abstraction layer.**
- **If a framework wants your Entities to inherit from it → derive proxies instead.**
- **If a DI framework annotation appears outside `Main` → it has crossed a boundary it shouldn't.**
- **If a technology is demanded for non-engineering reasons → bolt it on the side** behind a narrow channel; don't fight it on merit alone.
- **If two code sections look alike → ask whether they change at the same rate for the same reasons.** If not, it's accidental duplication — leave them apart.
- **If every type is `public` → your packages are folders, not encapsulation**, and all four organization styles collapse into one.

## Decision Tree: Where Does This Code Go?

- Would the rule make/save money **even if executed by a clerk with an abacus**?
  - → **Entity** (innermost). Pure business; no DB, UI, or framework.
- Does it only make sense **because a computer is involved** — sequencing, validating, constraining the flow?
  - → **Use Case**. It controls the dance of the Entities; it must not reveal delivery.
- Does it **convert** between inner formats and an external agency?
  - → **Interface Adapters**. All MVC lives here. **All SQL lives here.**
- Is it a framework, driver, database, or web machinery?
  - → **Frameworks & Drivers** (outermost). Glue code only.
- Is it construction, wiring, config, or literals?
  - → **`Main`** — *"the dirtiest of all the dirty components."*

## Decision Tree: Which Decoupling Mode?

1. **Start at source level** (monolith with disciplined internal boundaries). Function calls, very cheap, can be chatty.
2. **Move to deployment level** (jars/DLLs) *when deployment or development friction appears.* Still function calls.
3. **Move to service level** *only when operational needs demand it.* Network latency: tens of ms to seconds; avoid chattiness.
4. **Keep the reverse path open** — a good architecture allows sliding back down to a monolith.

> Martin's preference: *"Push the decoupling to the point where a service **could** be formed, should it become necessary; but then leave the components in the same address space as long as possible."*

**Against service-by-default**: expensive, encourages coarse-grained decoupling, and *"dealing with service boundaries where none are needed is a waste of effort, memory, and cycles. And, yes, I know that the last two are cheap — but the first is not."*

## Decision Tree: Full, Partial, or No Boundary?

- **Full** — reciprocal interfaces + I/O structures + independent components. Expensive to build *and* maintain.
- **Skip the last step** — full code, single deployment. No release-management burden; decays quietly.
- **Strategy** — one-directional inversion. Cheap; backchannels prevented only by discipline.
- **Facade** — no inversion; clients get transitive dependencies and recompile.
- **None** — cheapest now, *"very expensive to add in later — even in the presence of comprehensive test-suites and refactoring discipline."*

> **The rule**: implement *"right at the inflection point where the cost of implementing becomes less than the cost of ignoring."* Watch for **the first inkling of friction**; review frequently.

## Trade-off Matrix: Component Cohesion (the tension triangle)

| Principle | Force | Sacrifice if ignored |
|---|---|---|
| **REP** — granule of reuse = granule of release | Inclusive (bigger) | Users can't consume your component |
| **CCP** — gather what changes together | Inclusive (bigger) | *"Too many components impacted when simple changes are made"* |
| **CRP** — don't force unneeded dependencies | **Exclusive (smaller)** | *"Too many unneeded releases are generated"* |

**Position shifts over time**: projects start on the **CCP side** (developability beats reuse) and slide toward **REP** as others begin consuming them.

## A/I Graph: Zones of Exclusion

| Zone | Position | Inhabitants | Verdict |
|---|---|---|---|
| **Zone of Pain** | (0, 0) — stable + concrete | **Database schemas** (volatile → maximum pain); `String` (nonvolatile → harmless) | Rigid: can't extend, can't change. **Only volatile components hurt here** |
| **Zone of Uselessness** | (1, 1) — abstract + unwanted | *"Leftover abstract classes that no one ever implemented"* | Detritus |
| **Main Sequence** | the line between (1,0) and (0,1) | Aim here; prefer the **endpoints** | *"Depended on to the extent that it is abstract; depends on others to the extent that it is concrete"* |

## Tells & Smells

| If you see… | You probably have… |
|---|---|
| A controller injecting a repository directly | Relaxed layering — possibly bypassing authorization |
| Every type marked `public` | Packages used for organization, not encapsulation |
| Test class per production class, method per method | **Structural coupling** — refactoring is now a two-sided edit |
| Tests that navigate the login screen to check business rules | Guaranteed fragility; the GUI is the most volatile thing you have |
| `@autowired` in a business object | A framework in the innermost circle; the wedding ring is on |
| An ORM row structure passed inward | Dependency Rule violation, and the most common one |
| `writeChar(translate(readChar()))` | High-level policy depending on IO — correct code, wrong architecture |
| A team asking "where are the views and controllers?" | **A good sign** — the delivery mechanism didn't colonize the structure |
| A top-level package tree naming Rails/Spring/ASP | An architecture that screams the framework, not the domain |
| Man-years of work demanded in a month | Time to re-examine the *requirement* (the pCCU was a week) |
| A tiger team rewriting while others maintain | A race the rewrite will lose |
| A "temporary" fork that will be reintegrated later | Permanent divergence — three attempts failed at 4-TEL |
| A framework that fits early and fights you later | The asymmetric marriage, on schedule |

## Order of Operations

- **New system**: identify **actors** → use cases → components per actor **and** per role → build finely, deploy coarsely.
- **Legacy code**: cover it → **make it work** → **then make it right.**
- **New framework**: date it, don't marry it. Keep it behind a boundary as long as possible.
- **Any detail decision** (DB, web, framework, DI): defer. *"A good architect maximizes the number of decisions not made."* If it's already been decided for you — **pretend it hasn't**.
