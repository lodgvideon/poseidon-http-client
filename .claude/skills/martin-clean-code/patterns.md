# Patterns & Techniques

Concrete, repeatable techniques from the book. Format: when to use → how → trade-offs.

---

## Extract Till You Drop
**When to use**: any function longer than ~20 lines, or with indent level > 2.
**How**: extract blocks inside `if`/`while` into named functions until each block is one line — usually a call. Stop when extracting would only restate the implementation [G34].
**Trade-offs**: more functions and more names to choose; the payoff is that each extracted name documents the block for free.

## The TO-Paragraph Test
**When to use**: deciding whether a function does "one thing."
**How**: say *"TO \<FunctionName\>, we do X, then Y, then Z."* If every step is exactly one level below the name, it does one thing. Cross-checks: can you extract a function whose name isn't a restatement? can the body be divided into sections?
**Trade-offs**: none — it's a reading exercise.

## Bury the Switch in an Abstract Factory
**When to use**: a `switch`/`if-else` chain on a type code, especially if the same shape will recur in other functions.
**How**: create an abstract base with the polymorphic operations, an interface factory, and one implementation whose `switch` creates derivatives. Apply the **ONE SWITCH rule**: one switch per selection type, hidden behind inheritance [G23].
**Trade-offs**: correct when new *types* are likelier than new *functions*; if new *operations* dominate, a procedural data-structure design is the better fit (Data/Object Anti-Symmetry).

## Argument Object
**When to use**: a function needs more than two or three arguments, or the same variables travel together.
**How**: `makeCircle(double x, double y, double radius)` → `makeCircle(Point center, double radius)`. Variables passed together are a concept deserving a name.
**Trade-offs**: none real — *"it may seem like cheating, but it's not."*

## Reduce Arity by Relocation
**When to use**: an unavoidable dyad like `writeField(outputStream, name)`.
**How**: three options — make the function a member of one argument (`outputStream.writeField(name)`); make the argument a field of the current class; or extract a class taking it in the constructor (`FieldWriter`).
**Trade-offs**: introduces a class or a field; usually worth it, because ignorable arguments are where bugs hide.

## Split on the Flag
**When to use**: any boolean or selector argument.
**How**: `render(boolean isSuite)` → `renderForSuite()` + `renderForSingleTest()`. Applies to enums and ints too [G15].
**Trade-offs**: more functions; *"in general it is better to have many functions than to pass some code into a function to select the behavior."*

## Extract Try/Catch Blocks
**When to use**: any function containing error handling mixed with logic.
**How**: pull the `try` body and the `catch` body into their own functions, leaving a function that is purely about error processing. Enforce: if `try` appears, it is the **first word**, and nothing follows the `catch`/`finally`.
**Trade-offs**: two extra functions; you gain the ability to read either concern while ignoring the other.

## Wrap the Third-Party API
**When to use**: any dependency you don't control — libraries, frameworks, boundary interfaces like `Map`.
**How**: define the interface you want; wrap or adapt the real one; translate its exceptions into one of your own (`ACMEPort` → `LocalPort`, `PortDeviceFailure`). Keep the boundary interface inside one class or close family — never in public APIs.
**Trade-offs**: an extra layer. Buys switchable dependencies, easy mocking, freedom from vendor API choices, and one place to change.

## Learning Tests
**When to use**: before integrating any unfamiliar library.
**How**: write tests calling the API exactly as you intend to use it; keep them in the suite and re-run on every upgrade.
**Trade-offs**: **free** — you had to learn the API anyway — with positive ROI: incompatibilities in new releases surface immediately.

## Define the Interface You Wish You Had
**When to use**: the code you depend on doesn't exist yet, or its owners haven't designed it.
**How**: write your own interface from what you actually need to say; write an **Adapter** when the real API lands; test with a fake in the meantime.
**Trade-offs**: an adapter to maintain; buys unblocked progress and a testing seam.

## Special Case Pattern *(Fowler)*
**When to use**: an "exceptional" branch is really a normal business rule (missing meal expenses → per diem).
**How**: return a class that encapsulates the special behavior (`PerDiemMealExpenses`) instead of throwing or returning null.
**Trade-offs**: an extra class; removes conditionals from every call site.

## Null Object / Empty Collection
**When to use**: any method tempted to return null.
**How**: return `Collections.emptyList()` or a special-case object. When a third-party API returns null, wrap it. Forbid *passing* null by policy so a null argument is itself the defect.
**Trade-offs**: none meaningful — *"the problem is not a missing null check, it's that there are too many."*

## Domain-Specific Testing Language
**When to use**: test code cluttered with setup detail and duplication.
**How**: refactor tests into helpers (`makePages`, `submitRequest`, `assertResponseIsXML`) that form a testing API. **Never design it up front** — let it emerge from refactoring.
**Trade-offs**: helper code to maintain, at a *dual standard* (may be inefficient, never unclean).

## Given-When-Then Naming
**When to use**: when you want each test to reach a single conclusion.
**How**: `givenPages(...)`, `whenRequestIsIssued(...)`, `thenResponseShouldBeXML()`.
**Trade-offs**: splitting for one-assert-per-test duplicates given/when. Template Method or `@Before` can fix it — Martin judges both *"too much mechanism for such a minor issue."* Prefer **one concept per test**.

## Dependency Injection at the Seam
**When to use**: a class depends on something slow, remote, or nondeterministic (`TokyoStockExchange`).
**How**: extract an interface (`StockExchange`), take it as a constructor argument, inject a stub in tests.
**Trade-offs**: one more interface. Buys testability, reuse, and isolation from change — DIP in practice.

## Separate Construction from Use
**When to use**: any lazy-initialization idiom (`if (service == null) service = new MyServiceImpl(...)`).
**How**: move all construction to `main` or a DI container; make every dependency arrow point away from `main`. Use an **Abstract Factory** when the application must control *when* an object is created.
**Trade-offs**: startup wiring becomes explicit and central; you give up "convenient" local construction — which is the point.

## Template Method
**When to use**: two methods share an algorithm and differ in one step (US vs EU vacation accrual).
**How**: abstract base holds the algorithm and calls an abstract hook; subclasses fill the hole.
**Trade-offs**: inheritance coupling. Use **Strategy** when composition fits better [G5].

## Explanatory Variables
**When to use**: any dense expression or regex-group access.
**How**: name intermediate values — `String key = match.group(1); String value = match.group(2);`
**Trade-offs**: more lines. *"It is hard to overdo this. More explanatory variables are generally better than fewer."*

## Encapsulate Conditionals & Boundary Conditions
**When to use**: compound booleans, and any `level + 1` appearing twice.
**How**: `if (shouldBeDeleted(timer))`; `int nextLevel = level + 1;`. Prefer positives: `shouldCompact()` over `!shouldNotCompact()` [G28, G29, G33].
**Trade-offs**: none.

## Bucket Brigade (Expose Temporal Coupling)
**When to use**: functions that must be called in order.
**How**: have each produce what the next consumes, so out-of-order calls don't compile. Naming the caller after the sequence (`findCommonPrefixAndSuffix` calling `findCommonPrefix`) also works — and survives someone "cleaning up" an arbitrary parameter.
**Trade-offs**: *"extra syntactic complexity exposes the true temporal complexity of the situation."*

## Jiggling (Force Threading Failures)
**When to use**: testing concurrent code whose bugs appear once in thousands of runs.
**How**: insert `ThreadJigglePoint.jiggle()` calls; use a no-op implementation in production and a randomized sleep/yield/pass-through in tests; run the suite hundreds of times. IBM's **ConTest** automates this.
**Trade-offs**: probabilistic, not proof — *"at least you can say you've done due diligence."*

## Break One Deadlock Condition
**When to use**: any system with multiple finite resource pools.
**How**: pick one of mutual exclusion / lock & wait / no preemption / circular wait and break it. **Global resource ordering** (breaking circular wait) is usually cheapest — a convention, not a mechanism.
**Trade-offs**: TANSTAAFL — each choice costs starvation, livelock, CPU, or longer-held locks.

## Nonblocking Update (CAS)
**When to use**: shared counters and flags currently guarded by `synchronized`.
**How**: `AtomicInteger` + `incrementAndGet()`.
**Trade-offs**: an object instead of a primitive; *"the cases where it will be slower are virtually nonexistent."*
