# Cheatsheet — Uncle Bob's Decisions

## Thresholds & Defaults

| Thing | Martin's number |
|---|---|
| Function length | *"hardly ever 20 lines"*; 2–4 lines is the target |
| Indent level in a function | **≤ 2** |
| Function arguments | 0 ideal, 1 good, 2 costly, **3 avoid**, 4+ needs "very special justification — and then shouldn't be used anyway" |
| Source file length | ~200 lines typical, **500 upper limit** (FitNesse: 50k lines total, avg file 65) |
| Line width | 80 arbitrary, 100–120 fine, **120 is Martin's limit**, beyond that "careless" |
| Class description | ≤ **25 words**, no "if"/"and"/"or"/"but" |
| Test coverage | 50% = "patchwork quilt"; 92% = "pretty good" |
| TDD cycle length | ~**30 seconds** |
| Switches per selection type | **1** (ONE SWITCH rule) |

## Decision Rules

- **If a name needs a comment → change the name**, not the comment.
- **If you're about to write a comment → try to express it in code first.** Comments are always failures.
- **If you want to comment out code → delete it.** Source control remembers.
- **If you want to mark a closing brace → shorten the function.**
- **If a list needs column alignment → the list is too long.** Split the class.
- **If a function takes a boolean → split it into two functions.**
- **If a function has an output argument → make it a method on that object instead.**
- **If a function both changes state and returns info → split it** (Command Query Separation).
- **If `try` appears in a function → it must be the first word**, and nothing follows `catch`/`finally`.
- **If you're tempted to return null → return a Special Case object or throw.** If a third-party API returns null, wrap it.
- **If you're passing null → stop. Forbid it by policy** so a null argument becomes the defect itself.
- **If you can't name a class concisely → it's too large.** `Manager`/`Processor`/`Super` in a name = aggregated responsibilities.
- **If a subset of variables is used by a subset of methods → a class is trying to escape.** Split it.
- **If you find yourself opening a class to modify it → consider fixing the design** (OCP). If you won't need the feature soon, leave it alone.
- **If you find one bug in a function → test that function exhaustively.** Bugs congregate.
- **If a test fails intermittently → treat it as a threading defect.** One-offs do not exist.
- **If adding a debug statement makes a deadlock disappear → it is not fixed.**
- **If two functions must be called in order → make the first produce what the second consumes.**
- **If the same `level + 1` appears twice → name it** `nextLevel`.

## Decision Tree: Object or Data Structure?

- Will you add new **types** more often than new operations?
  - → **Objects**: hide data, expose polymorphic behavior. Law of Demeter applies.
- Will you add new **operations** more often than new types?
  - → **Data structures + procedures**: expose data, no behavior. Demeter does not apply.
- Both, or unsure?
  - → Pick one and commit. **Never build a hybrid** — it inherits the costs of both and the benefits of neither.

## Decision Tree: I See a `switch`

1. Is it selecting on a **type code**? → Bury it in an Abstract Factory; dispatch polymorphically.
2. Is this the **only** switch for that selection, creating polymorphic objects, hidden behind inheritance? → Tolerable.
3. Are new *functions* genuinely more likely than new *types*? → Keep it; you're in procedural territory by design.
4. Otherwise → **suspect it.** *"Most people use switch statements because it's the obvious brute force solution."*

## Decision Tree: Deadlock

Break exactly **one** of the four conditions:
- **Circular wait** → global resource ordering. *Cheapest — a convention, not a mechanism.* **Start here.**
- **Mutual exclusion** → `AtomicInteger`, or more resources than threads.
- **Lock & wait** → release everything and restart on contention. *Risks starvation and livelock; always implementable as a last resort.*
- **No preemption** → request mechanism to reclaim resources. *Request bookkeeping gets tricky.*

## Trade-off Matrix: Comment Types

| Comment | Keep? | Condition |
|---|---|---|
| Legal / license | ✅ | Refer to external license; let the IDE fold it |
| Warning of consequences | ✅ | Prevents a plausible "optimization" into a bug |
| Explanation of intent | ✅ | Documents a decision the code can't state |
| TODO | ⚠️ | Explains a degenerate implementation; scan and delete regularly |
| Clarification | ⚠️ | Only for code you cannot change; high risk of being wrong |
| Informative | ⚠️ | Usually a rename or a new class removes the need |
| Javadoc (public API) | ✅ | Genuinely valuable — and can lie like any other comment |
| Javadoc (internal) | ❌ | "Anathema" |
| Redundant / noise / mumbling | ❌ | — |
| Journal / changelog / byline | ❌ | Source control's job |
| Commented-out code | ❌ | "An abomination" |
| HTML in comments | ❌ | The tool's job, not yours |

## Tells & Smells — Fast Recognition

| If you see… | You probably have… |
|---|---|
| A function divided into commented sections | A function doing many things (and a class hiding inside) |
| The same shape of `if/else` in several modules | Missing polymorphism [G5] |
| Scattered `+1`s and `-1`s | A mis-based variable; fix the convention, not the sites |
| `getX()` that also creates X | A name hiding a side effect [N7] |
| A dangling `false` at a call site | A selector argument [G15] |
| `a.getB().getC().doSomething()` | A train wreck; ask what the caller actually wanted |
| A method reaching through another object's accessors | Feature Envy — sometimes necessary |
| An always-false `if` / never-executed line | A wrong algorithm, not dead weight |
| A function repaired for bugs before | The next bug's location [T6] |
| Tests failing only above N characters | A boundary condition [T5, T7] |
| Constants declared "somewhere convenient" | Artificial coupling [G13] / misplaced responsibility [G17] |
| A base class naming its derivatives | Broken abstraction [G7] |
| An interface holding constants, inherited for access | *"Hideous"* — use `import static` [J2] |
| `percentFull()` on a general `Stack` | Wrong level of abstraction — and you cannot fake your way out |

## Order of Operations

**Simple Design, in priority order** (conflicts resolve toward the top):
1. Runs all the tests
2. Contains no duplication
3. Expresses the intent of the programmer
4. Minimizes the number of classes and methods

**Working on legacy code**: cover it → make it work → *then* make it right.
**Writing new code**: write it dirty under test → refine → never ship the rough draft.
