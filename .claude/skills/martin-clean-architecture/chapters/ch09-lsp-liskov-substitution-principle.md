# Chapter 9: LSP — The Liskov Substitution Principle

*Part III: Design Principles*

## Core Idea
LSP began as a rule for inheritance; it has become a rule about **every interface and implementation**. **"A simple violation of substitutability can cause a system's architecture to be polluted with a significant amount of extra mechanisms."**

## Framework: Liskov's Definition (1988)

> *"What is wanted here is something like the following substitution property: If for each object `o1` of type `S` there is an object `o2` of type `T` such that for all programs `P` defined in terms of `T`, the behavior of `P` is unchanged when `o1` is substituted for `o2`, then `S` is a subtype of `T`."*
> — Barbara Liskov, "Data Abstraction and Hierarchy," *SIGPLAN Notices* 23, 5 (May 1988)

**The conforming case** (Fig 9.1): `License` with `calcFee()`, called by a `Billing` application; `PersonalLicense` and `BusinessLicense` use different fee algorithms. *"This design conforms to the LSP because the behavior of the `Billing` application does not depend, in any way, on which of the two subtypes it uses."*

## Worked Example: The Square/Rectangle Problem

The canonical violation (Fig 9.2):

```java
Rectangle r = …
r.setW(5);
r.setH(2);
assert(r.area() == 10);   // fails if the … produced a Square
```

**Why `Square` is not a proper subtype of `Rectangle`**: *"the height and width of the `Rectangle` are **independently mutable**; in contrast, the height and width of the `Square` must change together. Since the `User` believes it is communicating with a `Rectangle`, it could easily get confused."*

**The tell that confirms the violation** — look at what the defense requires:

> *"The only way to defend against this kind of LSP violation is to add mechanisms to the `User` (such as an `if` statement) that detects whether the `Rectangle` is, in fact, a `Square`. **Since the behavior of the `User` depends on the types it uses, those types are not substitutable.**"*

## Framework: LSP Extended to Architecture

*"In the early years of the object-oriented revolution, we thought of the LSP as a way to guide the use of inheritance… However, over the years the LSP has morphed into a broader principle of software design that pertains to interfaces and implementations."*

The interfaces in question take many forms:
- A Java-style interface implemented by several classes
- Several Ruby classes sharing the same method signatures
- **A set of services that all respond to the same REST interface**

*"In all of these situations, and more, the LSP is applicable because there are users who depend on well-defined interfaces, and on the substitutability of the implementations of those interfaces."*

## Worked Example: The Taxi Dispatch Aggregator

**The system**: an aggregator across many taxi dispatch services. Customers pick the most appropriate taxi regardless of company; the system dispatches via a RESTful service whose URI comes from the driver database.

Driver Bob's dispatch URI is `purplecab.com/driver/Bob`, and the system PUTs:

```
purplecab.com/driver/Bob
    /pickupAddress/24 Maple St.
    /pickupTime/153
    /destination/ORD
```

*"Clearly, this means that all the dispatch services, for all the different companies, must conform to the same REST interface. They must treat the `pickupAddress`, `pickupTime`, and `destination` fields identically."*

**The violation**: Acme's programmers *"didn't read the spec very carefully"* and abbreviated `destination` to **`dest`**. And Acme cannot simply be dropped — *"Acme is the largest taxi company in our area, and Acme's CEO's ex-wife is our CEO's new wife, and … Well, you get the picture."*

**The naive fix, and why it's rejected:**

```java
if (driver.getDispatchUri().startsWith("acme.com"))…
```

> *"No architect worth his or her salt would allow such a construction to exist in the system. Putting the word 'acme' into the code itself creates an opportunity for all kinds of horrible and mysterious errors, **not to mention security breaches**."*

**And it doesn't stop**: what if Acme buys Purple Taxi, keeps both brands and websites, but unifies the systems? *"Would we have to add another `if` statement for 'purple'?"*

**The actual cost** — a configuration-driven dispatch command creation module, keyed by dispatch URI:

| URI | Dispatch Format |
|---|---|
| `Acme.com` | `/pickupAddress/%s/pickupTime/%s/dest/%s` |
| `*.*` | `/pickupAddress/%s/pickupTime/%s/destination/%s` |

> *"And so our architect has had to add a **significant and complex mechanism** to deal with the fact that the interfaces of the restful services are not all substitutable."*

**The lesson to carry**: one misspelled field in one partner's API bought a configuration database, a lookup path, and a format engine — permanently.

## Mental Models
- **Test substitutability by looking at the caller, not the type.** If users need an `if` to distinguish implementations, the implementations are not substitutable — regardless of what the type hierarchy claims.
- **Every substitutability violation gets paid for in mechanism.** The question is never "can we handle it" but "what permanent complexity does handling it install?"
- **Hard-coded vendor names are a smell with teeth.** They invite mysterious errors *and* security breaches, and they multiply on every merger or rebrand.
- **When you must handle a non-conforming implementation, push the variation into data.** A configuration table is bad; scattered `if`s are worse.

## Anti-patterns
- **`Square extends Rectangle`** — when a subtype tightens an invariant the supertype leaves free.
- **`startsWith("acme.com")`** — special-casing a partner by name inside business logic.
- **Assuming "same REST shape" without enforcement** — nothing in the type system stops a partner from shipping `dest`.

## Key Takeaways
1. LSP defines subtyping behaviorally: substituting `o1` for `o2` must leave every program's behavior unchanged.
2. `Square`/`Rectangle` fails because independently mutable dimensions become jointly mutable.
3. If the user needs type-detection logic, substitutability is already broken.
4. LSP applies to Java interfaces, duck-typed classes, **and REST services alike**.
5. Architectural LSP violations are paid for in permanent extra mechanism — configuration databases, format tables, lookup paths.
6. Never encode a specific vendor's name in the code; drive the variation from data if you must accept it at all.

## Connects To
- **Ch 8 (OCP)**: substitutable implementations are what make extension-without-modification possible.
- **Ch 11 (DIP)**: depending on abstractions only works if the concretions are truly substitutable.
- **Ch 27 (Services: Great and Small)**: cross-service interfaces face this problem at scale.
- **Ch 34 (The Missing Chapter)**: the practical consequences of partner interfaces that don't conform.
- **Clean Code Ch 6**: Liskov is implicit in the object/data-structure dichotomy.
