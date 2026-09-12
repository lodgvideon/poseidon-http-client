# Chapter 34: The Missing Chapter

*by Simon Brown — Part VI: Details*

## Core Idea
**"Your best design intentions can be destroyed in a flash if you don't consider the intricacies of the implementation strategy."** Four code-organization styles are conceptually different but **syntactically identical if every type is `public`** — so **use the compiler to enforce your architecture**.

## Reference Table: Four Ways to Organize Code

Running example throughout: an online book store, use case *"customers being able to view the status of their orders."* Java types: `OrdersController`, `OrdersService`, `OrdersServiceImpl`, `OrdersRepository`, `JdbcOrdersRepository`.

| Style | Slicing | What it gives you | What it costs |
|---|---|---|---|
| **Package by layer** | **Horizontal** — web / business logic / persistence, *"grouped by what it does from a technical perspective"* | Fowler: *"a good way to get started"*; *"a very quick way to get something up and running"* | *"Once your software grows in scale and complexity, you will quickly find that **having three large buckets of code isn't sufficient**."* And *"a layered architecture **doesn't scream anything about the business domain** — put two layered architectures from very different business domains side by side and they will look **eerily similar**"* |
| **Package by feature** | **Vertical** — by *"related features, domain concepts, or aggregate roots"*, all types in one package named for the concept | *"The top-level organization of the code now **screams something about the business domain**… this code base has something to do with **orders** rather than the web, services, and repositories."* Easier to find everything a use-case change touches | *"In my opinion, **both are suboptimal**"* |
| **Ports and adapters** | **Inside / outside** — *"the 'inside' contains all of the domain concepts, whereas the 'outside' contains the interactions with the outside world"* | *"The major rule here is that **the 'outside' depends on the 'inside' — never the other way around**."* Note the rename: `OrdersRepository` → **`Orders`**, because *"the naming of everything on the 'inside' should be stated in terms of the **ubiquitous domain language**. We talk about 'orders' when we're having a discussion about the domain, not the 'orders repository'"* | — |
| **Package by component** | **Coarse-grained components** — *"bundling all of the responsibilities related to a single coarse-grained component into a single Java package"* | *"If you're writing code that needs to do something with orders, **there's just one place to go — the `OrdersComponent`**."* Inside, separation of concerns is maintained, *"but that's a **component implementation detail that consumers don't need to know about**"* | — |

**Brown's own definition of "component"**, distinguished from Martin's *(units of deployment; in Java, jar files)*:
> *"**A grouping of related functionality behind a nice clean interface, which resides inside an execution environment like an application.**"* — from the **C4 software architecture model**: a system is made of **containers** (web apps, mobile apps, stand-alone apps, databases, file systems), each containing **components**, implemented by **classes**. *"Whether each component resides in a separate jar file is **an orthogonal concern**."*

**And the relationship to micro-services:**
> *"This is akin to what you might end up with if you adopted a micro-services or SOA — a separate `OrdersService` that encapsulates everything related to handling orders. **The key difference is the decoupling mode.** You can think of **well-defined components in a monolithic application as being a stepping stone to a micro-services architecture**."*

## Worked Example: The New Hire Who Bypassed the Business Logic

The story that motivates the whole chapter:

> *"Suppose that you hire someone new… you give the newcomer another **orders**-related use case. Since the person is new, **he wants to make a big impression and get this use case implemented as quickly as possible.** After sitting down with a cup of coffee for a few minutes, the newcomer discovers an existing `OrdersController` class, so he decides that's where the code for the new orders-related web page should go. But it needs some orders data from the database. The newcomer has an epiphany: **'Oh, there's an `OrdersRepository` interface already built, too. I can simply dependency-inject the implementation into my controller. Perfect!'**"*

**The result**: a **relaxed layered architecture** — *"the dependency arrows **still point downward**, but the `OrdersController` is now additionally **bypassing the `OrdersService`**."*

**Why this is genuinely dangerous:**
> *"In some situations, this is the intended outcome — if you're trying to follow the **CQRS** pattern, for example. In many other cases, **bypassing the business logic layer is undesirable, especially if that business logic is responsible for ensuring authorized access to individual records**."*

**And why nobody noticed**: *"I see this happen a lot with teams that I visit as a consultant, and **it's usually revealed when teams start to visualize what their code base really looks like, often for the first time**."*

**The big problem with layered architectures** that the chapter withholds until here: *"**We can cheat by introducing some undesirable dependencies, yet still create a nice, acyclic dependency graph.**"* The acyclic graph is not evidence of a correct architecture.

## Framework: Three Enforcement Options, Ranked

The needed rule is *"'Web controllers should never access repositories directly.' **The question, of course, is enforcement.**"*

| Mechanism | What teams say | Verdict |
|---|---|---|
| **Discipline and code reviews** | *"We enforce this principle through good discipline and code reviews, **because we trust our developers**"* | *"This confidence is great to hear, but **we all know what happens when budgets and deadlines start looming ever closer**"* |
| **Static analysis at build time** (NDepend, Structure101, Checkstyle) | Rules like *"types in package `**/web` should not access types in `**/data`"*, executed after compilation | *"A little crude, but it can do the trick… The problem with **both** approaches is that they are **fallible, and the feedback loop is longer than it should be**"* |
| **The compiler** | Access modifiers + package structure | **"I'd personally like to use the compiler to enforce my architecture if at all possible."** |

*"If left unchecked, this practice can turn a code base into a **'big ball of mud.'**"*

## Worked Example: The `public` Keyword Collapses All Four Styles

> *"Something I see on a regular basis is an **overly liberal use of the `public` access modifier**… It's almost as if we, as developers, **instinctively use the `public` keyword without thinking. It's in our muscle memory.** If you don't believe me, take a look at the code samples for books, tutorials, and open source frameworks on GitHub."*

> *"Marking all of your types as `public` means **you're not taking advantage of the facilities that your programming language provides with regard to encapsulation**. In some cases, there's literally **nothing preventing somebody from writing some code to instantiate a concrete implementation class directly**, violating the intended architecture style."*

**Organization vs. encapsulation** — the chapter's sharpest point:
> *"If you make all types public, **the packages are simply an organization mechanism (a grouping, like folders), rather than being used for encapsulation**. Since public types can be used from anywhere, **you can effectively ignore the packages** because they provide very little real value."*

> *"**All four architectural approaches presented earlier in this chapter are exactly the same when we overuse this designation.** Take a close look at the arrows between each of the types: **They're all identical** regardless of which architectural approach you're trying to adopt. **Conceptually the approaches are very different, but syntactically they are identical.**"*

> *"Furthermore, you could argue that when you make all of the types public, **what you really have are just four ways to describe a traditional horizontally layered architecture**. This is a neat trick, and of course nobody would ever make all of their Java types public. **Except when they do. And I've seen it.**"*

**What proper access modifiers buy, style by style:**

| Style | Must be `public` | Can be **package protected** |
|---|---|---|
| **Package by layer** | `OrdersService`, `OrdersRepository` interfaces (inbound dependencies from other packages) | `OrdersServiceImpl`, `JdbcOrdersRepository` — *"Nobody needs to know about them; they are an implementation detail"* |
| **Package by feature** | `OrdersController` — *"the sole entry point into the package"* | Everything else. **Caveat**: *"nothing else in the code base, outside of this package, can access information related to orders unless they go through the controller. **This may or may not be desirable**"* |
| **Ports and adapters** | `OrdersService`, `Orders` interfaces | Implementation classes, *"dependency injected at runtime"* |
| **Package by component** | **`OrdersComponent` interface only** | Everything else |

> *"**The fewer public types you have, the smaller the number of potential dependencies.** There's now **no way** that code outside this package can use the `OrdersRepository` interface or implementation directly, so **we can rely on the compiler to enforce this architectural principle**."*

*(In .NET, use `internal` — *"although you would need to create a separate assembly for every component."* Footnote: *"Unless you cheat and use Java's reflection mechanism, but please don't do that!"*)*

**Scope of this advice**: *"What I've described here relates to a **monolithic application**, where all of the code resides in a single source code tree. If you are building such an application (**and many people are**), I would certainly encourage you to **lean on the compiler** to enforce your architectural principles, **rather than relying on self-discipline and post-compilation tooling**."*

## Framework: Other Decoupling Modes

**Module systems** — OSGi, and the Java 9 module system: *"you can make a distinction between types that are **public** and types that are **published**. For example, you could create an `Orders` module where **all of the types are marked as public, but publish only a small subset** of those types for external consumption."*

**Splitting source code trees.** For ports and adapters, three trees:
1. *"Business and domain (everything independent of technology and framework choices): `OrdersService`, `OrdersServiceImpl`, `Orders`"*
2. *"The web: `OrdersController`"*
3. *"Data persistence: `JdbcOrdersRepository`"*

*"The latter two have a **compile-time dependency on the business and domain code**, which itself doesn't know anything about the web or the data persistence code."* Implement via separate modules or projects in Maven, Gradle, or MSBuild.

> *"This is very much an **idealistic** solution, though, because there are **real-world performance, complexity, and maintenance issues** associated with breaking up your source code in this way."*

**The two-tree simplification, and its trap** — worth knowing by name:
> *"**The 'Périphérique anti-pattern of ports and adapters.'** The city of Paris has a ring road called the Boulevard Périphérique, which allows you to **circumnavigate Paris without entering the complexities of the city**. Having all of your infrastructure code in a single source code tree means that **it's potentially possible for infrastructure code in one area (e.g., a web controller) to directly call code in another area (e.g., a database repository), without navigating through the domain**. This is especially true if you've forgotten to apply appropriate access modifiers."*

## Mental Models
- **A clean dependency diagram proves nothing if every type is public.** Check access modifiers before believing an architecture diagram.
- **Ask of every `public`: who outside this package genuinely needs this?** Default to package protected and widen only under pressure.
- **The new hire is not the problem — the absence of enforcement is.** Any architecture that depends on people not taking the shortcut will eventually meet a deadline.
- **Prefer compiler enforcement > static analysis > discipline**, in that order, because the feedback loop shortens at each step.
- **Watch for the ring road.** A single "infrastructure" tree lets adapters talk to each other behind the domain's back.

## Key Takeaways
1. Package by layer, feature, ports-and-adapters, and component are conceptually distinct — and syntactically identical if everything is `public`.
2. Layered architectures let you cheat: undesirable dependencies can still produce a clean acyclic graph.
3. The relaxed layered architecture (controller → repository, skipping the service) can silently bypass authorization logic.
4. Discipline and code reviews fail under deadline pressure; static analysis is crude with a long feedback loop.
5. Use packages for **encapsulation**, not just organization — then the compiler enforces the architecture.
6. Package by component exposes exactly one public interface per component and hides everything else.
7. Consider module systems (public vs. published) and split source trees, mindful of their real costs.
8. Beware the Périphérique anti-pattern: one infrastructure tree lets adapters route around the domain.
9. *"Think about how to map your desired design on to code structures… **be pragmatic**, and take into consideration the **size of your team, their skill level, and the complexity of the solution** in conjunction with your time and budgetary constraints."*

## Connects To
- **Ch 12 (Components)**: Martin's definition (units of deployment) vs. Brown's (grouping behind a clean interface) — both are given, deliberately.
- **Ch 21 (Screaming Architecture)**: *"a layered architecture doesn't scream anything about the business domain."*
- **Ch 22**: ports and adapters as the inside/outside form of the Dependency Rule.
- **Ch 27 (Services)**: well-defined components in a monolith as a stepping stone to micro-services.
- **Ch 16 (Independence)**: decoupling modes, here evaluated at the source-tree and access-modifier level.
- **Fowler**, *"Presentation Domain Data Layering"*; **the C4 model** (structurizr.com/help/c4); **"Big Ball of Mud"** (laputan.org/mud).
