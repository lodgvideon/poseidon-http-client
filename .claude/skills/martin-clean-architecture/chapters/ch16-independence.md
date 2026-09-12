# Chapter 16: Independence

*Part V: Architecture*

## Core Idea
A good architecture supports use cases, operation, development, and deployment — and **leaves the decoupling mode itself as an open option**, so a system can be born a monolith, grow into services, and slide back again.

## Reference Table: The Four Things Architecture Must Support

| Concern | What architecture does |
|---|---|
| **Use cases** | *"The architecture must support the intent of the system."* Architecture can't do much for *behavior*, but it can **clarify and expose** it: *"A shopping cart application with a good architecture **will look like a shopping cart application**… Developers will not have to hunt for behaviors, because those behaviors will be first-class elements visible at the top level"* |
| **Operation** | *"A more substantial, and less cosmetic, role."* 100,000 customers/second, or big-data cubes queried in milliseconds, dictate structure — parallel services, lightweight threads, isolated processes, or *"simple monolithic programs running in a single process"* |
| **Development** | **Conway's law**: *"Any organization that designs a system will produce a design whose structure is a copy of the organization's communication structure."* Many teams require *"well-isolated, independently developable components"* allocated to teams that work independently |
| **Deployment** | The goal is **"immediate deployment."** *"A good architecture does not rely on dozens of little configuration scripts and property file tweaks. It does not require manual creation of directories or files that must be arranged just so"* |

**The honest difficulty**, stated plainly:
> *"Most of the time we don't know what all the use cases are, nor do we know the operational constraints, the team structure, or the deployment requirements. Worse, even if we did know them, **they will inevitably change** as the system moves through its life cycle. In short, the goals we must meet are indistinct and inconstant. **Welcome to the real world.**"*

> *"But all is not lost: Some principles of architecture are relatively **inexpensive** to implement and can help balance those concerns, even when you don't have a clear picture of the targets you have to hit."*

## Framework: Decoupling Layers, Then Use Cases

**Horizontal — decoupling layers.** Apply SRP and CCP to the *known intent* of the system, separating what changes for different reasons:

| Layer | Changes because… |
|---|---|
| **UI** | *"User interfaces change for reasons that have nothing to do with business rules"* |
| **Application-specific business rules** | e.g. *"the validation of input fields is a business rule that is closely tied to the application itself"* |
| **Application-independent business rules** | e.g. *"the calculation of interest on an account and the counting of inventory"* — associated with the **domain** |
| **Database / query language / schema** | *"Technical details that have nothing to do with the business rules or the UI"* |

**Vertical — decoupling use cases.** *"The use case for adding an order… almost certainly will change at a different rate, and for different reasons, than the use case that deletes an order."*

> *"Use cases are **narrow vertical slices that cut through the horizontal layers**. Each use case uses some UI, some application-specific business rules, some application-independent business rules, and some database functionality."*

So: separate the add-order UI from the delete-order UI; do the same with business rules and with the database. **Keep the use cases separate down the vertical height of the system.**

> *"If you decouple the elements of the system that change for different reasons, then you can **continue to add new use cases without interfering with old ones**."*

**Why this also solves operation**: *"If the different aspects of the use cases are separated, then those that must run at a high throughput are likely **already separated** from those that must run at a low throughput."* But the benefit requires the right **mode** — to run on separate servers, components *"cannot depend on being together in the same address space… They must be independent services, which communicate over a network."*

**Why it also solves development and deployment**: strongly decoupled components mitigate team interference *"irrespective of whether they are organized as feature teams, component teams, layer teams, or some other variation."* And if the decoupling is done well, *"it should be possible to **hot-swap layers and use cases in running systems**. Adding a new use case could be as simple as adding a few new jar files or services."*

## Framework: True vs. Accidental Duplication

> *"Architects often fall into a trap — a trap that hinges on their **fear of duplication**."*

| Kind | Definition | What to do |
|---|---|---|
| **True duplication** | *"Every change to one instance necessitates the same change to every duplicate"* | *"We are honor-bound as professionals to reduce and eliminate it"* |
| **False / accidental duplication** | Two similar sections that *"evolve along different paths — if they change at different rates, and for different reasons"* | **Leave them apart.** *"Return to them in a few years, and you'll find that they are very different from each other"* |

**Two concrete traps:**
- **Similar screens across use cases**: *"The architects will likely be strongly tempted to share the code for that structure. But should they?… **Most likely it is accidental.** As time goes by, the odds are that those two screens will diverge… For this reason, care must be taken to avoid unifying them. Otherwise, separating them later will be a challenge."*
- **A database record shaped like a view**: *"You may be tempted to simply pass the database record up to the UI, rather than to create a view model that looks the same and copy the elements across. **Be careful: This duplication is almost certainly accidental.** Creating the separate view model is not a lot of effort, and it will help you keep the layers properly decoupled."*

> *"Resist the temptation to commit the sin of **knee-jerk elimination of duplication**. Make sure the duplication is real."*

## Framework: The Three Decoupling Modes

| Mode | What is controlled | How components communicate | Also called |
|---|---|---|---|
| **Source level** | Dependencies between source modules, so changes don't force recompilation of others (e.g. Ruby Gems) | Same address space, **simple function calls**; one executable in memory | **Monolithic structure** |
| **Deployment level** | Dependencies between deployable units — jars, DLLs, shared libraries — so changes don't force rebuild/redeploy | Many still in the same address space via function calls; others in separate processes via IPC, sockets, or shared memory | Independently deployable units |
| **Service level** | Dependencies reduced **to the level of data structures**; every execution unit independent of others' source and binary changes | **Network packets only** | Services / micro-services |

**Which is best?** *"It's hard to know which mode is best during the early phases of a project. Indeed, **as the project matures, the optimal mode may change**."*

**Against service-by-default** — Martin names two costs:
1. *"It is expensive and **encourages coarse-grained decoupling**. No matter how 'micro' the micro-services get, the decoupling is not likely to be fine-grained enough."*
2. *"Expensive, both in development time and in system resources. Dealing with service boundaries where none are needed is a waste of effort, memory, and cycles. **And, yes, I know that the last two are cheap — but the first is not.**"*

**Martin's stated preference:**
> *"Push the decoupling to the point where a service **could** be formed, should it become necessary; but then **leave the components in the same address space as long as possible**. This leaves the option for a service open."*

The progression: start at source level → move to deployment level if deployment or development issues arise → *"carefully choose which deployable units to turn into services, and gradually shift the system in that direction."*

**And the reverse must work too:**
> *"A good architecture will allow a system to be born as a monolith, deployed in a single file, but then to grow into a set of independently deployable units, and then all the way to independent services and/or micro-services. Later, as things change, **it should allow for reversing that progression and sliding all the way back down into a monolith**."*

*"Over time, the operational needs of the system may decline. What once required decoupling at the service level may now require only deployment-level or even source-level decoupling."*

## Mental Models
- **Decoupling mode is an option, not an identity.** "We are a micro-services shop" is a decision made once and never revisited — exactly what a good architecture avoids.
- **Ask "will these diverge?" before deduplicating.** Different change rates and reasons ⇒ accidental duplication ⇒ leave them alone.
- **Conway's law cuts both ways.** Your org chart will shape the architecture; a good architecture also *enables* whatever team organization you choose.
- **Copying a record into a view model is cheap; un-merging two screens is not.** Prefer the reversible mistake.

## Key Takeaways
1. Architecture must serve use cases, operation, development, and deployment — and the targets are *indistinct and inconstant*.
2. Make the intent visible: a shopping cart system should look like one at the top level.
3. Decouple horizontally into layers **and** vertically into use cases; the two together let you add features without touching old ones.
4. The same decoupling that serves use cases serves operation — provided the mode allows separate address spaces.
5. Distinguish true from accidental duplication; unifying accidentally-similar code is expensive to undo.
6. Three modes — source, deployment, service — and the optimal one changes over a project's life.
7. Prefer decoupling *to the point where a service could be formed*, then stay in-process as long as possible.
8. A good architecture supports the progression toward services **and the reversal back to a monolith**.

## Connects To
- **Ch 15**: the four life-cycle concerns, here turned into concrete decoupling strategy.
- **Ch 7 (SRP)** and **Ch 13 (CCP)**: the tools used to decide what to separate.
- **Ch 17 (Boundaries)**: where the lines go once you've decided to draw them.
- **Ch 18 (Boundary Anatomy)**: the runtime cost and form of each decoupling mode.
- **Ch 21 (Screaming Architecture)**: "will look like a shopping cart application," developed.
- **Ch 27 (Services: Great and Small)**: the case against service-by-default, at length.
