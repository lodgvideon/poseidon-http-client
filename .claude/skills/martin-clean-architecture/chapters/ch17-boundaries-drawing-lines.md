# Chapter 17: Boundaries — Drawing Lines

*Part V: Architecture*

## Core Idea
**"Software architecture is the art of drawing lines that I call boundaries."** They separate elements and restrict each side from knowing about the other. Early lines exist **to defer decisions** and keep those decisions from polluting the core business logic.

## Framework: What Boundaries Defend Against

> *"Recall that the goal of an architect is to minimize the human resources required to build and maintain the required system. What is it that saps this kind of people-power? **Coupling — and especially coupling to premature decisions.**"*

**Which decisions are premature?** *"Decisions that have nothing to do with the business requirements — the use cases — of the system. These include decisions about **frameworks, databases, web servers, utility libraries, dependency injection**, and the like."*

> *"A good system architecture is one in which decisions like these are rendered **ancillary and deferrable**… and allows those decisions to be made at the latest possible moment, without significant impact."*

**Where do lines go?**
> **"Boundaries are drawn where there is an *axis of change*. The components on one side of the boundary change at different rates, and for different reasons, than the components on the other side."**

> *"This is simply the **Single Responsibility Principle** again. The SRP tells us where to draw our boundaries."*

## Worked Example 1: Company P — a premature topology

A 1980s monolithic desktop app grew into a successful GUI product. When the web arrived in the late 1990s, P hired *"a bunch of hotshot twenty-something Java programmers"* who *"had dreams of server farms dancing in their heads"* and adopted a **three-tier "architecture"** — GUI servers, middleware servers, database servers.

*(Martin's footnote is the whole point: "The word 'architecture' appears in quotes here because **three-tier is not an architecture; it's a topology**. It's exactly the kind of decision that a good architecture strives to defer.")*

**The decision, made very early**: every domain object gets **three instantiations** — one per tier — with method invocations converted to objects, serialized, and marshaled across the wire.

**The cost of adding one field to one record:**

| Artifact | Count |
|---|---|
| Classes to update | 3 (one per tier) |
| Message protocols to design (data travels both directions) | 4 |
| Protocol handlers (send + receive per protocol) | 8 |
| Executables to rebuild | 3 |

Plus, at runtime: *"all the object instantiations, all the serializations, all the marshaling and de-marshaling, all the building and parsing of messages, all the socket communications, timeout managers, retry scenarios."*

**The punchline, twice over:**
- During development they had no server farm — they ran all three executables on one machine, **paying every serialization cost anyway**, for years.
- > *"The irony is that company P **never sold a system that required a server farm**. Every system they ever deployed was a single server… in anticipation of a server farm that never existed, and never would."*

> *"The tragedy is that the architects, by making a premature decision, **multiplied the development effort enormously**."*

*"The story of P is not isolated. I've seen it many times and in many places. Indeed, **P is a superposition of all those places**."*

## Worked Example 2: Company W — premature SOA

A local fleet-management business hired an *"Architect"* — *"and, let me tell you, **control** was this guy's middle name"* — who built a huge domain model, a suite of services to manage it, and *"put all the developers on a path to Hell."*

**Adding a contact's name, address, and phone to a sales record required:**
1. Ask the `ServiceRegistry` for the service ID of `ContactService`
2. Send a `CreateContact` message — *"this message had **dozens of fields** that all had to have valid data in them — data to which the programmer had no access, since all the programmer had was a name, address, and phone number"*
3. **Fake the missing data**
4. Jam the new contact's ID into the sales record and send `UpdateContact` to `SaleRecordService`

**And to test anything**: fire up all the necessary services one by one, plus the message bus, plus the BPel server — then absorb *"the propagation delays as these messages bounced from service to service, and waited in queue after queue."*

**The diagnosis is precise, and not anti-service:**
> *"There's **nothing intrinsically wrong** with a software system that is structured around services. The error at W was the **premature adoption and enforcement** of a suite of tools that promised SoA — that is, the premature adoption of a massive suite of domain object services."*

## Worked Example 3: FitNesse — an architectural success

Martin and his son Micah, 2001. The constraint that drove everything: **"Download and Go"** — *"anything we produced should not require people to download more than one jar file."*

**Decision 1 — write their own web server.** *"This might sound absurd. Even in 2001 there were plenty of open source web servers."* But *"a bare-bones web server is a very simple piece of software to write and **it allowed us to postpone any web framework decision until much later**."* (Years later they slipped in Velocity.)

**Decision 2 — refuse to think about the database.** MySQL was in mind, but *"we purposely delayed that decision by employing a design that made the decision irrelevant"* — an interface, `WikiPage`, between all data access and the repository.

**The timeline of deferral:**

| Period | Implementation | What it enabled |
|---|---|---|
| First 3 months | `MockWikiPage` — data access methods **stubbed** | Worked purely on translating wiki text to HTML |
| Next ~1 year | `InMemoryPage` — a hash table of wiki pages in RAM | *"We got the whole first version working this way… create pages, link to pages, all the fancy wiki formatting, and even run tests with FIT. **What we couldn't do was save any of our work.**"* |
| Then | `FileSystemWikiPage` — hash tables written to flat files | Development of more features continued |
| 3 months later | **MySQL abandoned entirely** | *"We deferred that decision into **nonexistence** and never looked back"* |
| Later still | A customer wrote `MySqlWikiPage` — *"He came back **a day later** with the whole system working in MySQL"* | Deferral cost nothing even when someone did want the original option |

**The measured payoff:**
> *"The fact that we did not have a database running for **18 months** of development meant that, for 18 months, we did not have schema issues, query issues, database server issues, password issues, connection time issues, and all the other nasty issues that raise their ugly heads when you fire up a database. **It also meant that all our tests ran fast**, because there was no database to slow them down."*

## Framework: Which Lines, and Which Direction

> *"You draw lines between things that matter and things that don't. The GUI doesn't matter to the business rules… The database doesn't matter to the GUI… The database doesn't matter to the business rules."*

**Anticipating the objection**: *"Many of us have been taught to believe that the database is inextricably connected to the business rules. Some of us have even been convinced that **the database is the embodiment of the business rules**. But… this idea is misguided. The database is a tool that the business rules use **indirectly**."*

**The structure (Figs 17.1–17.3):**
- `BusinessRules` use a `DatabaseInterface`; `DatabaseAccess` implements it and drives the actual `Database`.
- **The boundary is drawn across the inheritance relationship, just below `DatabaseInterface`.**
- Both arrows leave `DatabaseAccess` — *"that means that **none of these classes knows that the `DatabaseAccess` class exists**."*
- At component scale: **`DatabaseInterface` lives in the `BusinessRules` component; `DatabaseAccess` lives in the `Database` component.**

> *"The `Database` knows about the `BusinessRules`. The `BusinessRules` do not know about the `Database`… It shows that **the `Database` does not matter to the `BusinessRules`, but the `Database` cannot exist without the `BusinessRules`**."*

*"If that seems strange, just remember: the `Database` component contains the code that translates the calls made by the `BusinessRules` into the query language of the database. **It is that translation code that knows about the `BusinessRules`.**"*

Result: Oracle, MySQL, Couch, Datomic, or flat files — *"The business rules don't care at all."*

## Framework: IO Is Irrelevant

> *"Developers and customers often get confused about what the system is. They see the GUI, and think that the GUI **is** the system… They fail to realize a critically important principle: **The IO is irrelevant.**"*

**The video game argument**: your experience is dominated by screen, mouse, buttons, sounds. *"You forget that behind that interface there is a **model** — a sophisticated set of data structures and functions — driving it. More importantly, **that model does not need the interface**. It would happily execute its duties, modeling all the events in the game, without the game ever being displayed on the screen."*

Same boundary, same direction: **the GUI cares about the BusinessRules**, never the reverse.

## Framework: Plugin Architecture, and the Plugin Argument

> *"The history of software development technology is the story of **how to conveniently create plugins** to establish a scalable and maintainable system architecture."*

With UI and database both treated as plugins, the UI can be web, client/server, SOA, console, or anything else; the database can be SQL, NoSQL, or file-system based. *"These replacements might not be trivial"* — a web-to-client-server UI swap would require reworking some communication — *"Even so, by starting with the presumption of a plugin structure, we have at very least **made such a change practical**."*

**The ReSharper / Visual Studio argument** (Fig 17.6) — an asymmetry made concrete: JetBrains is in Russia, Microsoft in Redmond. *"It's hard to imagine two development teams that are more separate."*

> *"The source code of ReSharper depends on the source code of Visual Studio. Thus **there is nothing that the ReSharper team can do to disturb the Visual Studio team. But the Visual Studio team could completely disable the ReSharper team** if they so desired."*

> *"That's a deeply asymmetric relationship, and it is one that **we desire to have in our own systems**… Arranging our systems into a plugin architecture creates **firewalls across which changes cannot propagate**."*

## Mental Models
- **Ask of every early decision: is this a business requirement or a topology?** Three-tier, SOA, and micro-services are topologies. Defer them.
- **Deferral is not procrastination — it's information gathering, and sometimes the decision dissolves.** FitNesse deferred MySQL "into nonexistence."
- **Direction of the arrow encodes who can hurt whom.** Point it at what you want to be immune.
- **No database for 18 months meant fast tests for 18 months.** Deferral pays compounding day-to-day dividends, not just optionality.

## Key Takeaways
1. Boundaries defend against coupling to premature decisions — frameworks, databases, web servers, DI.
2. Draw lines on axes of change; SRP tells you where they go.
3. Premature topology (P) multiplies development effort for a scale that may never arrive.
4. Premature SOA (W) is the same error at service granularity — services aren't wrong, *premature* services are.
5. Put the interface in the business-rules component and the implementation in the detail component; the detail depends on the policy.
6. IO is irrelevant: the model doesn't need the interface, so the interface must depend on the model.
7. Treat UI and database as plugins to create firewalls that changes cannot cross.
8. Aim for the ReSharper/Visual Studio asymmetry inside your own system.

## Connects To
- **Ch 7 (SRP)**: the rule that locates every boundary.
- **Ch 11 (DIP)** and **Ch 14 (SAP)**: *"Dependency arrows are arranged to point from lower-level details to higher-level abstractions"* — the chapter's own closing attribution.
- **Ch 18 (Boundary Anatomy)**: what a boundary crossing costs at runtime.
- **Ch 22 (The Clean Architecture)**: the full concentric form of these lines.
- **Ch 30 (The Database Is a Detail)**: the "database embodies the business rules" belief, refuted at length.
- **Ch 5**: plugin architecture as the payoff of safe polymorphism.
