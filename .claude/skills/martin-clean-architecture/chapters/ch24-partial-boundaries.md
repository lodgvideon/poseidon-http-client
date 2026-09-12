# Chapter 24: Partial Boundaries

*Part V: Architecture*

## Core Idea
Full architectural boundaries are expensive to build *and to maintain*. A **partial boundary** holds the place for one — *"'Yeah, but I might'"* — at reduced cost, with the honest risk that it **degrades if the boundary never materializes**.

## Framework: Why Partial At All

> *"Full-fledged architectural boundaries are expensive. They require **reciprocal polymorphic Boundary interfaces**, Input and Output data structures, and all of the dependency management necessary to isolate the two sides into independently compilable and deployable components. **That takes a lot of work. It's also a lot of work to maintain.**"*

> *"This kind of anticipatory design is often frowned upon by many in the Agile community as a violation of **YAGNI**: 'You Aren't Going to Need It.' Architects, however, sometimes look at the problem and think, **'Yeah, but I might.'**"*

## Reference Table: Three Partial Boundary Techniques

| Technique | What you build | What you save | How it degrades |
|---|---|---|---|
| **Skip the Last Step** | Everything for a full boundary — reciprocal interfaces, input/output data structures, complete dependency management — **then compile and deploy it all as a single component** | *"It does **not require the administration of multiple components**. There's no version number tracking or release management burden. **That difference should not be taken lightly.**"* Code cost is unchanged | Dependencies quietly start crossing in the wrong direction |
| **One-Dimensional Boundary** (Strategy pattern) | A `ServiceBoundary` interface used by clients and implemented by `ServiceImpl`. *"The necessary dependency inversion is in place in an attempt to isolate the Client from the ServiceImpl"* | Only **one** direction of isolation instead of reciprocal interfaces — much cheaper to set up and maintain | *"The separation can degrade **pretty rapidly**… **Without reciprocal interfaces, nothing prevents this kind of backchannel other than the diligence and discipline of the developers and architects**"* |
| **Facade** | A `Facade` class listing all services as methods and deploying calls to classes *"the client is not supposed to access"* | *"Even the **dependency inversion is sacrificed**"* — the cheapest option | *"The Client has a **transitive dependency on all those service classes**. In static languages, a change to the source code in one of the Service classes **will force the Client to recompile**. Also, you can imagine how easy backchannels are to create"* |

## Worked Example: FitNesse's Web Component — and Its Decay

**The design**: FitNesse's web server component was *"designed to be separable from the wiki and testing part."* The intent was to reuse the web component for other web-based applications.

**Why it stayed merged**: the **"download and go"** goal. *"We did not want users to have to download two components… users would download one jar file and execute it without having to hunt for other jar files, work out version compatibilities, and so on."*

**And then the honest ending** — the reason this example is in the book:
> *"The story of FitNesse also points out **one of the dangers of this approach**. Over time, as it became clear that there would **never be a need for a separate web component**, the separation between the web component and the wiki component **began to weaken**. Dependencies started to cross the line in the wrong direction. **Nowadays, it would be something of a chore to re-separate them.**"*

**The lesson to carry**: a partial boundary is only maintained by intent. Once the team stops believing the boundary will ever be needed, entropy collects the difference — and the option you were paying to keep quietly expires.

## Mental Models
- **Partial boundaries are options with a maintenance premium.** You pay upfront (code) and continuously (discipline) for the right to split later. If nobody believes in the split, stop paying.
- **The three techniques trade cost against enforceability, in order.** Skip-the-last-step is fully enforced but full-price; Strategy is half-enforced; Facade enforces nothing and leaks transitive dependencies.
- **"Nothing prevents this other than diligence" is a design statement, not a disclaimer.** Choosing a weaker technique means choosing to rely on people rather than the compiler — decide that deliberately.
- **YAGNI and "yeah, but I might" are both defensible.** The chapter refuses to pick a side; it gives you graduated middle options instead.

## Key Takeaways
1. Full boundaries cost reciprocal interfaces, I/O data structures, and multi-component administration — build *and* maintenance.
2. **Skip the last step**: build the full boundary, ship one component. Same code cost, no release-management burden.
3. **Strategy (one-dimensional)**: dependency inversion in one direction only; cheap, but backchannels are prevented only by discipline.
4. **Facade**: cheapest of all, no inversion, and clients acquire transitive dependencies that force recompilation.
5. Every partial boundary degrades if the anticipated split never happens — FitNesse's web/wiki separation is the cautionary case.
6. *"It is one of the functions of an architect to decide where an architectural boundary might one day exist, and whether to fully or partially implement that boundary."*

## Connects To
- **Ch 17–18 (Boundaries, Boundary Anatomy)**: what the full-price version looks like.
- **Ch 25 (Layers and Boundaries)**: the "watchful eye" doctrine — deciding *when* to upgrade a partial boundary to a full one.
- **Ch 16 (Independence)**: "push the decoupling to the point where a service could be formed" is the same instinct at deployment scale.
- **Ch 7 (SRP)**: the Facade solution to the `Employee` problem is this pattern used for a different purpose.
- **GOF**: Strategy, Facade.
