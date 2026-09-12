# Chapter 18: Boundary Anatomy

*Part V: Architecture*

## Core Idea
**"At runtime, a boundary crossing is nothing more than a function on one side of the boundary calling a function on the other side and passing along some data. The trick to creating an appropriate boundary crossing is to manage the *source code* dependencies."**

**Why source code?** *"Because when one source code module changes, other source code modules may have to be changed or recompiled, and then redeployed. **Managing and building firewalls against this change is what boundaries are all about.**"*

## Reference Table: The Four Boundary Strengths

| Boundary | Physical form | Communication | Cost | Chattiness |
|---|---|---|---|---|
| **Monolith** (source-level) | *"No strict physical representation"* — a disciplined segregation within one processor and address space; a single executable | Function calls | *"Very fast and inexpensive"* | **Can be very chatty** |
| **Deployment component** | DLL, jar, Ruby Gem, UNIX shared library | Function calls | One-time hit for dynamic linking or runtime loading | **Can still be very chatty** |
| **Local process** | Separate address spaces, same processor / multicore | Sockets, mailboxes, message queues; sometimes shared memory | OS calls, data marshaling and decoding, interprocess context switches — *"moderately expensive"* | **Should be carefully limited** |
| **Service** | *"The strongest boundary."* Location-independent; assumes all communication is over the network | Network | *"Very slow compared to function calls. Turnaround times can range from **tens of milliseconds to seconds**"* | **Avoid where possible**; deal with high latency |

## Framework: The Two Crossings

**Crossing *with* the flow of control** (Fig 18.1) — the simplest case: a low-level `Client` calls `f()` on a higher-level `Service`, passing `Data`.
- Both the **runtime** and **compile-time** dependencies point **the same direction**, toward the higher-level component.
- **The definition of the `Data` is on the *called* side of the boundary.**

**Crossing *against* the flow of control** (Fig 18.2) — a high-level `Client` needs a lower-level service:
- The `Client` calls `f()` on `ServiceImpl` **through a `Service` interface**.
- *"**All dependencies cross the boundary from right to left toward the higher-level component.**"* The runtime dependency opposes the compile-time dependency.
- **The definition of the data structure is on the *calling* side of the boundary.**

**Which side owns the data structure is the tell** for which kind of crossing you have — worth checking when a boundary feels wrong.

## Framework: The Monolith Is a Real Architecture

> *"The fact that the boundaries are **not visible during the deployment** of a monolith does not mean that they are not present and meaningful. Even when statically linked into a single executable, **the ability to independently develop and marshal the various components for final assembly is immensely valuable**."*

**Why OO matters here specifically:**
> *"Such architectures almost always depend on some kind of **dynamic polymorphism** to manage their internal dependencies. This is one of the reasons that object-oriented development has become such an important paradigm… Without OO, or an equivalent form of polymorphism, architects must fall back on the dangerous practice of using **pointers to functions**. Most architects find prolific use of pointers to functions to be too risky, so **they are forced to abandon any kind of component partitioning**."*

*(Footnote worth keeping: static polymorphism — generics or templates — can sometimes manage dependencies in monoliths, especially in C++. But **"the decoupling afforded by generics cannot protect you from the need for recompilation and redeployment the way dynamic polymorphism can."** And static polymorphism is not an option at the deployment-component level at all.)*

**What disciplined partitioning buys even inside one executable:**
> *"Teams can work independently of each other on their own components **without treading on each other's toes**. High-level components remain independent of lower-level details."*

Delivery form: *"Since the deployment of monoliths usually requires compilation and static linking, components in these systems are typically delivered as **source code**."*

## Key Concepts
- **Deployment components** are *"the same as monoliths"* with one exception — deployment does not involve compilation; units arrive in binary or equivalent deployable form. Deploying is *"simply the gathering of these deployable units together in some convenient form, such as a WAR file, or even just a directory."*
- **Threads** — *"**not architectural boundaries or units of deployment**, but rather a way to organize the schedule and order of execution."* They may sit wholly inside one component or spread across many.
- **Local processes as uber-components** — *"The process consists of lower-level components that manage their dependencies through dynamic polymorphism."* Each may be a statically linked monolith or composed of dynamically linked components; separate monolithic processes may have the same components compiled into each.
- **The segregation rule is identical at every level**: *"Source code dependencies point in the same direction across the boundary, and **always toward the higher-level component**."*

**The concrete form of that rule at process and service level:**
> *"For local processes, this means that the source code of the higher-level processes must not contain the **names, or physical addresses, or registry lookup keys** of lower-level processes."*

> *"The source code of higher-level services must not contain any specific physical knowledge (e.g., **a URI**) of any lower-level service."*

> *"Remember that the architectural goal is for **lower-level processes to be plugins to higher-level processes**."*

## Framework: Real Systems Mix Boundaries

> *"Most systems, other than monoliths, use **more than one boundary strategy**. A system that makes use of service boundaries may also have some local process boundaries. Indeed, **a service is often just a facade for a set of interacting local processes**. A service, or a local process, will almost certainly be either a monolith composed of source code components or a set of dynamically linked deployment components."*

> *"This means that the boundaries in a system will often be a mixture of **local chatty boundaries** and boundaries that are **more concerned with latency**."*

## Mental Models
- **Boundary strength and communication cost move together.** Choosing a stronger boundary is choosing to pay more per crossing — so design the traffic, not just the topology.
- **Chattiness is a design budget.** In-process boundaries can afford fine-grained conversation; service boundaries cannot. Crossing granularity should be decided per boundary, not per system.
- **A URI in high-level source is the service-level equivalent of an `#include` pointing the wrong way.** Physical knowledge flowing downhill breaks the plugin relationship.
- **Invisible boundaries still work.** A monolith with disciplined internal boundaries is architecturally sound — and is what makes later extraction into services possible.

## Key Takeaways
1. A boundary crossing at runtime is just a function call with data; the architecture lives in the *source code dependencies*.
2. Four strengths — monolith, deployment component, local process, service — increasing in isolation and in cost per crossing.
3. With the flow of control: both dependencies point the same way, and the data structure lives on the *called* side.
4. Against the flow of control: use dynamic polymorphism; all source dependencies point toward the higher level, and the data structure lives on the *calling* side.
5. Monolithic boundaries are real and valuable even though deployment can't see them.
6. Dynamic polymorphism is what makes internal partitioning safe; without it architects abandon partitioning rather than use function pointers.
7. Threads are a scheduling mechanism, not a boundary.
8. Higher-level code must never contain the names, addresses, registry keys, or URIs of lower-level code.
9. Expect a mixture: chatty local boundaries plus latency-sensitive remote ones in the same system.

## Connects To
- **Ch 16 (Independence)**: the three decoupling modes, here anatomized with their costs.
- **Ch 5**: safe polymorphism as the precondition for internal boundaries.
- **Ch 11 (DIP)**: crossing against the flow of control *is* dependency inversion.
- **Ch 24 (Partial Boundaries)** and **Ch 25 (Layers and Boundaries)**: cheaper and more complex boundary forms.
- **Ch 27 (Services: Great and Small)**: why the strongest boundary is not automatically the best one.
