# Chapter 27: Services — Great and Small

*Part V: Architecture*

## Core Idea
**"Architectural boundaries do not fall *between* services. Rather, those boundaries run *through* the services, dividing them into components."** Services are function calls across process boundaries — useful, but **not architecturally significant in themselves**.

## Framework: Services Are Not an Architecture

> *"The architecture of a system is defined by **boundaries that separate high-level policy from low-level detail and follow the Dependency Rule**. Services that simply separate application behaviors are **little more than expensive function calls**, and are not necessarily architecturally significant."*

**The analogy that makes it precise**: in a monolithic or component-based system, architecture is defined by *"certain function calls that cross architectural boundaries and follow the Dependency Rule. **Many other functions in those systems, however, simply separate one behavior from another and are not architecturally significant.** So it is with services."*

**The fair caveat**: *"This is not to say that all services should be architecturally significant. There are often **substantial benefits** to creating services that separate functionality across processes and platforms — whether they obey the Dependency Rule or not."*

## Framework: The Two Fallacies

### The Decoupling Fallacy

**The claim**: services run in different processes or processors, so they can't touch each other's variables, and their interfaces must be well defined.

**The rebuttal**: *"There is certainly some truth to this — **but not very much truth**. Yes, services are decoupled **at the level of individual variables**. However, they can still be coupled by **shared resources** within a processor, or on the network. What's more, **they are strongly coupled by the data they share**."*

> *"If a new field is added to a data record that is passed between services, then **every service that operates on the new field must be changed**. The services must also **strongly agree about the interpretation** of the data in that field. Thus those services are strongly coupled to the data record and, therefore, **indirectly coupled to each other**."*

**And on well-defined interfaces**: *"That's certainly true — **but it is no less true for functions**. Service interfaces are **no more formal, no more rigorous, and no better defined than function interfaces**. Clearly, then, this benefit is something of an illusion."*

### The Fallacy of Independent Development and Deployment

**The claim**: dedicated teams own services; this scales to hundreds or thousands of independently developable services.

**The rebuttal, in two parts:**
1. *"History has shown that large enterprise systems can be built from **monoliths and component-based systems** as well as service-based systems. Thus **services are not the only option** for building scalable systems."*
2. *"The decoupling fallacy means that services **cannot always be independently developed, deployed, and operated**. To the extent that they are coupled by data or behavior, the development, deployment, and operation **must be coordinated**."*

## Worked Example: The Kitty Problem

**The system** — the taxi aggregator, split into micro-services *(footnote: "Therefore the number of micro-services will be roughly equal to the number of programmers.")*:

| Service | Responsibility |
|---|---|
| `TaxiUI` | Deals with customers ordering taxis on mobile devices |
| `TaxiFinder` | Examines `TaxiSupplier` inventories, determines candidate taxis, deposits them *"into a short-term data record attached to that user"* |
| `TaxiSelector` | Takes the user's criteria — cost, time, luxury, driver experience — and chooses among the candidates |
| `TaxiDispatcher` | Orders the chosen taxi |

**A year in, marketing announces: kitten delivery.** Customers order kittens delivered to homes or businesses; collection points are set up across the city; a nearby taxi collects a kitten and delivers it.

**The constraints that make it cut across everything:**
- One taxi supplier has agreed; *"others are likely to follow. Still others may decline."*
- *"Some drivers may be **allergic to cats**, so those drivers should never be selected for this service."*
- *"Some customers will undoubtedly have similar allergies, so **a vehicle that has been used to deliver kittens within the last 3 days** should not be selected for customers who declare such allergies."*

> *"Look at that diagram of services. **How many of those services will have to change to implement this feature? All of them.** Clearly, the development and deployment of the kitty feature will have to be very carefully coordinated. **In other words, the services are all coupled**, and cannot be independently developed, deployed, and maintained."*

**The general diagnosis:**
> *"This is the problem with **cross-cutting concerns**. Every software system must face this problem, whether service oriented or not. **Functional decompositions… are very vulnerable to new features that cut across all those functional behaviors.**"*

## Worked Example: Objects to the Rescue

**How SOLID solves the same problem**: create classes *"that could be polymorphically extended to handle new features."*

- Classes correspond roughly to the original services, **but with boundaries following the Dependency Rule**.
- *"Much of the logic of the original services is preserved within the **base classes** of the object model."*
- Ride-specific logic is extracted into a **`Rides` component**; the new feature goes into a **`Kittens` component**.
- Both *"**override the abstract base classes** in the original components using a pattern such as **Template Method or Strategy**."*
- The classes implementing those features are *"created by **factories under the control of the UI**."*

**The result:**
> *"When the Kitty feature is implemented, **the `TaxiUI` must change. But nothing else needs to be changed.** Rather, **a new jar file, or Gem, or DLL is added to the system and dynamically loaded at runtime**. Thus the Kitty feature is decoupled, and independently developable and deployable."*

## Framework: Component-Based Services

> *"Can we do that for services? And the answer is, of course: **Yes! Services do not need to be little monoliths.**"*

**The Java formulation, concretely:**
- Think of a service as *"a set of **abstract classes in one or more jar files**."*
- Think of each new feature as *"another jar file that contains classes that **extend the abstract classes** in the first jar files."*
- > *"Deploying a new feature then becomes **not a matter of redeploying the services**, but rather a matter of simply **adding the new jar files to the load paths** of those services. In other words, **adding new features conforms to the Open-Closed Principle**."*

> *"The services still exist as before, but **each has its own internal component design**, allowing new features to be added as new derivative classes."*

## Mental Models
- **Ask of any service split: does this boundary separate policy from detail, or just behavior from behavior?** Only the first is architecture; the second is an expensive function call.
- **Shared data records are the real coupling.** Process isolation buys you nothing against a schema change that ripples through every consumer.
- **Functional decomposition is the vulnerability, not the service mechanism.** Slicing by function guarantees that cross-cutting features touch every slice.
- **A service that can't accept a new jar without redeploying isn't OCP-compliant** — it's a little monolith.
- **"Number of micro-services ≈ number of programmers" is a warning, not a design.**

## Key Takeaways
1. Services are function calls across process/platform boundaries; some are architecturally significant and some aren't.
2. Decoupling is real only at the variable level — shared data records and shared resources couple services tightly.
3. Service interfaces are no more rigorous than function interfaces.
4. Independent deployment holds only while services are uncoupled by data or behavior.
5. Cross-cutting features (the Kitty problem) hit every service in a functional decomposition.
6. SOLID + polymorphic extension confines the same feature to one new deployable unit plus the UI.
7. Design services with **internal component architectures** following the Dependency Rule; add features as new jars on the load path.
8. **Boundaries run through services, not between them** — *"the components within the services"* define the architecture.

## Connects To
- **Ch 13 (CCP)**: the forward reference to "The Kitty Problem" comes from there — this is the cross-component change problem CCP predicts.
- **Ch 16 (Independence)**: the case against service-by-default decoupling.
- **Ch 22**: the Dependency Rule is the criterion for architectural significance.
- **Ch 9 (LSP)**: the same taxi aggregator, showing what non-substitutable service interfaces cost.
- **Ch 34 (The Missing Chapter)**: what actually enforces boundaries in practice.
- **GOF**: Template Method, Strategy, Abstract Factory.
