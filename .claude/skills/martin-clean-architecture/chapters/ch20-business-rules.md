# Chapter 20: Business Rules

*Part V: Architecture*

## Core Idea
**"Business rules are the reason a software system exists… They are the family jewels."** They come in two kinds — **Critical Business Rules** (Entities) and **application-specific rules** (Use Cases) — and Entities must never know about Use Cases.

## Framework: The Two Kinds of Business Rule

### Critical Business Rules → Entities

> *"Strictly speaking, business rules are rules or procedures that **make or save the business money**. **Very** strictly speaking, these rules would make or save the business money, **irrespective of whether they were implemented on a computer**. They would make or save money even if they were executed manually."*

The test in one example: *"The fact that a bank charges N% interest for a loan is a business rule that makes the bank money. **It doesn't matter if a computer program calculates the interest, or if a clerk with an abacus calculates the interest.**"*

- **Critical Business Data** — the data those rules need, which *"would exist even if the system were not automated"* (a loan needs a balance, an interest rate, a payment schedule).
- **Entity** *(Ivar Jacobson's term)* — *"an object within our computer system that embodies a small set of critical business rules operating on Critical Business Data."* It contains that data or has very easy access to it; its **interface consists of the functions implementing the Critical Business Rules**.

> *"This class stands alone as a representative of the business. It is **unsullied with concerns about databases, user interfaces, or third-party frameworks**. It could serve the business in any system, irrespective of how that system was presented, or how the data was stored, or how the computers in that system were arranged. **The Entity is pure business and nothing else.**"*

**On the word "class"**: *"Some of you may be concerned that I called it a class. Don't be. **You don't need to use an object-oriented language to create an Entity.** All that is required is that you bind the Critical Business Data and the Critical Business Rules together in a single and separate software module."*

### Application-Specific Rules → Use Cases

> *"Some business rules make or save money by **defining and constraining the way that an automated system operates**. These rules would not be used in a manual environment, because they make sense **only as part of an automated system**."*

**The worked example**: a bank does not want loan officers offering payment estimates until they have gathered and validated contact information and confirmed the candidate's credit score is 500 or higher. So the system will not proceed to the payment estimation screen until the contact screen is filled and verified and the score confirmed above the cutoff.

> *"A **use case** is a description of the way that an automated system is used. It specifies the input to be provided by the user, the output to be returned to the user, and the processing steps involved in producing that output."*

- *"Use cases contain the rules that specify **how and when** the Critical Business Rules within the Entities are invoked. **Use cases control the dance of the Entities.**"*
- *"A use case is an object. It has one or more functions that implement the application-specific business rules. It also has data elements that include the input data, the output data, and the references to the appropriate Entities."*

**What a use case must NOT describe:**
> *"The use case does not describe the user interface other than to informally specify the data coming in and the data going back out. **From the use case, it is impossible to tell whether the application is delivered on the web, or on a thick client, or on a console, or is a pure service.** This is very important… **How the data gets in and out of the system is irrelevant to the use cases.**"*

## Framework: Why Entities Are *Higher* Level Than Use Cases

This is the counterintuitive part, and Martin answers it directly:

> *"**Entities have no knowledge of the use cases that control them.** This is another example of the direction of the dependencies following the Dependency Inversion Principle. High-level concepts, such as Entities, know nothing of lower-level concepts, such as use cases. Instead, the lower-level use cases know about the higher-level Entities."*

> *"**Why are Entities high level and use cases lower level?** Because use cases are **specific to a single application** and, therefore, are **closer to the inputs and outputs** of that system. Entities are **generalizations that can be used in many different applications**, so they are farther from the inputs and outputs."*

> **"Use cases depend on Entities; Entities do not depend on use cases."**

## Framework: Request and Response Models

> *"A well-formed use case object should have **no inkling** about the way that data is communicated to the user, or to any other component. **We certainly don't want the code within the use case class to know about HTML or SQL!**"*

**The rule**: the use case class accepts **simple request data structures** as input and returns **simple response data structures** as output.

> *"These data structures **are not dependent on anything**. They do not derive from standard framework interfaces such as `HttpRequest` and `HttpResponse`. They know nothing of the web, nor do they share any of the trappings of whatever user interface might be in place."*

> *"**This lack of dependencies is critical.** If the request and response models are not independent, then the use cases that depend on them will be **indirectly bound** to whatever dependencies the models carry with them."*

**The specific temptation, named and refused:**
> *"You might be tempted to have these data structures contain **references to Entity objects**. You might think this makes sense because the Entities and the request/response models share so much data. **Avoid this temptation!** The purpose of these two objects is very different. **Over time they will change for very different reasons**, so tying them together in any way violates the Common Closure and Single Responsibility Principles. The result would be **lots of tramp data, and lots of conditionals** in your code."*

## Reference Table: Entity vs. Use Case

| | **Entity** | **Use Case** |
|---|---|---|
| Rules | Critical Business Rules | Application-specific rules |
| Would exist without a computer? | **Yes** — a clerk with an abacus | **No** — *"they make sense only as part of an automated system"* |
| Scope | *"Generalizations usable in many different applications"* | *"Specific to a single application"* |
| Distance from IO | Far → **higher level** | Closer → **lower level** |
| Knows about the other? | **No** | **Yes** — it *"controls the dance of the Entities"* |
| Knows about UI / DB / frameworks | Never | Never (only via request/response models) |

## Mental Models
- **Apply the abacus test.** If a clerk with an abacus could execute the rule and still make the business money, it's an Entity rule. If it only makes sense because a computer is involved, it's a use case rule.
- **"Higher level" means more reusable, not more important-sounding.** Entities outrank use cases because they're farther from IO and usable across applications.
- **A use case you can read without learning the delivery mechanism is a correct use case.** If you can tell it's a web app, the boundary has leaked.
- **Shared data is not a reason to share a type.** Request models and Entities look alike today and diverge tomorrow — the CCP/SRP argument from Ch 16's accidental duplication, applied to a specific, tempting case.

## Key Takeaways
1. Critical Business Rules make or save money regardless of automation; bind them with Critical Business Data into **Entities**.
2. Entities are pure business — no databases, no UI, no frameworks — and don't require an OO language.
3. Use cases are application-specific rules that control *how and when* Entities are invoked.
4. A use case must never reveal the delivery mechanism; IO is irrelevant to it.
5. Entities are higher level than use cases because they're general and far from IO; the dependency runs **use case → Entity**.
6. Pass simple, dependency-free request/response structures — never `HttpRequest`, never Entity references.
7. Business rules should be *"the most independent and reusable code in the system,"* with lesser concerns plugged into them.

## Connects To
- **Ch 19 (Policy and Level)**: distance-from-IO is exactly why Entities outrank use cases.
- **Ch 22 (The Clean Architecture)**: Entities and Use Cases are the two innermost circles.
- **Ch 16 (Independence)**: application-specific vs. application-independent business rules as horizontal layers.
- **Ch 23 (Presenters and Humble Objects)**: where the response model goes next.
- **Ivar Jacobson**, *Object Oriented Software Engineering* (Addison-Wesley, 1992): the source of both "Entity" and "use case."
