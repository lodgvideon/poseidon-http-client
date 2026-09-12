# Chapter 21: Screaming Architecture

*Part V: Architecture*

## Core Idea
**"So what does the architecture of your application scream?"** A top-level directory structure should announce *what the system does*, not *what framework built it*. **"Frameworks are tools to be used, not architectures to be conformed to."**

## Framework: The Blueprint Test

- Blueprints for a single-family residence: front entrance, foyer, living room, dining room, kitchen nearby, dinette, family room. *"There would be no question that you were looking at a single family home. **The architecture would scream: 'HOME.'**"*
- Blueprints for a library: grand entrance, check-in/out clerk area, reading areas, small conference rooms, *"gallery after gallery capable of holding bookshelves."* *"That architecture would scream: **'LIBRARY.'**"*

**The question turned on your own code:**
> *"When you look at the top-level directory structure, and the source files in the highest-level package, do they scream **'Health Care System,'** or **'Accounting System,'** or **'Inventory Management System'**? Or do they scream **'Rails,'** or **'Spring/Hibernate,'** or **'ASP'**?"*

## Framework: The Theme of an Architecture

From Ivar Jacobson's *Object Oriented Software Engineering* — and note the subtitle: ***A Use Case Driven Approach***.

> *"Jacobson makes the point that **software architectures are structures that support the use cases of the system**. Just as the plans for a house or a library scream about the use cases of those buildings, so should the architecture of a software application scream about the use cases of the application."*

> *"Architectures are not (or should not be) about frameworks. **Architectures should not be supplied by frameworks.** Frameworks are tools to be used, not architectures to be conformed to. **If your architecture is based on frameworks, then it cannot be based on your use cases.**"*

## Framework: The Purpose of an Architecture

> *"Good architectures are centered on use cases so that architects can safely describe the structures that support those use cases **without committing to frameworks, tools, and environments**."*

**The house analogy, extended into the actual argument:**
> *"The first concern of the architect is to make sure that the house is **usable** — not to ensure that the house is made of bricks. Indeed, the architect takes pains to ensure that **the homeowner can make decisions about the exterior material (bricks, stone, or cedar) later**, after the plans ensure that the use cases are met."*

> *"A good architecture makes it unnecessary to decide on Rails, or Spring, or Hibernate, or Tomcat, or MySQL, **until much later** in the project. A good architecture makes it **easy to change your mind** about those decisions, too."*

## Framework: But What About the Web?

> *"Is the web an architecture? Does the fact that your system is delivered on the web dictate the architecture of your system? **Of course not!** The web is a **delivery mechanism — an IO device** — and your application architecture should treat it as such."*

> *"Indeed, the decision that your application will be delivered over the web is **one that you should defer**. Your system architecture should be **as ignorant as possible about how it will be delivered**. You should be able to deliver it as a console app, or a web app, or a thick client app, or even a web service app, **without undue complication or change to the fundamental architecture**."*

## Framework: Frameworks Are Tools, Not Ways of Life

**Why the pressure exists** — and it's a fair description of how framework literature works:
> *"Framework authors often believe **very deeply** in their frameworks. The examples they write for how to use their frameworks are told from the point of view of a **true believer**. Other authors who write about the framework also tend to be **disciples of the true belief**. They show you the way to use the framework. Often they assume an **all-encompassing, all-pervading, let-the-framework-do-everything** position."*

> **"This is not the position you want to take."**

**The prescribed posture, as a checklist:**
- *"Look at each framework with a **jaded eye**. View it **skeptically**."*
- *"Yes, it might help, **but at what cost?**"*
- *"Ask yourself **how you should use it**, and **how you should protect yourself from it**."*
- *"Think about how you can **preserve the use-case emphasis** of your architecture."*
- *"**Develop a strategy that prevents the framework from taking over that architecture.**"*

## Framework: Testable Architectures — the falsifiable version of the claim

This is what makes "screaming" more than an aesthetic preference. If the architecture really is about use cases, a specific technical consequence follows:

> *"If your system architecture is all about the use cases, and if you have kept your frameworks at arm's length, then **you should be able to unit-test all those use cases without any of the frameworks in place**."*

| You should NOT need… | To run your tests |
|---|---|
| The **web server** running | ✗ |
| The **database** connected | ✗ |
| Framework machinery | ✗ |

> *"Your **Entity** objects should be plain old objects that have no dependencies on frameworks or databases or other complications. Your **use case** objects should coordinate your Entity objects. Finally, **all of them together should be testable in situ**, without any of the complications of frameworks."*

**This is the test to actually run**: try to unit-test a use case with nothing else booted. Whether it works tells you whether your architecture screams.

## Worked Example: The Onboarding Conversation

The chapter closes with the concrete outcome to aim for:

> *"If you are building a health care system, then when new programmers look at the source repository, their first impression should be, **'Oh, this is a health care system.'** Those new programmers should be able to **learn all the use cases of the system, yet still not know how the system is delivered.**"*

And when they notice the gap:

> *"'We see some things that look like models — **but where are the views and controllers?**'"*
>
> And you should respond:
>
> *"'Oh, those are **details that needn't concern us at the moment**. We'll decide about them later.'"*

## Mental Models
- **Read your own repo root as a stranger would.** The top-level package names are the loudest documentation you have; make them name the domain.
- **A framework's documentation is written by a believer.** Discount accordingly — and plan your protection before you plan your usage.
- **"Where are the controllers?" is a good sign, not a gap.** It means the delivery mechanism didn't colonize the structure.
- **Testability without infrastructure is the objective proof of a screaming architecture** — everything else is a matter of naming taste.

## Key Takeaways
1. Your top-level structure should announce the domain, not the framework.
2. Architecture supports use cases; frameworks are tools, and an architecture based on one cannot be based on your use cases.
3. Make the house usable before choosing the bricks — defer Rails/Spring/Hibernate/Tomcat/MySQL, and keep changing your mind cheap.
4. The web is an IO device and a deferrable delivery decision; you should be able to ship the same system as a console app.
5. Treat every framework skeptically: ask how to use it *and* how to protect yourself from it.
6. The concrete test: unit-test every use case with no web server, no database, no framework.
7. New programmers should learn all the use cases without learning the delivery mechanism.

## Connects To
- **Ch 15**: keeping options open — this chapter is that principle applied to frameworks specifically.
- **Ch 16 (Independence)**: *"a shopping cart application will look like a shopping cart application"* — the forward reference is to this chapter.
- **Ch 20 (Business Rules)**: use cases as first-class, framework-free objects.
- **Ch 22 (The Clean Architecture)**: the concentric structure that makes framework-free testing possible.
- **Ch 32 (Frameworks Are Details)**: the full argument, including the marriage-proposal analogy.
- **Ivar Jacobson**, *Object Oriented Software Engineering: A Use Case Driven Approach*.
