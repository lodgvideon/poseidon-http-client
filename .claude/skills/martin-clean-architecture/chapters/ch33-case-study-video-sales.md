# Chapter 33: Case Study — Video Sales

*Part VI: Details*

## Core Idea
The whole book applied to one product, showing **two dimensions of separation**: actors (SRP) and levels (the Dependency Rule). *"The different reasons correspond to the actors; the different rates correspond to the different levels of policy."*

## The Product

A website that sells videos — *"reminiscent of cleancoders.com, the site where I sell my software tutorial videos."*

| Rule | Detail |
|---|---|
| Individuals | Pay one price to **stream**, a higher price to **download and own permanently** |
| Businesses | **Streaming only**, purchased in batches with **quantity discounts** |
| Individuals | *"Typically act as **both** the viewers and the purchasers"* |
| Businesses | *"Often have people who **buy** the videos that **other people will watch**"* |

## Framework: Step 1 — Use Case Analysis

> *"Our first step in determining the initial architecture of the system is to **identify the actors and use cases**."*

**Four main actors**, each with distinct needs:

| Actor | Needs |
|---|---|
| **Viewer** | Watch videos; view catalog |
| **Purchaser** | Buy videos; view catalog |
| **Author** | *"Supply their video files, written descriptions, and ancillary files with **exams, problems, solutions, source code**, and other materials"* |
| **Administrator** | *"Add new video series, add and delete videos to and from the series, and **establish prices for various licenses**"* |

**Why the actor list *is* the architecture:**
> *"According to the **Single Responsibility Principle**, these four actors will be the **four primary sources of change** for the system. Every time some new feature is added, or some existing feature is changed, that step will be taken to serve **one** of these actors. Therefore we want to **partition the system such that a change to one actor does not affect any of the other actors**."*

**Scoping honesty**: *"The use cases shown are **not a complete list**. For example, you won't find log-in or log-out use cases. The reason for this omission is simply **to manage the size of the problem in this book**."*

**Abstract use cases** — Martin's own notation, shown as dashed:
> *"An **abstract use case** is one that **sets a general policy that another use case will flesh out**. `View Catalog as Viewer` and `View Catalog as Purchaser` both **inherit from** the `View Catalog` abstract use case."*

And the judgment call behind it, stated as a judgment call:
> *"On the one hand, **it was not strictly necessary** for me to create that abstraction. I could have left it out without compromising any features. On the other hand, **these two use cases are so similar that I thought it wise to recognize the similarity and find a way to unify it early in the analysis**."*

*(Footnote: "This is my own notation for 'abstract' use cases. It would have been more standard to use a UML stereotype such as `<<abstract>>`, but I don't find adhering to such standards very useful nowadays.")*

## Framework: Step 2 — Component Architecture

The partitioning has **two axes at once**:
- The familiar **views, presenters, interactors, controllers** partition
- *"I've broken each of those categories up by their **corresponding actors**"*

> *"Each of the components represents a **potential `.jar` file or `.dll` file**. Each will contain the views, presenters, interactors, and controllers that have been allocated to it."*

**How the abstract use case shows up in components**: special `Catalog View` and `Catalog Presenter` components. *"I assume that those views and presenters will be coded into **abstract classes** within those components, and that the **inheriting components will contain view and presenter classes that inherit from those abstract classes**."*

## Worked Example: Would You Really Ship All Those Jars?

The question every reader asks, answered directly:

> *"**Yes and no.** I would certainly **break the compile and build environment up this way**, so that I could build independent deliverables like that. **I would also reserve the right to combine all those deliverables into a smaller number** of deliverables if necessary."*

**Three concrete regroupings of the same source structure:**

| Grouping | Jars | Rationale |
|---|---|---|
| By layer | **5** — views, presenters, interactors, controllers, utilities | *"I could then independently deploy the components that are most likely to change independently of each other"* |
| Views+presenters together | **2** — (views + presenters), (interactors + controllers + utilities) | Groups the UI-facing half |
| Most primitive | **2** — (views + presenters), (everything else) | Minimum viable split |

> *"**Keeping these options open will allow us to adapt the way we deploy the system based on how the system changes over time.**"*

This is Ch 16's "decoupling mode is an option" made concrete: **the source partition is fine-grained; the deployment partition is chosen later and can change.**

## Framework: Dependency Management — reading the arrows

> *"The **flow of control** proceeds from **right to left**. Input occurs at the controllers, and that input is processed into a result by the interactors. The presenters then format the results, and the views display those presentations."*

> *"Notice that **the arrows do not all flow from the right to the left. In fact, most of them point from left to right.** This is because the architecture is following the **Dependency Rule**. All dependencies cross the boundary lines in one direction, and they **always point toward the components containing the higher-level policy**."*

**The two arrow types carry different meanings** — a detail worth keeping:

| Arrow | Meaning | Direction relative to control flow |
|---|---|---|
| **Open** | *using* relationship | **With** the flow of control |
| **Closed** | *inheritance* relationship | **Against** the flow of control |

> *"This depicts our use of the **Open-Closed Principle** to make sure that the dependencies flow in the right direction, and that **changes to low-level details do not ripple upward to affect high-level policies**."*

## Mental Models
- **Start from actors, not from entities or tables.** The actor list predicts where changes will originate, and therefore where the seams must be.
- **Partition source finely, deploy coarsely.** Building N components doesn't commit you to shipping N artifacts — but shipping one artifact from one component commits you forever.
- **Recognize near-duplicate use cases early and abstract them once** — while it's cheap, and while you can still see they're the same thing.
- **When arrows and control flow disagree, that's the design working**, not a diagramming error.

## Key Takeaways
1. Identify actors and use cases first; the actors are the primary sources of change.
2. Partition so a change serving one actor cannot affect another (SRP).
3. Abstract use cases capture general policy that concrete use cases flesh out — use them when similarity is obvious early.
4. Partition components along **both** axes: the view/presenter/interactor/controller roles **and** the actors.
5. Build independently, deploy flexibly — keep the jar grouping as a decision you can revisit.
6. Control flows right-to-left; most dependencies point left-to-right, toward higher-level policy.
7. Using relationships follow control flow; inheritance relationships oppose it — that's OCP doing its job.
8. Two dimensions of separation, one goal: *"separate components that change for different reasons, and at different rates."*

## Connects To
- **Ch 7 (SRP)**: actors as the definition of responsibility, driving the whole partition.
- **Ch 22**: views, presenters, interactors, and controllers with the Dependency Rule.
- **Ch 16 (Independence)**: deployment grouping as a deferred, reversible decision.
- **Ch 8 (OCP)**: the hierarchy of protection that the arrow directions implement.
- **Ch 34 (The Missing Chapter)**: how this diagram survives — or doesn't — contact with real package structure and access modifiers.
