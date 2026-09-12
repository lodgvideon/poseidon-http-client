# Chapter 13: Component Cohesion

*Part IV: Component Principles*

## Core Idea
Three principles decide **which classes belong in which component** — and **they fight each other**. Good architects find a position in the tension triangle that fits the team's *current* concerns, knowing those concerns will move.

## Framework: The Three Cohesion Principles

### REP — The Reuse/Release Equivalence Principle
> **"The granule of reuse is the granule of release."**

- **Why**: *"People who want to reuse software components cannot, and will not, do so unless those components are tracked through a release process and are given release numbers."*
- Not only for compatibility: developers *"need to know when new releases are coming, and which changes those new releases will bring."* It is common for a developer to see a release's changes and **decide to stay on the old one** — so the process must produce notifications and release documentation supporting that decision.
- **The design consequence**: the classes and modules in a component *"must belong to a cohesive group… there must be some overarching theme or purpose that those modules all share,"* and they **should be releasable together** — sharing a version number, release tracking, and documentation should *make sense* to author and users alike.
- **Martin's honesty about this one**: *"This is weak advice: Saying that something should 'make sense' is just a way of waving your hands in the air and trying to sound authoritative… **Weak though the advice may be, the principle itself is important, because violations are easy to detect — they don't 'make sense.'** If you violate the REP, your users will know, and they won't be impressed with your architectural skills."*

### CCP — The Common Closure Principle
> **"Gather into components those classes that change for the same reasons and at the same times. Separate into different components those classes that change at different times and for different reasons."**

- **This is SRP restated for components.** *"Just as the SRP says that a class should not contain multiple reasons to change, so the CCP says that a component should not have multiple reasons to change."*
- **The economic argument**: *"For most applications, **maintainability is more important than reusability**."* If changes are confined to one component, only that component is redeployed — *"other components that don't depend on the changed component do not need to be revalidated or redeployed."*
- **The binding test**: *"If two classes are so tightly bound, either physically or conceptually, that they always change together, then they belong in the same component."*
- **Relation to OCP**: it is *"'closure' in the OCP sense of the word"* that CCP addresses. Since **100% closure is not attainable, closure must be strategic** — design classes closed to the *most common* kinds of changes you expect or have experienced. CCP then **gathers into one component those classes closed to the same types of changes.**

**The shared sound bite for SRP and CCP:**
> *"Gather together those things that change at the same times and for the same reasons. Separate those things that change at different times or for different reasons."*

### CRP — The Common Reuse Principle
> **"Don't force users of a component to depend on things they don't need."**

- **What to put together**: *"Classes are seldom reused in isolation… reusable classes collaborate with other classes that are part of the reusable abstraction."* Example: **a container class and its associated iterators** — tightly coupled, reused together, so they belong together. Expect such a component to have *"classes that have lots of dependencies on each other."*
- **What to keep apart** — the sharper half: when component A uses component B, *"perhaps the using component uses only one class within the used component — **but that still doesn't weaken the dependency**."* Every change to B likely forces changes to A, and *"even if no changes are necessary… it will likely still need to be recompiled, revalidated, and redeployed. **This is true even if the using component doesn't care about the change.**"*
- **The rule that follows**: *"We want to make sure that the classes that we put into a component are **inseparable** — that it is impossible to depend on some and not on the others."*
- > *"Therefore the CRP tells us **more about which classes shouldn't be together** than about which classes should be together."*

**Relation to ISP**: *"The CRP is the generic version of the ISP. The ISP advises us not to depend on classes that have methods we don't use. The CRP advises us not to depend on components that have classes we don't use."*

> **Both reduce to: "Don't depend on things you don't need."**

## Worked Example: The Tension Diagram (Fig 13.1)

> *"You may have already realized that the three cohesion principles tend to fight each other."*

| Principle | Force | Effect on component size |
|---|---|---|
| **REP** | Inclusive | **Larger** |
| **CCP** | Inclusive | **Larger** |
| **CRP** | Exclusive | **Smaller** |

*"It is the tension between these principles that good architects seek to resolve."* The edges of the diagram describe **the cost of abandoning the principle on the opposite vertex**:

| Focus on… | …and you get |
|---|---|
| **REP + CRP** (abandoning CCP) | *"Too many components are impacted when simple changes are made"* |
| **CCP + REP** (abandoning CRP) | *"Too many unneeded releases are generated"* |

**The position moves over the project's life** — this is the practical payoff:

> *"Early in the development of a project, the **CCP is much more important than the REP**, because **developability is more important than reuse**."*

> *"Generally, projects tend to start on the **right hand side** of the triangle, where the only sacrifice is reuse. As the project matures, and other projects begin to draw from it, the project will **slide over to the left**."*

> *"This means that the component structure of a project can vary with time and maturity. **It has more to do with the way that project is developed and used, than with what the project actually does.**"*

## Mental Models
- **Cohesion is not "does one thing."** *"We once thought that cohesion was simply the attribute that a module performs one, and only one, function. However, the three principles of component cohesion describe a much more complex variety of cohesion."*
- **Pick your position on the triangle deliberately, and revisit it.** *"The balance is almost always dynamic. The partitioning that is appropriate today might not be appropriate next year."*
- **Expect the composition to jitter.** Components *"will likely jitter and evolve with time as the focus of the project changes from developability to reusability."*
- **Weak principles can still be useful when violations are loud.** REP can't be stated precisely, but you'll know when it's broken.

## Key Takeaways
1. REP: the granule of reuse is the granule of release — components need version numbers, release notes, and a coherent theme.
2. CCP: SRP for components — gather what changes together for the same reasons; maintainability usually beats reusability.
3. CRP: don't force users to depend on what they don't need — classes in a component should be inseparable.
4. CRP is ISP one level up; both say "don't depend on things you don't need."
5. REP and CCP inflate components; CRP shrinks them. The architect's job is choosing the balance.
6. Ignoring CCP → too many components hit by simple changes. Ignoring CRP → too many unneeded releases.
7. Early projects favor CCP (developability); mature, reused projects slide toward REP.

## Connects To
- **Ch 7 (SRP)**: CCP is its component-level form.
- **Ch 10 (ISP)**: CRP is its generic version.
- **Ch 8 (OCP)**: strategic closure is what CCP gathers on.
- **Ch 14 (Component Coupling)**: cohesion decides what goes in; coupling decides how components may relate.
- **Ch 27 (Services: Great and Small)**: "The Kitty Problem" is the cross-component change problem CCP exists to prevent.
- **Tim Ottinger**: credited with the tension diagram idea.
