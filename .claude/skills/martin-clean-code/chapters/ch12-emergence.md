# Chapter 12: Emergence

*by Jeff Langr*

## Core Idea
Kent Beck's **Four Rules of Simple Design**, followed in priority order, cause good design to *emerge* — they let developers adhere to principles and patterns that otherwise take years to learn.

## The Framework: Four Rules of Simple Design

A design is "simple" if it, **in order of importance**:

1. **Runs all the tests**
2. **Contains no duplication**
3. **Expresses the intent of the programmer**
4. **Minimizes the number of classes and methods**

## Rule 1: Runs All the Tests

- *"A system might have a perfect design on paper, but if there is no simple way to verify that the system actually works as intended, then all the paper effort is questionable."*
- **Systems that aren't testable aren't verifiable. Arguably, a system that cannot be verified should never be deployed.**
- **The causal chain — why this rule alone improves design:**
  - Making systems testable **pushes toward small, single-purpose classes** — it is simply easier to test classes that conform to SRP.
  - **Tight coupling makes tests hard to write** — so the more tests you write, the more you reach for DIP, dependency injection, interfaces, and abstraction to minimize coupling.
  - *"Remarkably, following a simple and obvious rule that says we need to have tests and run them continuously impacts our system's adherence to the primary OO goals of low coupling and high cohesion. **Writing tests leads to better designs.**"*

## Rules 2–4: Refactoring

Once tests exist, you are empowered to keep the code clean by **incrementally refactoring**: for each few lines added, pause and reflect — *did we just degrade the design?* If so, clean it up and run the tests. **The tests eliminate the fear that cleaning up will break something.**

During the refactoring step you can apply the entire body of good-design knowledge: increase cohesion, decrease coupling, separate concerns, modularize system concerns, shrink functions and classes, choose better names — and apply the final three rules.

## Rule 2: No Duplication

**"Duplication is the primary enemy of a well-designed system."** It represents additional work, additional risk, and additional unnecessary complexity.

Forms of duplication, from obvious to subtle:
- Lines that look exactly alike.
- Lines that are *similar* — often they can be massaged to look more alike so they can be refactored.
- **Duplication of implementation** — two methods maintaining the same fact independently:

```java
int size() {}
boolean isEmpty() {}

// isEmpty could track its own boolean while size tracks a counter — duplicated state.
// Instead, tie one to the other:
boolean isEmpty() { return 0 == size(); }
```

**Reuse in the small.** Even a few lines are worth eliminating:

```java
// Before — the three-line image-replacement ritual appears twice.
public void scaleToOneDimension(float desiredDimension, float imageDimension) {
    if (Math.abs(desiredDimension - imageDimension) < errorThreshold) return;
    float scalingFactor = desiredDimension / imageDimension;
    scalingFactor = (float)(Math.floor(scalingFactor * 100) * 0.01f);

    RenderedOp newImage = ImageUtilities.getScaledImage(image, scalingFactor, scalingFactor);
    image.dispose();
    System.gc();
    image = newImage;
}

public synchronized void rotate(int degrees) {
    RenderedOp newImage = ImageUtilities.getRotatedImage(image, degrees);
    image.dispose();
    System.gc();
    image = newImage;
}

// After
public void scaleToOneDimension(float desiredDimension, float imageDimension) {
    if (Math.abs(desiredDimension - imageDimension) < errorThreshold) return;
    float scalingFactor = desiredDimension / imageDimension;
    scalingFactor = (float)(Math.floor(scalingFactor * 100) * 0.01f);

    replaceImage(ImageUtilities.getScaledImage(image, scalingFactor, scalingFactor));
}

public synchronized void rotate(int degrees) {
    replaceImage(ImageUtilities.getRotatedImage(image, degrees));
}

private void replaceImage(RenderedOp newImage) {
    image.dispose();
    System.gc();
    image = newImage;
}
```

**Why the small scale matters:** extracting commonality at this tiny level makes you *recognize SRP violations*. You may then move the extracted method to another class, which **elevates its visibility** — and a teammate may spot the chance to abstract it further and reuse it elsewhere. *"This 'reuse in the small' can cause system complexity to shrink dramatically. Understanding how to achieve reuse in the small is essential to achieving reuse in the large."*

## Worked Example: Template Method for Higher-Level Duplication

```java
// Before — two methods with the same three-step algorithm, differing in one step.
public class VacationPolicy {
    public void accrueUSDivisionVacation() {
        // code to calculate vacation based on hours worked to date
        // code to ensure vacation meets US minimums
        // code to apply vacation to payroll record
    }
    public void accrueEUDivisionVacation() {
        // code to calculate vacation based on hours worked to date
        // code to ensure vacation meets EU minimums
        // code to apply vacation to payroll record
    }
}

// After — Template Method. Subclasses fill in the "hole" in the algorithm,
// supplying only the bits that are not duplicated.
abstract public class VacationPolicy {
    public void accrueVacation() {
        calculateBaseVacationHours();
        alterForLegalMinimums();
        applyToPayroll();
    }
    private void calculateBaseVacationHours() { /* ... */ }
    abstract protected void alterForLegalMinimums();
    private void applyToPayroll() { /* ... */ }
}

public class USVacationPolicy extends VacationPolicy {
    @Override protected void alterForLegalMinimums() { /* US specific logic */ }
}

public class EUVacationPolicy extends VacationPolicy {
    @Override protected void alterForLegalMinimums() { /* EU specific logic */ }
}
```

## Rule 3: Expressive

**The economic argument**: the majority of a software project's cost is **long-term maintenance**. To minimize defects when introducing change, you must be able to understand what a system does. As complexity grows, understanding takes longer and misunderstanding becomes likelier. *"The clearer the author can make the code, the less time others will have to spend understanding it."*

The trap: *"It's easy to write code that we understand, because at the time we write it we're deep in an understanding of the problem. Other maintainers of the code aren't going to have so deep an understanding."*

**Five ways to be expressive:**

| Means | Detail |
|---|---|
| **Good names** | Hear a class or function name and **not be surprised** when you discover its responsibilities |
| **Small functions and classes** | Small units are easy to name, easy to write, easy to understand |
| **Standard nomenclature** | Design patterns are largely about communication — naming a class after **Command** or **Visitor** succinctly describes your design to other developers |
| **Well-written unit tests** | *"A primary goal of tests is to act as documentation by example."* A reader should get a quick understanding of what a class is about |
| **Trying** | The most important one — see below |

*"But the most important way to be expressive is to **try**. All too often we get our code working and then move on to the next problem without giving sufficient thought to making that code easy for the next person to read. Remember, the most likely next person to read the code will be you."*

*"So take a little pride in your workmanship… **Care is a precious resource.**"*

## Rule 4: Minimal Classes and Methods

**The self-limiting rule** — *"Even concepts as fundamental as elimination of duplication, code expressiveness, and the SRP can be taken too far."* In pursuing small classes and methods you might create too many tiny ones.

**Where the excess comes from**: *"High class and method counts are sometimes the result of pointless dogmatism."* Two named examples:
- A coding standard insisting on **an interface for each and every class**.
- Developers insisting that fields and behavior **must always** be separated into data classes and behavior classes.

*"Such dogma should be resisted and a more pragmatic approach adopted."*

**Priority reminder**: this is the **lowest priority** of the four rules. Keeping counts low matters — but *"it's more important to have tests, eliminate duplication, and express yourself."*

## Mental Models
- **The rules are ordered, and the order is load-bearing.** When two rules conflict, the earlier one wins: never sacrifice tests for a lower class count, never keep duplication to preserve expressiveness metrics.
- **Emergence, not prescription.** These rules don't tell you the design; followed continuously, they *produce* one. Rule 1 forces decoupling, Rule 2 forces abstraction, Rule 3 forces naming, Rule 4 forces restraint.
- **Practices don't replace experience** — *"Clearly not."* But they are *"a crystallized form of the many decades of experience enjoyed by the authors,"* and following them can encourage adherence to principles that otherwise take years to learn.

## Key Takeaways
1. Four rules, in priority order: runs all tests → no duplication → expresses intent → minimal classes and methods.
2. Testability is the first-order design force: it drives small classes (SRP) and loose coupling (DIP) automatically.
3. Refactor continuously, in the space of a few lines — tests make this safe by removing fear.
4. Hunt duplication at every scale, including duplicated *implementation* of the same fact.
5. Reuse in the small is the prerequisite for reuse in the large.
6. Use Template Method to remove higher-level duplication where an algorithm differs by one step.
7. Expressiveness comes from names, small units, standard pattern nomenclature, readable tests — and from actually trying.
8. Keep counts low, but never at the expense of the first three rules; resist dogma that manufactures classes.

## Connects To
- **Ch 1**: Beck's Rules of Simple Code appear via Ron Jeffries — this chapter is their full treatment.
- **Ch 3**: DRY as "the root of all evil in software"; small functions as the expressive unit.
- **Ch 9**: "Runs all the tests" is why F.I.R.S.T. and clean tests matter structurally, not just hygienically.
- **Ch 10**: SRP and the cohesion split are what Rule 2 makes you notice.
- **Ch 11**: "use the simplest thing that can possibly work" — Rule 4 at system scale.
- **Ch 17**: [G5] Duplication, [G16] Obscured Intent, [G25] Replace Magic Numbers with Named Constants.
- **XPE (Beck, 1999)**: the original four rules. **GOF**: Template Method, Command, Visitor.
