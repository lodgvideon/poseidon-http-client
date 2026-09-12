# Chapter 19: Policy and Level

*Part V: Architecture*

## Core Idea
**"A strict definition of 'level' is 'the distance from the inputs and outputs.'"** Source code dependencies should be **decoupled from data flow and coupled to level** — low-level components always depend on high-level ones.

## Framework: Software as Policy

> *"Software systems are statements of policy. Indeed, at its core, that's all a computer program actually is. **A computer program is a detailed description of the policy by which inputs are transformed into outputs.**"*

That policy decomposes into many smaller statements — how business rules are calculated, how reports are formatted, how input data is validated.

> *"Part of the art of developing a software architecture is carefully separating those policies from one another, and **regrouping them based on the ways that they change**. Policies that change for the same reasons, and at the same times, are at the **same level** and belong together in the same component. Policies that change for different reasons, or at different times, are at **different levels** and should be separated."*

**The resulting structure**: *"forming the regrouped components into a **directed acyclic graph**. The nodes… are the components that contain policies at the same level. The directed edges are the dependencies between those components. **They connect components that are at different levels.**"*

**Which dependencies?** Source code, compile-time ones: Java `import`, C# `using`, Ruby `require`. *"They are the dependencies that are necessary for the compiler to function."*

> *"In every case, **low-level components are designed so that they depend on high-level components**."*

## Framework: The Definition of Level

> **"The farther a policy is from both the inputs and the outputs of the system, the higher its level. The policies that manage input and output are the lowest-level policies in the system."**

*(Meilir Page-Jones called the highest-level component the **"Central Transform"** in* The Practical Guide to Structured Systems Design*, 2nd ed., 1988.)*

## Worked Example: The Encryption Program

**The system** (Fig 19.1): read characters from an input device → translate them using a table → write the translated characters to an output device. `Translate` is the highest-level component *"because it is the component that is farthest from the inputs and outputs."*

**The critical observation**: *"the data flows and the source code dependencies **do not always point in the same direction**. This, again, is part of the art of software architecture."*

**The wrong architecture** — and it looks perfectly reasonable:

```java
function encrypt() {
    while (true)
        writeChar(translate(readChar()));
}
```

> *"This is incorrect architecture because **the high-level `encrypt` function depends on the lower-level `readChar` and `writeChar` functions**."*

**The right architecture** (Fig 19.2): an `Encrypt` class plus `CharReader` and `CharWriter` **interfaces**, enclosed by a dashed border. *"All dependencies crossing that border point **inward**. This unit is the highest-level element in the system."* `ConsoleReader` and `ConsoleWriter` are concrete classes — *"low level because they are close to the inputs and outputs."*

**What it buys**: *"This makes the encryption policy usable in a wide range of contexts. When changes are made to the input and output policies, they are not likely to affect the encryption policy."*

## Reference Table: Why Level Predicts Change

| Level | Distance from IO | Change frequency | Change urgency | Change importance |
|---|---|---|---|---|
| **Higher** (e.g. the encryption algorithm) | Far | *"Tend to change **less frequently**"* | Lower | *"For **more important** reasons"* |
| **Lower** (e.g. the IO devices) | Close | *"Tend to change **frequently**"* | *"With **more urgency**"* | *"But for **less important** reasons"* |

*"Even in the trivial example of the encryption program, it is far more likely that the IO devices will change than that the encryption algorithm will change. If the encryption algorithm does change, it will likely be for a more substantive reason."*

> *"Keeping these policies separate, with all source code dependencies pointing in the direction of the higher-level policies, **reduces the impact of change**. Trivial but urgent changes at the lowest levels of the system have little or no impact on the higher, more important, levels."*

**Restated as plugins** (Fig 19.3): *"lower-level components should be **plugins** to the higher-level components. The `Encryption` component knows nothing of the `IODevices` component; the `IODevices` component depends on the `Encryption` component."*

## Mental Models
- **Measure level by distance from IO, not by position in a call stack or a diagram.** The function that reads a character is low-level even when it's called first.
- **Data flow direction is not dependency direction.** Getting comfortable with them diverging is the skill this chapter teaches.
- **Urgency and importance are inversely correlated with level** — which is exactly the Ch 2 Eisenhower problem showing up structurally.
- **A one-line function can be an architectural error.** `writeChar(translate(readChar()))` is correct code and wrong architecture.

## Key Takeaways
1. A program *is* a statement of policy; architecture is the art of separating and regrouping those policies by how they change.
2. Level = distance from inputs and outputs. IO policies are always lowest.
3. Group same-level policies into components; connect different levels with directed edges — and keep the graph acyclic.
4. Couple source dependencies to **level**, not to **data flow**.
5. High-level policy must not name low-level functions; invert with interfaces so all dependencies cross the border inward.
6. Low-level policies change often, urgently, and for unimportant reasons — isolate them so that churn doesn't reach the important code.
7. Lower-level components are plugins to higher-level ones.

## Connects To
- **Ch 15**: policy vs. details; this chapter defines "level" precisely.
- **Ch 22**: the Dependency Rule generalizes "all dependencies point inward" to concentric circles.
- **Ch 20 (Business Rules)**: why Entities are higher level than use cases — Entities are farther from IO.
- **The chapter's own closing exercise**: *"This discussion has involved a mixture of the SRP, OCP, CCP, DIP, SDP, and SAP. Look back and see if you can identify where each principle was used, and why."*
