# Chapter 25: Layers and Boundaries

*Part V: Architecture*

## Core Idea
**"Architectural boundaries exist everywhere."** The architect's job is to guess intelligently which ones to build, then **watch** — implementing each boundary *"right at the inflection point where the cost of implementing becomes less than the cost of ignoring."*

## Framework: The API Ownership Rule

The chapter's most transferable technical rule, stated while walking through the diagram:

> **"The API is defined and owned by the *user*, rather than by the implementer."**

> *"`GameRules` communicates with `Language` through an API that **`GameRules` defines and `Language` implements**. `Language` communicates with `TextDelivery` using an API that **`Language` defines but `TextDelivery` implements**."*

And inside each component, the inversion is **reciprocal**: *"we would find polymorphic Boundary interfaces used by the code inside `GameRules` and implemented by the code inside the `Language` component. We would **also** find polymorphic Boundary interfaces used by `Language` and implemented by code inside `GameRules`."*

Variations plug into the abstract API components: `English` and `Spanish` implement interfaces defined in `Language`; `SMS` and `CloudData` implement theirs.

## Worked Example: Hunt the Wumpus

**The setup**: the 1972 text adventure — `GO EAST`, `SHOOT WEST` — hunting a Wumpus through caverns while avoiding traps and pits.

*(Martin's own footnote guards the example: "It should be just as clear that **we would not apply the clean architecture approach to something as trivial as this game**. After all, the entire program can probably be written in 200 lines of code or less. In this case, we're using a simple program as a **proxy for a much larger system** with significant architectural boundaries.")*

**Boundary 1 — language.** Keep the text UI but decouple it so the game ships in different markets. *"The game rules will communicate with the UI component using a **language-independent API**, and the UI will translate the API into the appropriate human language."* Result: *"any number of UI components can reuse the same game rules. The game rules do not know, nor do they care, which human language is being used."*

**Boundary 2 — persistence.** State might live in flash, in the cloud, or just in RAM. *"In any of those cases, we don't want the game rules to know the details."*

**Boundary 3 — the one that's easy to miss.** *"**Language is not the only axis of change for the UI.** We also might want to vary the mechanism by which we communicate the text… a normal shell window, or text messages, or a chat application."* That is a second axis, and therefore a second potential boundary — so an API separates `Language` from `TextDelivery`.

**Reading the resulting diagram (Fig 25.4):**
- All arrows point **up**, putting `GameRules` at the top — *"this orientation makes sense because `GameRules` is the component that contains the **highest-level policies**."*
- *(Footnote: "If you are confused by the direction of the arrows, remember that they point in the direction of **source code dependencies, not in the direction of data flow**." And: the top component would once have been called the **Central Transform** — Page-Jones, 1988.)*

**The flow of information**, which runs opposite to the arrows:
1. Input arrives from the user through **`TextDelivery`** (bottom left)
2. It rises through **`Language`**, *"getting translated into commands to `GameRules`"*
3. **`GameRules`** processes it and sends data down to **`DataStorage`** (lower right)
4. `GameRules` sends output back down to `Language`, which translates it and delivers it through `TextDelivery`

> *"This organization effectively divides the flow of data into **two streams**. The stream on the left is concerned with **communicating with the user**, and the stream on the right is concerned with **data persistence**. Both streams meet at the top at `GameRules`."*

## Framework: Crossing and Splitting the Streams

**Crossing** — *"Are there always two data streams? **No, not at all.**"* Add multiplayer over the net and a network component gives you **three** streams, all controlled by `GameRules`. *"As systems become more complex, the component structure may split into many such streams."*

**Splitting** — the more interesting case. *"At this point you may be thinking that all the streams eventually meet at the top in a single component. **If only life were so simple!**"*

Inside `GameRules` there are actually **two levels of policy**:

| Policy | Concerns | Level |
|---|---|---|
| **MoveManagement** | *"The **mechanics of the map**"* — how caverns connect, which objects are where, how to move the player, what events they must deal with | Lower |
| **PlayerManagement** | *"The **health** of the player, and the **cost or benefit** of a particular event"* — losing health, gaining it by finding food; *"eventually that policy would decide whether the player **wins or loses**"* | Higher |

The lower level *declares events* upward — **`FoundFood`**, **`FellInPit`** — and the higher level manages player state.

**Is that a boundary?** Make it concrete: in a *massive multiplayer* version, **`MoveManagement` runs locally on the player's computer while `PlayerManagement` runs on a server**, offering a micro-service API to all connected `MoveManagement` components.

> *"**A full-fledged architectural boundary exists between `MoveManagement` and `PlayerManagement`** in this case."*

## Framework: The Watchful Eye

The chapter's conclusion is a genuine dilemma, and Martin refuses to resolve it cheaply:

> *"On the one hand, some very smart people have told us that we should **not anticipate the need for abstraction**. This is the philosophy of YAGNI. There is wisdom in this message, since **over-engineering is often much worse than under-engineering**. On the other hand, when you discover that you truly do need an architectural boundary where none exists, **the costs and risks can be very high** to add such a boundary."*

And note the sharp qualifier on the "add it later" escape hatch:
> *"When such boundaries are ignored, they are **very expensive to add in later — even in the presence of comprehensive test-suites and refactoring discipline**."*

**The prescribed practice, in order:**
1. *"You must **see the future**. You must **guess — intelligently**."*
2. *"Weigh the costs and determine where the architectural boundaries lie, and which should be **fully** implemented, which **partially**, and which **ignored**."*
3. **"But this is not a one-time decision.** You don't simply decide at the start of a project."
4. *"Rather, **you watch**. You pay attention as the system evolves. You note where boundaries may be required, and then **carefully watch for the first inkling of friction** because those boundaries don't exist."*
5. *"At that point, you weigh the costs of implementing versus the cost of ignoring — **and you review that decision frequently**."*

> *"Your goal is to implement the boundaries **right at the inflection point where the cost of implementing becomes less than the cost of ignoring**. **It takes a watchful eye.**"*

## Mental Models
- **One component can hide several axes of change.** The UI's language and its delivery mechanism vary independently — finding the *second* axis is the skill.
- **Whoever uses the API owns it.** Interfaces belong to the caller's component, not the implementer's. This is what makes the dependency point the right way.
- **"Everything meets at the top" is a simplification.** High-level policy itself stratifies, and those strata can end up on different machines.
- **Watch for friction, not for milestones.** The signal to build a boundary is felt pain, reviewed frequently — not a phase in a plan.
- **Over-engineering is worse than under-engineering — but late boundaries are expensive.** Hold both; that tension is the job.

## Key Takeaways
1. Architectural boundaries exist everywhere; recognizing which ones matter is the architect's work.
2. Full boundaries are expensive to build — and boundaries ignored are expensive to retrofit, even with good tests.
3. The API is defined and owned by the *user* component, implemented by the downstream one, with reciprocal interfaces inside.
4. Look for multiple axes of change within one component (language *and* delivery mechanism).
5. Data flow splits into streams (UI, persistence, network) that meet at the highest-level policy.
6. High-level policy itself splits: `MoveManagement` reports events like `FoundFood` upward to `PlayerManagement`.
7. Deployment topology can turn an internal split into a full boundary — local client vs. server micro-service.
8. Don't decide boundaries once. Watch, feel friction, and implement at the inflection point.

## Connects To
- **Ch 24 (Partial Boundaries)**: the graduated options you choose among while watching.
- **Ch 22**: the Dependency Rule applied to a multi-stream, multi-level system.
- **Ch 19 (Policy and Level)**: why `GameRules` sits at the top and the Central Transform reference.
- **Ch 27 (Services: Great and Small)**: what happens when the boundary becomes a micro-service.
- **Meilir Page-Jones**, *Practical Guide to Structured Systems Design*, 2nd ed., 1988.
