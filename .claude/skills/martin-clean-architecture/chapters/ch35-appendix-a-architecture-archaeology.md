# Appendix A: Architecture Archaeology

*Part VII: Appendix*

## Core Idea
> *"To unearth the principles of good architecture, let's take a **45-year journey** through some of the projects I have worked on since 1970. Some of these projects are interesting from an architectural point of view. Others are interesting because of the **lessons learned** and because of **how they fed into subsequent projects**."*

The value here is not the history — it's that every principle in this book has a scar behind it.

## The Projects, in Order

| Era | Project | What it was |
|---|---|---|
| Late 1960s | **Union Accounting System** | Teamsters Local 705 accounting on a **GE Datanet 30** |
| 1970s | **Laser Trim**, **Aluminum Die-Cast Monitoring** | Early industrial systems |
| 1970s–80s | **4-TEL** / **The Service Area Computer (SAC)** | Telephone line testing and craftsman dispatch |
| 1980s | **BOSS**, **pCCU**, **DLU/DRU**, **VRS** | C-language era; distributed telecom hardware |
| Late 1980s | **The Electronic Receptionist**, **Craft Dispatch System** | — |
| Early 1990s | **Clear Communications**, **ROSE**, **Architects Registry Exam** | The consulting years |

*"I also purposely stopped this history in the **early 1990s** — because I have **another book to write** about the events of the late 1990s."*

## Worked Example: The Grand Redesign in the Sky (SAC → C/UNIX)

**The setup, 1980s**: proprietary minicomputer architectures from the late 1960s were falling out of fashion — *"that, plus the **horrible architecture of the SAC software**, induced our technical management to start a complete re-architecture."*

The plan: rewrite in **C on UNIX**, first on an Intel 8086, later on custom 80286 hardware called **"Deep Thought."** A select group — **"The Tiger Team"** — was commissioned.

**Attempt 1**: *"I won't bore you with the details of the initial fiasco. Suffice it to say that the first Tiger Team **failed entirely after burning two or three man-years** on a software project that **never delivered anything**."*

**Attempt 2 (≈1982)**: *"It took years, then more years, and then even more years. **I don't know when the first UNIX-based SAC was finally deployed**; I believe I had left the company by then (1988). Indeed, **I'm not at all sure it ever was deployed.**"*

**The diagnosis, stated plainly:**
> *"**It is very difficult for a redesign team to catch up with a large staff of programmers who are actively maintaining the old system.**"*

**And the concrete example of why — the European fork.** Sales expanded to Europe; the redesign wasn't ready, so the old M365 systems were deployed there. But European phone systems, craft organization, and bureaucracies all differed, so a team in the UK **forked the U.S. code** and modified it.

The consequences, in order:
1. *"Bugs were found on both sides of the Atlantic that needed repair on the other side. But the modules had changed significantly, so it was **very difficult to determine whether the fix made in the United States would work in the United Kingdom**."*
2. After years of heartburn and a new high-throughput transatlantic line, a serious reintegration attempt was made — *"This effort **failed the first, second, and third times** it was tried. The two code bases, **though remarkably similar, were still too different to reintegrate** — especially in the rapidly changing market environment."*
3. *"Meanwhile, the 'Tiger Team'… realized that **it also had to deal with this European/US dichotomy**. And, of course, that **did nothing to accelerate their progress**."*

**This is Ch 1's Grand Redesign in the Sky, lived** — the redesign team racing a moving target, and losing.

## Worked Example: The Schedule Trap and the pCCU

**The trap**: *"There were always urgent matters that required us to **postpone development of the CCU/CMU architecture**. We felt safe about this decision because the phone companies were **consistently delaying the deployment of digital switches**… we felt confident that we had plenty of time, so we consistently delayed our development."*

**The call**: *"One of our customers is deploying a digital switch **next month**. We have to have a working CCU/CMU by then."*

> *"I was aghast! **How could we possibly do man-years of development in a month?** But my boss had a plan…"*

**The plan was to re-examine the requirement rather than the schedule.** The customer was tiny:
- One central office, two local distribution points
- The "local" distribution points held **ordinary analog switches** serving several hundred customers
- Those switches *"were of a kind that could be dialed by a normal COLT"*
- **The phone number itself encoded the routing**: *"If the phone number had a 5, 6, or 7 in a certain position, it went to distribution point 1; otherwise, it went to distribution point 2."*

So no CCU/CMU was needed — just a small computer at the central office, connected by modem to two standard COLTs, decoding the phone number and relaying commands.

> *"Thus was born the **pCCU**… **It took me about a week to develop.**"*

**Man-years → one week**, by discovering that the general architecture wasn't the requirement.

## Worked Example: The Reusability Lesson (Architects Registry Exam)

**The contract**: ETS, under contract to NCARB, automating the registration exam for *"the kind who design buildings."* Candidates solve architectural problems — a public library, a restaurant, a church — and draw the diagrams. Previously, senior architects were gathered as jurors to score submissions: *"big, expensive events and… the source of much ambiguity and delay."*

**The scope**: 18 test vignettes × 2 applications each = **36 applications**. *"The 18 GUI apps all used similar gestures and mechanisms. The 18 scoring applications all used the same mathematical techniques."*

**The plan**, sold to the client: *"We'd spend a **long time working on the first application**, but then **the rest would just pop out every few weeks**."*

**Martin's own warning to the reader at this point:**
> *"At this point you should be **face-palming or banging your head on this book**. Those of you who are old enough may remember the **'reuse' promise of OO**. We were all convinced, back then, that if you just wrote good clean object-oriented C++ code, you would just naturally produce lots and lots of reusable code."*

**Year 1**: two people, full time, on `Vignette Grande` — the most complicated of the batch. Result: **45,000 lines of framework, 6,000 lines of application.** Delivered; ETS contracted for the other 17.

**Then**: *"But something went wrong. We found that **the reusable framework we had created was not particularly reusable**. It did not fit well into the new applications being written. **There were subtle frictions that just didn't work.**"*

They told ETS the 45,000-line framework needed rewriting. *"I don't need to tell you that **ETS was not particularly happy** with this news."*

**Year 2 — the method that actually worked:**
> *"We set the old framework aside and began writing **four new vignettes simultaneously**. We would borrow ideas and code from the old framework but **rework them so that they fit into all four without modification**."*

Result: another 45,000-line framework **plus** four vignettes of 3,000–6,000 lines each. And the dependency structure came out right:

| Application type | Where the high-level policy lived |
|---|---|
| **GUI applications** | *"All the high-level GUI policy was **in the framework**. The vignette code was just glue"* — vignettes were **plugins to the framework** |
| **Scoring applications** | *"The high-level scoring policy was **in the vignette**. The **scoring framework plugged into the scoring vignette**"* |

> *"Of course, both of these applications were **statically linked C++** applications, so **the notion of plugin was nowhere in our minds**. And yet, **the way the dependencies ran was consistent with the Dependency Rule.**"*

**The outcome**: the next four *"started popping out the back end every few weeks, just as we had predicted."* The delay had cost nearly a year, so they hired another programmer. *"We met our dates and our commitments. Our customer was happy. We were happy. Life was good."*

**The lesson, stated as a rule:**
> **"You can't make a reusable framework until you first make a *usable* framework. Reusable frameworks require that you build them in concert with several reusing applications."**

## Mental Models
- **A redesign races a moving target it cannot catch.** The maintenance team keeps shipping; the rewrite team keeps falling behind. Both SAC attempts prove it.
- **Forks that "will be reintegrated later" are permanent.** Three failed attempts, on remarkably similar code bases, in a fast-moving market.
- **When the schedule collapses, re-examine the requirement, not the plan.** The pCCU was a week's work because someone asked what this customer actually needed.
- **Deferring architectural work because the external deadline keeps slipping is the schedule trap** — the slip ends without warning.
- **Reusability is discovered across N clients, never designed for one.** Build the framework in concert with several applications, or build it twice.
- **The Dependency Rule shows up even where "plugin" isn't a concept.** Statically linked C++ still ran its dependencies the right way — the rule is about direction, not mechanism.

## Key Takeaways
1. Every principle in this book has a project behind it; the appendix is the evidence.
2. The Grand Redesign in the Sky is not a hypothetical — one attempt burned 2–3 man-years delivering nothing, and the successor may never have shipped.
3. A redesign team cannot outrun a large team actively maintaining the old system.
4. A "temporary" fork across geographies becomes permanent divergence and multiplies the redesign's difficulty.
5. Beware the schedule trap: delaying architecture because an external date keeps slipping until it suddenly doesn't.
6. Question the requirement when the estimate is impossible — man-years became a week.
7. **You cannot build a reusable framework without first building a usable one, in concert with several reusing applications.**
8. Dependency direction is what matters; the plugin mechanism is incidental.

## Connects To
- **Ch 1**: the Grand Redesign in the Sky and the tiger-team race, described in principle — here in practice.
- **Ch 5, 12**: statically linked C++ obeying the Dependency Rule long before plugins were feasible.
- **Ch 22**: the framework/vignette relationship is the Dependency Rule at work.
- **Ch 32 (Frameworks Are Details)**: the reuse promise of OO, and why frameworks fight you as you outgrow them.
- **Ch 13 (Component Cohesion)**: *"early in the development of a project, the CCP is much more important than the REP, because developability is more important than reuse"* — the ETS story is that trade-off learned the hard way.
