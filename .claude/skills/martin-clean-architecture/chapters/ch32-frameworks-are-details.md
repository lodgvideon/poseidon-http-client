# Chapter 32: Frameworks Are Details

*Part VI: Details*

## Core Idea
**"Don't marry the framework!"** The relationship is an **asymmetric marriage** — you make a huge, long-term commitment; the framework author makes none to you.

## Framework: Why Framework Authors Aren't On Your Side

> *"Most framework authors offer their work for free because they want to be helpful to the community. They want to give back. **This is laudable.** However, regardless of their high-minded motives, **those authors do not have your best interests at heart. They can't, because they don't know you, and they don't know your problems.**"*

> *"Framework authors know **their own** problems, and the problems of their coworkers and friends. And they write their frameworks to solve **those** problems — not yours."*

**The fair concession**: *"Of course, your problems will likely overlap with those other problems quite a bit. **If this were not the case, frameworks would not be so popular.** To the extent that such overlap exists, frameworks can be very useful indeed."*

## Framework: The Asymmetric Marriage

> *"The relationship between you and the framework author is **extraordinarily asymmetric**. **You must make a huge commitment to the framework, but the framework author makes no commitment to you whatsoever.**"*

**How the documentation works on you:**
> *"The author, and other users of that framework, advise you on how to integrate your software with the framework. Typically, this means **wrapping your architecture around that framework**. The author recommends that you **derive from the framework's base classes**, and **import the framework's facilities into your business objects**. The author urges you to **couple your application to the framework as tightly as possible**."*

**And the incentives behind that advice — stated without accusation of malice, which makes it more persuasive:**
> *"For the framework author, coupling to his or her own framework is **not a risk**. The author **wants** to couple to that framework, because the author has **absolute control** over it."*

> *"What's more, **the author wants *you* to couple to the framework, because once coupled in this way, it is very hard to break away.** Nothing feels more validating to a framework author than a bunch of users **willing to inextricably derive from the author's base classes**."*

> *"In effect, the author is asking you to **marry** the framework — to make a huge, long-term commitment. And yet, **under no circumstances will the author make a corresponding commitment to you.** It's a **one-directional marriage**. You take on all the risk and burden; the framework author takes on nothing at all."*

## Reference Table: The Four Risks

| # | Risk | Detail |
|---|---|---|
| **1** | **Framework architecture is often not clean** | *"Frameworks tend to **violate the Dependency Rule**. They ask you to inherit their code into your business objects — **your Entities!** They want their framework coupled into that **innermost circle**. Once in, that framework isn't coming back out. **The wedding ring is on your finger; and it's going to stay there.**"* |
| **2** | **You outgrow it** | *"The framework may help you with some early features. However, as your product matures, **it may outgrow the facilities of the framework**. If you've put on that wedding ring, you'll find **the framework fighting you more and more** as time passes."* |
| **3** | **It evolves away from you** | *"You may be stuck **upgrading to new versions that don't help you**. You may even find **old features, which you made use of, disappearing** or changing in ways that are difficult for you to keep up with."* |
| **4** | **Something better arrives** | *"A new and better framework may come along **that you wish you could switch to**."* |

## Framework: The Solution — Date, Don't Marry

> **"Oh, you can *use* the framework — just don't *couple* to it. Keep it at arm's length. Treat the framework as a detail that belongs in one of the outer circles of the architecture. Don't let it into the inner circles."**

**The specific technique when a framework demands inheritance:**
> *"If the framework wants you to derive your business objects from its base classes, **say no! Derive proxies instead**, and keep those proxies in **components that are plugins to your business rules**."*

**The worked example — Spring, treated fairly:**
> *"Maybe you like Spring. **Spring is a good dependency injection framework.** Maybe you use Spring to auto-wire your dependencies. **That's fine, but you should not sprinkle `@autowired` annotations all throughout your business objects. Your business objects should not know about Spring.**"*

> *"Instead, you can use Spring to **inject dependencies into your `Main` component**. It's OK for `Main` to know about Spring since **`Main` is the dirtiest, lowest-level component** in the architecture."*

That is the whole rule in one line: **the framework may touch `Main`, and nothing further inward.**

## Framework: The Frameworks You Must Marry

> *"There are some frameworks that you simply **must** marry. If you are using C++, for example, you will likely have to marry **STL** — it's hard to avoid. If you are using Java, you will almost certainly have to marry the **standard library**."*

> *"**That's normal — but it should still be a *decision*.** You must understand that when you marry a framework to your application, **you will be stuck with that framework for the rest of the life cycle of that application**. For better or for worse, in sickness and in health, for richer, for poorer, forsaking all others, you will be using that framework. **This is not a commitment to be entered into lightly.**"*

> *"When faced with a framework, **try not to marry it right away. See if there aren't ways to date it for a while** before you take the plunge. Keep the framework behind an architectural boundary if at all possible, **for as long as possible**. Perhaps you can find a way to **get the milk without buying the cow**."*

## Mental Models
- **Read framework documentation as advocacy, not as architecture guidance.** The author's incentives favor coupling; yours don't.
- **The test is one question: can my Entities compile without the framework?** If they import it, inherit from it, or carry its annotations, you're married.
- **Proxies are the escape hatch when inheritance is demanded.** Derive from the framework in a plugin component; keep the business object clean.
- **A framework marriage should appear in a decision record.** STL and the JDK are fine to marry — but knowingly, and with the lifetime cost acknowledged.
- **`Main` is the quarantine ward.** DI frameworks, ORMs, and web frameworks may live there; they may not travel inward.

## Key Takeaways
1. Frameworks are not architectures, though some try to be.
2. Framework authors solve their own problems, not yours — however generous their motives.
3. The commitment is one-directional: you take all the risk; the author takes none.
4. Frameworks tend to violate the Dependency Rule by asking to live inside your Entities.
5. Four risks: unclean architecture, outgrowing it, unhelpful evolution, and a better alternative you can't reach.
6. Use frameworks; don't couple to them. When inheritance is demanded, derive proxies in plugin components.
7. Confine DI frameworks to `Main` — no `@autowired` in business objects.
8. Some marriages (STL, the standard library) are unavoidable — make them deliberately, knowing they last the application's lifetime.

## Connects To
- **Ch 21 (Screaming Architecture)**: *"Look at each framework with a jaded eye… develop a strategy that prevents the framework from taking over."*
- **Ch 26 (The Main Component)**: `Main` as the only place a DI framework may operate.
- **Ch 22**: the Dependency Rule that frameworks violate by entering the innermost circle.
- **Ch 10 (ISP)**: adopting framework F drags in its database D — the same coupling risk from a different angle.
- **Ch 30–31**: the database and the web as the sibling details.
- **Ch 8 (Boundaries) / Clean Code Ch 8**: wrapping third-party APIs and writing learning tests.
