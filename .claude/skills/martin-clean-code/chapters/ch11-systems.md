# Chapter 11: Systems

*by Dr. Kevin Dean Wampler*

## Core Idea
> *"Complexity kills. It sucks the life out of developers, it makes products difficult to plan, build, and test."* — Ray Ozzie, CTO, Microsoft

Cities work because they have evolved appropriate levels of abstraction and modularity — individuals manage components effectively **without understanding the big picture**. Software systems need the same separation of concerns at the system level, and the key one is **separating construction from use**.

## Frameworks Introduced

- **Separate Constructing a System from Using It**
  - Analogy: a hotel under construction has cranes, hard hats, and an external elevator; a year later they are gone and both the building and the people in it look entirely different. **Construction is a very different process from use.**
  - Rule: separate the **startup process** — where application objects are constructed and dependencies are wired — from the **runtime logic** that takes over afterward.

- **Separation of Main** — move all construction to `main` or modules called by `main`, and design the rest of the system assuming everything is built and wired.
  - The invariant: **all dependency arrows cross the barrier in one direction, pointing away from `main`.** The application has no knowledge of `main` or of the construction process.

- **Abstract Factory** [GOF] — for when the application must control *when* an object is created (e.g. `LineItem` instances added to an `Order`), while keeping the *details* of construction outside the application. The `LineItemFactoryImplementation` lives on the `main` side of the line; the application stays decoupled from how a `LineItem` is built, yet controls timing and can pass application-specific constructor arguments.

- **Dependency Injection (DI)** — Inversion of Control applied to dependency management.
  - **IoC moves secondary responsibilities from an object to other objects dedicated to the purpose, thereby supporting SRP.** An object should not instantiate its own dependencies; it passes that responsibility to an authoritative mechanism — usually `main` or a special-purpose container.
  - **JNDI is only partial DI**: `jndiContext.lookup("NameOfMyService")` — the object doesn't control what is returned, but it *still actively resolves* the dependency.
  - **True DI**: the class is **completely passive**. It exposes setter methods or constructor arguments; the container instantiates and wires. Which objects are used is specified in configuration or a special-purpose construction module.
  - On lazy initialization under DI: most containers won't construct until needed, and many offer factory invocation or proxies for lazy evaluation. *"Don't forget that lazy instantiation/evaluation is just an optimization and perhaps premature!"*

- **Cross-Cutting Concerns and AOP** — concerns like persistence **cut across the natural object boundaries of a domain**. You want one strategy (one DBMS, consistent naming, consistent transactional semantics) — yet in practice the same code gets spread across many objects.
  - *"The persistence framework might be modular and our domain logic, in isolation, might be modular. The problem is the fine-grained intersection of these domains."*
  - **Aspects** are modular constructs specifying which points in the system have their behavior modified in a consistent way, applied **noninvasively** (no manual editing of target source).

- **Test Drive the System Architecture** — if domain logic is written as POJOs decoupled from architecture concerns, **you can truly test drive your architecture**, evolving it from simple to sophisticated by adopting technologies on demand.
  - **BDUF (Big Design Up Front) is harmful** — it inhibits adaptation both because of psychological resistance to discarding prior effort and because early architecture choices shape all subsequent design thinking.
  - Why software differs from buildings: architects must do BDUF because radical change to a physical structure mid-construction isn't feasible. Software has its own physics, but **radical change is economically feasible if concerns are separated effectively.**
  - Not rudderless: keep expectations of scope, goals, schedule, and general structure — *"However, we must maintain the ability to change course in response to evolving circumstances."*

- **Optimize Decision Making** — give responsibilities to the most qualified people, and **postpone decisions until the last possible moment**. *"This isn't lazy or irresponsible; it lets us make informed choices with the best possible information. A premature decision is a decision made with suboptimal knowledge."*

- **Use Standards Wisely, When They Add Demonstrable Value** — standards ease reuse, recruiting, and wiring. But the process of creating them can take too long for industry to wait, and some lose touch with adopters' real needs. Many teams used EJB2 *because it was a standard* when lighter designs sufficed; Martin has seen teams *"become obsessed with various strongly hyped standards and lose focus on implementing value for their customers."*

- **Systems Need Domain-Specific Languages** — small scripting languages or APIs that let code read like structured prose a domain expert might write.
  - Why: a good DSL **minimizes the communication gap** between a domain concept and the code implementing it. *"If you are implementing domain logic in the same language that a domain expert uses, there is less risk that you will incorrectly translate the domain into the implementation."*
  - DSLs let all levels of abstraction and all domains be expressed as POJOs, from high-level policy to low-level detail.

## The Chapter's Thesis, Verbatim
> An optimal system architecture consists of modularized domains of concern, each of which is implemented with Plain Old Java (or other) Objects. The different domains are integrated together with minimally invasive Aspects or Aspect-like tools. This architecture can be test-driven, just like the code.

## Anti-patterns

**Lazy Initialization mixed into runtime logic:**

```java
public Service getService() {
    if (service == null)
        service = new MyServiceImpl(...);  // Good enough default for most cases?
    return service;
}
```

The idiom has merits — no construction overhead until use, faster startup, never returns null. But:
- **Hard-coded dependency** on `MyServiceImpl` and everything its constructor needs. You cannot compile without resolving them, *"even if we never actually use an object of this type at runtime!"*
- **Testing problem**: a heavyweight `MyServiceImpl` requires assigning a Test Double or Mock to `service` before the call — and since construction is mixed with runtime processing, **all execution paths must be tested**, including the null branch.
- **SRP violation in the small** — the method has two responsibilities.
- **Worst of all**: *"Why does the class with this method have to know the global context? … Is it even possible for one type to be right for all possible contexts?"*
- One occurrence isn't serious — but applications contain **many** such idioms, so the global setup strategy ends up *"scattered across the application, with little modularity and often significant duplication."*

**EJB2 — an architecture that blocks organic growth.** An Entity Bean required a local/remote interface, an implementation class with abstract accessors plus container lifecycle methods, and XML deployment descriptors:

```java
public abstract class Bank implements javax.ejb.EntityBean {
    // Business logic...
    public abstract String getStreetAddr1();
    public abstract Collection getAccounts();

    public void addAccount(AccountDTO accountDTO) {
        InitialContext context = new InitialContext();
        AccountHomeLocal accountHome = context.lookup("AccountHomeLocal");
        AccountLocal account = accountHome.create(accountDTO);
        Collection accounts = getAccounts();
        accounts.add(account);
    }

    // EJB container logic — required, and usually empty:
    public void setEntityContext(EntityContext ctx) {}
    public void unsetEntityContext() {}
    public void ejbActivate() {}
    public void ejbPassivate() {}
    public void ejbLoad() {}
    public void ejbStore() {}
    public void ejbRemove() {}
}
```

The damage: business logic **tightly coupled to the container** (you must subclass container types and implement lifecycle methods); isolated unit testing is impractical (mock the container — hard — or deploy to a real server); reuse outside EJB2 is effectively impossible; **even OO is undermined** — one bean cannot inherit from another, forcing DTOs that are structs with no behavior, redundant types holding the same data, and boilerplate to copy between them.

**Java Proxies** — workable but not the answer. The JDK's dynamic proxies **only work with interfaces**; proxying classes needs byte-code libraries (CGLIB, ASM, Javassist). Even a minimal `BankProxyHandler` requires reflection-based method-name dispatch:

```java
public class BankProxyHandler implements InvocationHandler {
    private Bank bank;
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();
        if (methodName.equals("getAccounts")) {
            bank.setAccounts(getAccountsFromDatabase());
            return bank.getAccounts();
        } else if (methodName.equals("setAccounts")) {
            bank.setAccounts((Collection<Account>) args[0]);
            setAccountsToDatabase(bank.getAccounts());
            return null;
        } else { ... }
    }
}
```
*"This code 'volume' and complexity are two of the drawbacks of proxies. They make it hard to create clean code!"* And proxies provide no mechanism for specifying system-wide execution points — which a true AOP solution requires.

## Worked Example: EJB2 → EJB3 (the POJO endpoint)

Spring's model — POJOs plus declarative configuration — was so compelling it drove a complete overhaul of the EJB standard:

```java
@Entity
@Table(name = "BANKS")
public class Bank implements java.io.Serializable {
    @Id @GeneratedValue(strategy=GenerationType.AUTO)
    private int id;

    @Embeddable   // An object 'inlined' in Bank's DB row
    public class Address {
        protected String streetAddr1;
        protected String city;
        protected String state;
        protected String zipCode;
    }
    @Embedded private Address address;

    @OneToMany(cascade = CascadeType.ALL, fetch = FetchType.EAGER, mappedBy="bank")
    private Collection<Account> accounts = new ArrayList<Account>();

    public void addAccount(Account account) {
        account.setBank(this);
        accounts.add(account);
    }
}
```

*"Because none of that information is outside of the annotations, the code is clean, clear, and hence easy to test drive, maintain, and so on."* The persistence details can move to XML descriptors for a truly pure POJO; teams whose mappings rarely change may keep annotations — *"but with far fewer harmful drawbacks compared to the EJB2 invasiveness."*

**The Spring "Russian doll" of decorators**: each bean wraps the next — a `Bank` domain object proxied by a DAO, itself proxied by a JDBC data source. The client believes it calls `getAccounts()` on a `Bank`; it is actually talking to the outermost of a set of nested **Decorators** [GOF]. Adding transactions or caching means adding another decorator. And the application code needed is two lines:

```java
XmlBeanFactory bf =
    new XmlBeanFactory(new ClassPathResource("app.xml", getClass()));
Bank bank = (Bank) bf.getBean("bank");
```

*"Because so few lines of Spring-specific Java code are required, the application is almost completely decoupled from Spring, eliminating all the tight-coupling problems of systems like EJB2."*

## Reference Table: AOP Mechanisms in Java

| Mechanism | Fit | Cost |
|---|---|---|
| **Java Proxies** | Simple cases — wrapping method calls in individual objects | Verbose, complex, interfaces-only (byte-code libs otherwise), no system-wide pointcuts |
| **Pure Java AOP** (Spring AOP, JBoss AOP) | **80–90% of cases where aspects are useful**; POJOs + declarative config | XML can be verbose and hard to read |
| **AspectJ** | The most full-featured — first-class aspects as modularity constructs | Must adopt new tools, language constructs, and idioms (mitigated by the annotation form) |

## Mental Models
- **Cities scale by growing, not by being right first.** Roads start narrow and get widened; services arrive as density increases. *"Who can justify the expense of a six-lane highway through the middle of a small town?"* — **it is a myth that we can get systems right the first time.** Implement today's stories, then refactor and expand for tomorrow's.
- **A good API should largely disappear from view** most of the time, so the team spends its creative effort on the user stories. Even well-designed APIs can be overkill when not really needed.
- **Use the simplest thing that can possibly work** — at every level, whether designing systems or individual modules.

## Key Takeaways
1. Separate construction from use: wire objects in `main` (or a container), and let the application assume everything is built.
2. All dependencies should point away from `main`; the application must not know how it was assembled.
3. Prefer true DI over service lookup — a class should be passive about its dependencies.
4. Cross-cutting concerns need aspect-like mechanisms; POJOs plus declarative configuration beat invasive container inheritance.
5. Architecture can grow incrementally when concerns are properly separated — BDUF is not required and is actively harmful.
6. Postpone decisions until the last responsible moment; a premature decision is made with suboptimal knowledge.
7. Adopt standards for demonstrable value, not for their own sake.
8. Build DSLs to close the gap between domain concepts and the code implementing them.

## Connects To
- **Ch 10**: SRP, OCP, and DIP scaled from classes to systems; DI is DIP with an authoritative wiring mechanism.
- **Ch 9**: architecture that can be test-driven is architecture whose parts can be stubbed — the `Portfolio`/`StockExchange` pattern at system scale.
- **Ch 6**: EJB2's DTOs are the data-structure side of the object/data-structure dichotomy.
- **Ch 8**: the boundary discipline applied to whole frameworks rather than single libraries.
- **Ch 12**: "use the simplest thing that can possibly work" is Emergent Design at the architecture level.
- **GOF**: Abstract Factory, Decorator. **Fowler**: *Inversion of Control Containers and the Dependency Injection pattern*. **Mezzaros07**: Test Doubles.
