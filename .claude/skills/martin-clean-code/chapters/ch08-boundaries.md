# Chapter 8: Boundaries

*by James Grenning*

## Core Idea
**"It's better to depend on something you control than on something you don't control, lest it end up controlling you."** Manage third-party code by having very few places that refer to it — wrap it, adapt it, and pin your understanding of it with tests.

## Frameworks Introduced

- **The Provider/User Tension** — providers of third-party packages strive for **broad applicability** (many environments, wide audience); users want an interface **focused on their particular needs**. This tension is what causes problems at boundaries.

- **Don't Pass Boundary Interfaces Around**
  - Rule: if you use a boundary interface like `Map`, **keep it inside the class, or close family of classes, where it is used. Avoid returning it from, or accepting it as an argument to, public APIs.**
  - Not the rule: *"We are not suggesting that every use of `Map` be encapsulated in this form."*
  - Why: passing `Map<Sensor>` liberally means many places to fix if `Map` ever changes — and it *did* change, when generics arrived in Java 5. Martin has seen systems unable to adopt generics because of the sheer magnitude of changes required.

- **Learning Tests** (Jim Newkirk's term, via Beck's *TDD*)
  - When to use: before integrating any unfamiliar third-party library.
  - How: instead of experimenting inside production code, write tests that call the third-party API **exactly as you expect to use it**. These are controlled experiments checking your understanding, focused on what *you* want out of the API.
  - Why: *"Learning the third-party code is hard. Integrating the third-party code is hard too. Doing both at the same time is doubly hard."*

- **Learning Tests Are Better Than Free**
  - They cost nothing — you had to learn the API anyway, and this was an easy, isolated way to do it.
  - They have **positive ROI**: run them against each new release to detect behavioral differences immediately. Third-party authors face their own pressures to change; each release carries risk.
  - Corollary: **even when you don't need the learning, a clean boundary should have outbound tests** exercising the interface the way production code does. Without them, *"we might be tempted to stay with the old version longer than we should."*

- **Using Code That Does Not Yet Exist** — the boundary between the known and the unknown.
  - How: **define the interface you wish you had.** It is under your control, which keeps client code readable and focused on what it is trying to accomplish.
  - Then: when the real API arrives, write an **Adapter** [GOF] to bridge the gap. The Adapter encapsulates the interaction and gives you a single place to change when the API evolves.
  - Bonus: this creates a **seam** [WELC] for testing — a `FakeTransmitter` lets you test the controller before the real API exists.

## Worked Example: Encapsulating `java.util.Map`

`Map` has a very broad interface. Two concrete liabilities:
- `clear()` sits right at the top of the method list — **any** recipient of your `Map` can wipe it, even if your design intends otherwise.
- Maps do not reliably constrain the types placed within them — any determined user can add anything.

```java
// Raw Map — every client must cast, over and over throughout the code.
Map sensors = new HashMap();
...
Sensor s = (Sensor)sensors.get(sensorId);

// Generics improve readability — but Map<Sensor> still offers far more capability
// than we need or want, and it is still passed around.
Map<Sensor> sensors = new HashMap<Sensor>();
...
Sensor s = sensors.get(sensorId);

// Encapsulated — the boundary interface is hidden inside Sensors.
public class Sensors {
    private Map sensors = new HashMap();

    public Sensor getById(String id) {
        return (Sensor) sensors.get(id);
    }
    //snip
}
```

**What this buys:**
- No user of `Sensors` cares whether generics are used — *"That choice has become (and always should be) an implementation detail."*
- The `Map` interface can evolve with very little impact on the rest of the application.
- The interface is **tailored and constrained** to the application's needs — easier to understand, harder to misuse.
- `Sensors` can now **enforce design and business rules** (e.g. nobody gets to `clear()`).

## Worked Example: Learning log4j

The chapter walks the actual discovery loop — each failure teaches one fact:

```java
// 1. Naive first attempt, from the intro docs. Expect "hello" on the console.
@Test
public void testLogCreate() {
    Logger logger = Logger.getLogger("MyLogger");
    logger.info("hello");
}
// → Error: we need something called an Appender.

// 2. Add a ConsoleAppender.
@Test
public void testLogAddAppender() {
    Logger logger = Logger.getLogger("MyLogger");
    ConsoleAppender appender = new ConsoleAppender();
    logger.addAppender(appender);
    logger.info("hello");
}
// → The Appender has no output stream. "Odd — it seems logical that it'd have one."

// 3. After some googling — this one works.
@Test
public void testLogAddAppender() {
    Logger logger = Logger.getLogger("MyLogger");
    logger.removeAllAppenders();
    logger.addAppender(new ConsoleAppender(
        new PatternLayout("%p %t %m%n"), ConsoleAppender.SYSTEM_OUT));
    logger.info("hello");
}
```

Then the probing continues: removing `ConsoleAppender.SYSTEM_OUT` still prints; removing `PatternLayout` brings back the missing-stream complaint. *"This is very strange behavior."* The docs reveal the default `ConsoleAppender` constructor is "unconfigured" — *"This feels like a bug, or at least an inconsistency, in log4j."*

The knowledge is then **encoded as tests**, not as tribal memory:

```java
public class LogTest {
    private Logger logger;

    @Before
    public void initialize() {
        logger = Logger.getLogger("logger");
        logger.removeAllAppenders();
        Logger.getRootLogger().removeAllAppenders();
    }

    @Test
    public void basicLogger() {
        BasicConfigurator.configure();
        logger.info("basicLogger");
    }

    @Test
    public void addAppenderWithStream() {
        logger.addAppender(new ConsoleAppender(
            new PatternLayout("%p %t %m%n"), ConsoleAppender.SYSTEM_OUT));
        logger.info("addAppenderWithStream");
    }

    @Test
    public void addAppenderWithoutStream() {
        logger.addAppender(new ConsoleAppender(
            new PatternLayout("%p %t %m%n")));
        logger.info("addAppenderWithoutStream");
    }
}
```

With that knowledge captured, it is encapsulated into an own logger class **so the rest of the application is isolated from the log4j boundary interface**.

## Worked Example: The Transmitter That Didn't Exist Yet

Radio communications system; the "Transmitter" subsystem was undefined and its owners had not designed the interface. Rather than block, the team started work far from the unknown.

Working against the boundary revealed what they *wanted* it to be:

> *Key the transmitter on the provided frequency and emit an analog representation of the data coming from this stream.*

So they defined their own `Transmitter` interface with a `transmit` method taking a frequency and a data stream — **the interface they wished they had.** `CommunicationsController` was written against it and stayed clean and expressive. When the real API landed, a `TransmitterAdapter` bridged the gap, encapsulating the interaction in one place.

## Mental Models
- **Interesting things happen at boundaries — change is one of them.** Good designs accommodate change without huge investment and rework; when using code outside your control, take special care to protect that investment.
- **Two tools, one goal**: *wrap* (as with `Map`) or *adapt* (as with `Transmitter`). Either way, code speaks better, usage is internally consistent across the boundary, and there are fewer maintenance points when the third party changes.
- **It's not your job to test third-party code — but it may be in your interest.** Learning tests and boundary tests are for your benefit, not the vendor's.

## Key Takeaways
1. Third-party interfaces are built for breadth; yours should be built for your needs. Wrap the gap.
2. Never pass a boundary interface (like `Map`) around the system — confine it to one class or close family.
3. Write learning tests to explore an unfamiliar API instead of experimenting in production code.
4. Re-run learning and boundary tests on every third-party upgrade — they turn incompatibility into an immediate signal.
5. When the code you depend on doesn't exist yet, define the interface you wish you had and adapt later.
6. An Adapter gives you one place to change and a seam for testing with fakes.
7. Keep the number of places referring to third-party particulars very small.

## Connects To
- **Ch 7**: `LocalPort` wrapping `ACMEPort` is the boundary practice applied to exceptions.
- **Ch 6**: hiding `Map` behind `Sensors` is data abstraction — expose the essence, not the storage.
- **Ch 9**: learning tests are unit tests aimed at someone else's code; the F.I.R.S.T. rules apply.
- **Ch 11**: boundaries between subsystems and the decoupling that makes systems testable.
- **Ch 17**: [G33] Encapsulate Boundary Conditions.
- **GOF**: Adapter. **WELC (Feathers)**: seams. **BeckTDD**: learning tests, pp. 136–137.
