# Chapter 17: Smells and Heuristics

## Core Idea
A catalogue of 66 code smells and heuristics, compiled by refactoring several programs and writing down *why* each change was made. **The list is not the point — the value system behind it is.** *"Clean code is not written by following a set of rules. You don't become a software craftsman by learning a list of heuristics. Professionalism and craftsmanship come from values that drive disciplines."*

Builds on Martin Fowler's Code Smells in *Refactoring*, plus Martin's own. Meant to be **read top to bottom once, then used as a reference.**

---

## Comments (C1–C5)

| # | Smell | Rule |
|---|---|---|
| **C1** | Inappropriate Information | Comments must not hold information better held in source control, issue tracking, or other record-keeping systems. Change histories, authors, last-modified-date, SPR numbers — none belong in comments. *Reserve comments for technical notes about the code and design.* |
| **C2** | Obsolete Comment | Old, irrelevant, incorrect. **Best not to write a comment that will become obsolete;** if you find one, update or delete it immediately. They *"migrate away from the code they once described"* and become floating islands of misdirection. |
| **C3** | Redundant Comment | Describes something that adequately describes itself: `i++; // increment i`, or a Javadoc saying no more than the signature. *"Comments should say things that the code cannot say for itself."* |
| **C4** | Poorly Written Comment | *"A comment worth writing is worth writing well."* Choose words carefully, correct grammar and punctuation, don't ramble, don't state the obvious, be brief. |
| **C5** | Commented-Out Code | **"An abomission."** It rots — calls functions that no longer exist, uses renamed variables, follows obsolete conventions. Nobody deletes it because everyone assumes someone else needs it. **Delete it. Source control remembers.** |

## Environment (E1–E2)

| # | Smell | Rule |
|---|---|---|
| **E1** | Build Requires More Than One Step | Building should be a single trivial operation — one command to check out, one to build. No hunting for stray JARs, XML files, or arcane context-dependent scripts. |
| **E2** | Tests Require More Than One Step | All unit tests with **one** command — best case, one button in the IDE. *"Being able to run all the tests is so fundamental and so important that it should be quick, easy, and obvious to do."* |

## Functions (F1–F4)

| # | Smell | Rule |
|---|---|---|
| **F1** | Too Many Arguments | None is best, then one, two, three. **More than three is very questionable and should be avoided with prejudice.** |
| **F2** | Output Arguments | Counterintuitive — readers expect arguments to be inputs. If a function must change state, have it change the state of the object it is called on. |
| **F3** | Flag Arguments | Booleans *"loudly declare that the function does more than one thing."* Eliminate them. |
| **F4** | Dead Function | Methods never called should be discarded. *"Don't be afraid to delete the function. Remember, your source code control system still remembers it."* |

---

## General (G1–G36)

**G1: Multiple Languages in One Source File.** A Java file may contain XML, HTML, YAML, Javadoc, English, JavaScript; a JSP even more. *"This is confusing at best and carelessly sloppy at worst."* The ideal is one language per file; realistically, **minimize both the number and the extent** of the extras.

**G2: Obvious Behavior Is Unimplemented.** Per the **Principle of Least Surprise**, implement what another programmer could reasonably expect. `DayDate.StringToDay("Monday")` should yield `Day.MONDAY` — and should handle common abbreviations and ignore case. *"When an obvious behavior is not implemented, readers and users of the code can no longer depend on their intuition about function names. They lose their trust in the original author."*

**G3: Incorrect Behavior at the Boundaries.** *"We seldom realize just how complicated correct behavior is."* Developers trust intuition instead of proving corner cases. **"Don't rely on your intuition. Look for every boundary condition and write a test for it."**

**G4: Overridden Safeties.** Chernobyl melted down because the plant manager overrode each safety mechanism one by one — they made an experiment inconvenient. Turning off compiler warnings to get a build to succeed risks endless debugging; *"turning off failing tests and telling yourself you'll get them to pass later is as bad as pretending your credit cards are free money."*

**G5: Duplication.** *"One of the most important rules in this book."* DRY (Hunt & Thomas); "Once, and only once" (Beck); ranked second by Ron Jeffries, just below getting tests to pass. **Every duplication is a missed opportunity for abstraction.** Three escalating forms:

| Form | Fix |
|---|---|
| Identical clumps of pasted code | Extract a method |
| The same `switch`/`if-else` chain recurring across modules | **Polymorphism** |
| Similar *algorithms* that share no similar lines | **Template Method** or **Strategy** |

*"Most of the design patterns that have appeared in the last fifteen years are simply well-known ways to eliminate duplication."* So are Codd's normal forms, OO itself, and structured programming.

**G6: Code at Wrong Level of Abstraction.** Separation between high- and low-level concepts must be **complete** — no detail-only constants, variables, or utilities in a base class.

```java
public interface Stack {
    Object pop() throws EmptyException;
    void push(Object o) throws FullException;
    double percentFull();     // ← wrong level: belongs in BoundedStack
}
```
And you cannot fake your way out by returning 0 for unbounded stacks: no stack is truly boundless, and `stack.percentFull() < 50.0` cannot prevent an `OutOfMemoryException`. *"Implementing the function to return 0 would be telling a lie… you cannot lie or fake your way out of a misplaced abstraction."*

**G7: Base Classes Depending on Their Derivatives.** *"In general, base classes should know nothing about their derivatives."* Exception: a strictly fixed set of derivatives (finite state machines) that always deploy together. The payoff of independence is deployment in separate jars — change a derivative without redeploying the base.

**G8: Too Much Information.** *"Well-defined modules have very small interfaces that allow you to do a lot with a little."* Fewer methods per class, fewer variables per function, fewer instance variables per class. **Hide your data, utility functions, constants, and temporaries.** Don't create lots of protected members for subclasses.

**G9: Dead Code.** Unreachable branches, `catch` blocks for exceptions never thrown, uncalled utilities, impossible `switch` cases. It still compiles but *"does not follow newer conventions or rules. It was written at a time when the system was different."* **Give it a decent burial.**

**G10: Vertical Separation.** Local variables declared just above first usage, with small vertical scope. Private functions defined just below their first usage — *"finding a private function should just be a matter of scanning downward."*

**G11: Inconsistency.** *"If you do something a certain way, do all similar things in the same way."* If `response` holds an `HttpServletResponse` in one function, use that name everywhere. If one method is `processVerificationRequest`, the sibling should be `processDeletionRequest`.

**G12: Clutter.** Default constructors with no implementation, unused variables, uncalled functions, comments that add nothing. *"Keep your source files clean, well organized, and free of clutter."*

**G13: Artificial Coupling.** *"A coupling between two modules that serves no direct purpose."* General enums nested inside specific classes force the whole application to know about those classes. It results from *"putting a variable, constant, or function in a temporarily convenient, though inappropriate, location. This is lazy and careless."*

**G14: Feature Envy** *(Fowler)*. A method interested in another class's variables — *"it wishes that it were inside that other class."*

```java
public Money calculateWeeklyPay(HourlyEmployee e) {
    int tenthRate = e.getTenthRate().getPennies();
    int tenthsWorked = e.getTenthsWorked();      // reaching into another object
    ...
}
```
**But sometimes it's a necessary evil**: `HourlyEmployeeReport.reportHours()` envies `HourlyEmployee` — yet moving the format string into `HourlyEmployee` would couple it to the report format, violating SRP, OCP, and the Common Closure Principle.

**G15: Selector Arguments.** *"There is hardly anything more abominable than a dangling `false` argument at the end of a function call."* Each selector combines many functions into one; they are *"just a lazy way to avoid splitting a large function."* `calculateWeeklyPay(boolean overtime)` should have been `straightPay()` and `overTimePay()`. **Selectors need not be boolean** — enums, ints, anything selecting behavior. *"In general it is better to have many functions than to pass some code into a function to select the behavior."*

**G16: Obscured Intent.** Run-on expressions, Hungarian notation, and magic numbers together produce the impenetrable:
```java
public int m_otCalc() {
    return iThsWkd * iThsRte +
        (int) Math.round(0.5 * iThsRte * Math.max(0, iThsWkd - 400));
}
```

**G17: Misplaced Responsibility.** *"One of the most important decisions a software developer can make is where to put code."* Use the Principle of Least Surprise: `PI` goes where the trig functions are; `OVERTIME_RATE` in `HourlyPayCalculator`. **The naming test**: between `getTotalHours` (report module) and `saveTimeCard` (timecard module), which name implies it calculates a total? If performance demands the other placement, *"the names of the functions ought to reflect this"* — add `computeRunningTotalOfHours`.

**G18: Inappropriate Static.** `Math.max(a, b)` is a good static — all data comes from arguments, and *"there is almost no chance that we'd want `Math.max` to be polymorphic."* But `HourlyPayCalculator.calculatePay(employee, overtimeRate)` looks similar and is wrong: you may well want `OvertimeHourlyPayCalculator` and `StraightTimeHourlyPayCalculator`. **Prefer nonstatic; when in doubt, make it nonstatic.**

**G19: Use Explanatory Variables** *(Beck)*. Break calculations into intermediate values with meaningful names:
```java
Matcher match = headerPattern.matcher(line);
if (match.find()) {
    String key = match.group(1);
    String value = match.group(2);
    headers.put(key.toLowerCase(), value);
}
```
*"It is hard to overdo this. More explanatory variables are generally better than fewer."*

**G20: Function Names Should Say What They Do.** `Date newDate = date.add(5);` — days, weeks, hours? Mutating or returning new? **If you have to read the implementation or documentation to know, find a better name** — `addDaysTo`/`increaseByDays` if mutating, `daysLater`/`daysSince` if not.

**G21: Understand the Algorithm.** Code gets "working" by plugging in `if`s and flags until the known test cases pass. *"It is not sufficient to leave the quotation marks around the word 'work.'… You must know that the solution is correct."* Often the best way to gain that understanding is **to refactor the function until it is obvious how it works.**

**G22: Make Logical Dependencies Physical.** A dependent module should not *assume* — it should **explicitly ask** for what it depends on. `HourlyReporter` holding `PAGE_SIZE = 55` assumes `HourlyReportFormatter` can handle 55-line pages. Physicalize it: add `getMaxPageSize()` to the formatter and call it.

**G23: Prefer Polymorphism to If/Else or Switch/Case.** Ch 6 argues switches are appropriate where new *functions* are likelier than new *types* — but *"most people use switch statements because it's the obvious brute force solution, not because it's the right solution,"* and function-volatile cases are rare. **Every switch statement should be suspect.**
> **The ONE SWITCH rule**: there may be no more than one switch statement for a given type of selection, and its cases must create the polymorphic objects that replace all other such switches in the system.

**G24: Follow Standard Conventions.** Every team follows a coding standard based on industry norms. *"The team should not need a document to describe these conventions because their code provides the examples."* And each member must be mature enough to realize *"it doesn't matter a whit where you put your braces so long as you all agree."*

**G25: Replace Magic Numbers with Named Constants.** 86,400 → `SECONDS_PER_DAY`; 55 → `LINES_PER_PAGE`. **But not dogmatically** — these read fine raw:
```java
double milesWalked = feetWalked/5280.0;
int dailyPay = hourlyRate * 8;
double circumference = radius * Math.PI * 2;
```
`FEET_PER_MILE`? 5280 is unique and recognizable alone on a page. `TWO`? Absurd. π is different: *"Every time someone sees 3.1415927535890793, they know that it is π, and so they fail to scrutinize it. (Did you catch the single-digit error?)"* — hence `Math.PI`.
**"Magic Number" applies to any non-self-describing token**, not just numbers:
```java
assertEquals(7777, Employee.find("John Doe").employeeNumber());
// → two magic values, both opaque:
assertEquals(HOURLY_EMPLOYEE_ID, Employee.find(HOURLY_EMPLOYEE_NAME).employeeNumber());
```

**G26: Be Precise.** *"Expecting the first match to be the only match to a query is probably naive. Using floating point numbers to represent currency is almost criminal. Avoiding locks and/or transaction management because you don't think concurrent update is likely is lazy at best. Declaring a variable to be an `ArrayList` when a `List` will do is overly constraining. Making all variables protected by default is not constraining enough."* **"Ambiguities and imprecision in code are either a result of disagreements or laziness."**

**G27: Structure over Convention.** *"Naming conventions are good, but they are inferior to structures that force compliance."* A `switch` over well-named enums is inferior to a base class with abstract methods — nobody is forced to write the switch the same way twice, but the compiler *does* force every concrete class to implement the abstract methods.

**G28: Encapsulate Conditionals.** `if (shouldBeDeleted(timer))` beats `if (timer.hasExpired() && !timer.isRecurrent())`.

**G29: Avoid Negative Conditionals.** `if (buffer.shouldCompact())` beats `if (!buffer.shouldNotCompact())`.

**G30: Functions Should Do One Thing.**
```java
// Three things: loop, check, pay.
public void pay() {
    for (Employee e : employees) {
        if (e.isPayday()) {
            Money pay = e.calculatePay();
            e.deliverPay(pay);
        }
    }
}
// → each does one thing.
public void pay() {
    for (Employee e : employees)
        payIfNecessary(e);
}
private void payIfNecessary(Employee e) {
    if (e.isPayday())
        calculateAndDeliverPay(e);
}
private void calculateAndDeliverPay(Employee e) {
    Money pay = e.calculatePay();
    e.deliverPay(pay);
}
```

**G31: Hidden Temporal Couplings.** *"Temporal couplings are often necessary, but you should not hide the coupling."* Structure arguments so the required order is obvious — **a bucket brigade**:
```java
// Hidden — another programmer can call reticulateSplines first → UnsaturatedGradientException.
public void dive(String reason) {
    saturateGradient();
    reticulateSplines();
    diveForMoog(reason);
}
// Exposed — each function produces what the next needs.
public void dive(String reason) {
    Gradient gradient = saturateGradient();
    List<Spline> splines = reticulateSplines(gradient);
    diveForMoog(splines, reason);
}
```
*"You might complain that this increases the complexity of the functions, and you'd be right. But that extra syntactic complexity exposes the true temporal complexity of the situation."*

**G32: Don't Be Arbitrary.** *"If a structure appears arbitrary, others will feel empowered to change it. If a structure appears consistently throughout the system, others will use it and preserve the convention."* A public class nested inside another with no need to be there, used by unrelated classes, is arbitrary. **Public classes that are not utilities of another class belong at the top level of their package.**

**G33: Encapsulate Boundary Conditions.** *"We don't want swarms of +1s and -1s scattered hither and yon."*
```java
if (level + 1 < tags.length) {
    parts = new Parse(body, tags, level + 1, offset + endTag);   // level+1 twice
}
// →
int nextLevel = level + 1;
if (nextLevel < tags.length) {
    parts = new Parse(body, tags, nextLevel, offset + endTag);
}
```

**G34: Functions Should Descend Only One Level of Abstraction.** *"This may be the hardest of these heuristics to interpret and follow… humans are just far too good at seamlessly mixing levels of abstraction."*
```java
// Mixes two levels: "a horizontal rule has a size" and "the syntax of the HR tag."
public String render() throws Exception {
    StringBuffer html = new StringBuffer("<hr");
    if (size > 0)
        html.append(" size=\"").append(size + 1).append("\"");
    html.append(">");
    return html.toString();
}
// Separated — render() constructs an HR tag without knowing HTML syntax.
public String render() throws Exception {
    HtmlTag hr = new HtmlTag("hr");
    if (extraDashes > 0)
        hr.addAttribute("size", hrSize(extraDashes));
    return hr.html();
}
private String hrSize(int height) {
    int hrSize = height + 1;
    return String.format("%d", hrSize);
}
```
**The refactoring caught a latent bug**: the original emitted `<hr>` rather than XHTML's `<hr/>`; `HtmlTag` had conformed long ago. And Martin's *first* attempt still mixed levels (tag construction vs. formatting the size) — *"when you break a function along lines of abstraction, you often uncover new lines of abstraction that were obscured by the previous structure."*

**G35: Keep Configurable Data at High Levels.** Defaults and configuration known at a high level must not be buried in low-level functions — expose them as arguments passed down. FitNesse parses command-line arguments on the very first executable line, with `DEFAULT_PATH`, `DEFAULT_ROOT`, `DEFAULT_PORT`, `DEFAULT_VERSION_DAYS` at the top of `Arguments`. *"The lower levels of the application do not own the values of these constants."*

**G36: Avoid Transitive Navigation.** If A collaborates with B and B with C, users of A should not know about C — no `a.getB().getC().doSomething()`. The Law of Demeter; the Pragmatic Programmers call it **"Writing Shy Code."** The cost of violating it: interposing a `Q` between B and C means finding and rewriting *every* `a.getB().getC()`. *"This is how architectures become rigid. Too many modules know too much about the architecture."*

---

## Java (J1–J3)

**J1: Avoid Long Import Lists by Using Wildcards.** Use two or more classes from a package → `import package.*;`. Beyond readability, there is a coupling argument: **specific imports are hard dependencies; wildcard imports are not.** A wildcard merely adds the package to the search path, so no true dependency is created. Caveat: wildcards can cause name conflicts between same-named classes in different packages — *"a nuisance but rare enough"* that wildcards are still generally better.

**J2: Don't Inherit Constants.** Putting constants in an interface and inheriting it to reach them is *"a hideous practice!"* — the constants end up hidden at the top of the inheritance hierarchy (`HourlyEmployee` → `Employee` → `implements PayrollConstants`). **"Don't use inheritance as a way to cheat the scoping rules of the language. Use a static import instead."**

**J3: Constants versus Enums.** *"Now that enums have been added to the language (Java 5), use them!"* The meaning of `int`s gets lost; the meaning of enums cannot, because they belong to a named enumeration. **Study the syntax carefully — enums can have methods and fields:**
```java
public enum HourlyPayGrade {
    APPRENTICE           { public double rate() { return 1.0; } },
    LEUTENANT_JOURNEYMAN { public double rate() { return 1.2; } },
    JOURNEYMAN           { public double rate() { return 1.5; } },
    MASTER               { public double rate() { return 2.0; } };
    public abstract double rate();
}
```

---

## Names (N1–N7)

**N1: Choose Descriptive Names.** *"Names in software are 90 percent of what make software readable."* Meanings drift as software evolves — **frequently reevaluate**. The bowling-score demonstration: `public int x()` with `q`, `z`, `kk`, `l[]` is a hodge-podge of symbols; renamed to `score()`, `frame`, `isStrike()`, `nextTwoBallsForStrike()`, it becomes readable enough that **you could write the missing functions from the inferred meaning.** *"The power of carefully chosen names is that they overload the structure of the code with description."*

**N2: Choose Names at the Appropriate Level of Abstraction.** Don't name after the implementation:
```java
// Commits to phone lines — wrong for hard-wired or USB-switched modems.
boolean dial(String phoneNumber);
String getConnectedPhoneNumber();
// Neutral about connection strategy.
boolean connect(String connectionLocator);
String getConnectedLocator();
```

**N3: Use Standard Nomenclature Where Possible.** Pattern names (`AutoHangupModemDecorator`), language conventions (`toString`), and the team's own **ubiquitous language** (Eric Evans, *DDD*). *"The more you can use names that are overloaded with special meanings that are relevant to your project, the easier it will be for readers to know what your code is talking about."*

**N4: Unambiguous Names.** `doRename()` containing a call to `renamePage()` tells you nothing about the difference. Better: `renamePageAndOptionallyAllReferences` — *"This may seem long, and it is, but it's only called from one place in the module, so its explanatory value outweighs the length."*

**N5: Use Long Names for Long Scopes.** *"Variable names like `i` and `j` are just fine if their scope is five lines long"* — replacing `i` with `rollCount` in a two-line loop would obfuscate. **The longer the scope, the longer and more precise the name.**

**N6: Avoid Encodings.** No `m_`, no `f`, no project/subsystem prefixes like `vis_`. *"Keep your names free of Hungarian pollution."*

**N7: Names Should Describe Side-Effects.** *"Don't use a simple verb to describe a function that does more than just that simple action."*
```java
public ObjectOutputStream getOos() throws IOException {
    if (m_oos == null)
        m_oos = new ObjectOutputStream(m_socket.getOutputStream());  // it creates, too
    return m_oos;
}
// → createOrReturnOos
```

---

## Tests (T1–T9)

| # | Heuristic | Rule |
|---|---|---|
| **T1** | Insufficient Tests | The common metric is *"That seems like enough."* **A test suite should test everything that could possibly break** — insufficient so long as any condition is unexplored or any calculation unvalidated |
| **T2** | Use a Coverage Tool! | Coverage reports gaps in your *testing strategy*. IDEs mark covered lines green, uncovered red — making unchecked `if`/`catch` bodies quick to find |
| **T3** | Don't Skip Trivial Tests | *"They are easy to write and their documentary value is higher than the cost to produce them."* |
| **T4** | An Ignored Test Is a Question about an Ambiguity | When requirements are unclear, express the question as a commented-out or `@Ignore`d test. **Which one depends on whether the ambiguity is about something that would compile** |
| **T5** | Test Boundary Conditions | *"We often get the middle of an algorithm right but misjudge the boundaries."* |
| **T6** | Exhaustively Test Near Bugs | **Bugs congregate.** Find one in a function → test that function exhaustively. *"You'll probably find that the bug was not alone."* |
| **T7** | Patterns of Failure Are Revealing | *"All tests with an input larger than five characters failed"* — the shape of the red/green report can spark the "Aha!" Another argument for complete, sensibly ordered test cases |
| **T8** | Test Coverage Patterns Can Be Revealing | What the **passing** tests do and don't execute gives clues to why the **failing** tests fail |
| **T9** | Tests Should Be Fast | *"A slow test is a test that won't get run. When things get tight, it's the slow tests that will be dropped from the suite."* |

---

## Key Takeaways
1. Duplication [G5] and abstraction-level mixing [G6, G34] are the two highest-yield smells — most others are consequences.
2. Names carry 90% of readability [N1]; length tracks scope [N5]; names must cover side effects [N7].
3. Prefer structure that *enforces* over convention that *requests* [G27] — and make hidden couplings physical [G22, G31].
4. Boundary conditions deserve tests [T5, G3] and encapsulation [G33]; bugs cluster there [T6].
5. Delete aggressively: commented-out code [C5], dead functions [F4], dead code [G9], clutter [G12].
6. Be precise about decisions [G26] and never fake your way out of a misplaced abstraction [G6].
7. Apply named-constant rules with judgment [G25] — `5280.0` reads better raw than `FEET_PER_MILE`.
8. **The list implies a value system; the value system is the point.**

## Connects To
- **Appendix C**: cross-reference showing where each heuristic is invoked elsewhere in the book.
- **Ch 2** → N1–N7; **Ch 3** → F1–F4, G30, G34; **Ch 4** → C1–C5; **Ch 5** → G10, G11, G35; **Ch 6** → G6, G36; **Ch 9** → T1–T9; **Ch 10** → G17, G18; **Ch 12** → G5, G16.
- **Ch 15–16**: these heuristics used live, with tags, on real code.
- **Refactoring (Fowler)**: the original Code Smells and Feature Envy. **PPP (Martin)**: SRP, OCP, Common Closure. **DDD (Evans)**: ubiquitous language. **Beck97/Beck07**: explanatory variables.
