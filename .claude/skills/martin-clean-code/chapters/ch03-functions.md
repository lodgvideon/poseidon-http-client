# Chapter 3: Functions

## Core Idea
Functions should be small, do one thing, and stay at a single level of abstraction — so the whole program reads as a top-down narrative in which each function introduces the next.

## Frameworks Introduced

- **Small! — then smaller**
  - Rule: functions should hardly ever be 20 lines long. Kent Beck's `Sparkle` had functions of two, three, or four lines — *"That's how short your functions should be."*
  - Corollary (**Blocks and Indenting**): blocks inside `if`/`else`/`while` should be **one line long**, and that line should probably be a function call. Therefore **the indent level of a function should not be greater than one or two.**
  - Why it works: the extracted call gets a descriptive name, so shrinking the function adds documentation for free.

- **Do One Thing** — *"FUNCTIONS SHOULD DO ONE THING. THEY SHOULD DO IT WELL. THEY SHOULD DO IT ONLY."*
  - **The TO-paragraph test** (from LOGO's `TO` keyword): describe the function as *"TO \<FunctionName\>, we do X, then Y."* If every step is exactly **one level below** the function's name, it does one thing.
  - **The extraction test**: if you can extract another function from it with a name that is **not merely a restatement of its implementation** [G34], it was doing more than one thing.
  - **The sections test**: a function divisible into sections (*declarations*, *initializations*, *sieve*) is doing more than one thing. Functions that do one thing cannot be reasonably divided into sections.

- **One Level of Abstraction per Function** — never mix `getHtml()` (high), `PathParser.render(pagePath)` (intermediate), and `.append("\n")` (low) in one body. Like broken windows, once details mix with essential concepts, more details accrete.

- **The Stepdown Rule**: every function is followed by those at the next level of abstraction, so you read the program descending one level at a time.
  - How it reads: *To include the setups and teardowns, we include setups, then content, then teardowns. To include the setups, we include the suite setup if this is a suite, then the regular setup. To include the suite setup, we search the parent hierarchy…*
  - Martin: this is hard to learn and *"is the key to keeping functions short and making sure they do one thing."*

- **Command Query Separation**: a function either **does** something or **answers** something — never both.
  - Failure: `if (set("username", "unclebob"))` — is `set` a verb or an adjective? Unresolvable from the call site.
  - Fix: `if (attributeExists("username")) { setAttribute("username", "unclebob"); }`

- **Prefer Exceptions to Returning Error Codes** — error codes are a subtle CQS violation that force the caller to handle the error immediately, producing deep nesting.
  - **Extract Try/Catch Blocks**: pull the `try` body and the `catch` body into their own functions.
  - **Error Handling Is One Thing**: if `try` appears in a function it must be the **very first word**, and nothing may follow the `catch`/`finally` blocks.
  - **The `Error.java` Dependency Magnet**: an error enum is imported everywhere, so changing it forces mass recompile — programmers then reuse wrong error codes rather than add new ones. Exception subclasses add no such pressure (an application of OCP).

- **Don't Repeat Yourself (DRY)** — *"Duplication may be the root of all evil in software."* Codd's normal forms, OO base classes, structured/aspect/component programming are all, in part, duplication-elimination strategies.

## Reference Table: Function Arguments

| Count | Name | Verdict |
|---|---|---|
| 0 | niladic | **Ideal** |
| 1 | monadic | Next best |
| 2 | dyadic | Acceptable, at a cost |
| 3 | triadic | **Avoid where possible** |
| 3+ | polyadic | *"requires very special justification — and then shouldn't be used anyway"* |

**The three legitimate monadic forms:**
1. **Ask a question about the argument** — `boolean fileExists("MyFile")`
2. **Transform it and return the result** — `InputStream fileOpen("MyFile")`
3. **Event** — input, no output, alters system state: `void passwordAttemptFailedNtimes(int attempts)`. Use with care; make it obvious it is an event.

Anything outside these forms confuses readers. If a function transforms its input, **the transformation must appear as the return value**: `StringBuffer transform(StringBuffer in)` beats `void transform(StringBuffer out)` — even if the body just returns its input.

**Reducing arity:**
- **Argument Objects** — `makeCircle(double x, double y, double radius)` → `makeCircle(Point center, double radius)`. Variables passed together are likely a concept deserving a name. This is not cheating.
- Make the function a member of one argument: `outputStream.writeField(name)`.
- Make the argument a field of the current class, or extract a class (`FieldWriter`) taking it in the constructor.
- **Argument Lists**: `String.format(String format, Object... args)` is *dyadic* — identical varargs count as one `List` argument. Same limits apply: `monad(Integer... args)`, `dyad(String name, Integer... args)`, `triad(String name, int count, Integer... args)`.
- **Verbs and Keywords**: monads should form a verb/noun pair — `writeField(name)`. The **keyword form** encodes argument names into the function name: `assertExpectedEqualsActual(expected, actual)` removes the need to remember ordering.

## Anti-patterns
- **Flag arguments** — *"Passing a boolean into a function is a truly terrible practice."* The signature loudly proclaims the function does more than one thing: one thing if true, another if false. `render(true)` is meaningless at the call site. Split into `renderForSuite()` and `renderForSingleTest()`.
- **Output arguments** — `appendFooter(s)`: does it append `s` as a footer, or a footer to `s`? Checking the signature is a cognitive break. In OO, `this` *is* the output argument: `report.appendFooter()`. If a function must change state, it should change the state of its owning object.
- **Side effects** — *"Side effects are lies."* They create **temporal coupling**: the function can only be called at certain times, and calling it out of order silently destroys data.
- **Switch statements** — by their nature they do N things, violate SRP (more than one reason to change) and OCP (must change when types are added). Worse: an unlimited number of *other* functions will repeat the same structure (`isPayday`, `deliverPay`, …).
- **Ignorable arguments** — `writeField(outputStream, name)` teaches you to skip the first parameter. *"The parts we ignore are where the bugs will hide."*
- **Ambiguous dyads** — even `assertEquals(expected, actual)` is problematic: no natural ordering, so the convention must be learned. `assertEquals(message, expected, actual)` is worse. But `assertEquals(1.0, amount, .001)` earns its double-take — it reminds you float equality is relative.

## Code Examples

**Burying a switch in an Abstract Factory** — the rule: switches are tolerable only if they appear **once**, are used to **create polymorphic objects**, and are **hidden behind an inheritance relationship** [G23].

```java
// Before — grows with every new employee type; this shape will be duplicated
// across isPayday(), deliverPay(), and every other type-dependent operation.
public Money calculatePay(Employee e) throws InvalidEmployeeType {
    switch (e.type) {
        case COMMISSIONED: return calculateCommissionedPay(e);
        case HOURLY:       return calculateHourlyPay(e);
        case SALARIED:     return calculateSalariedPay(e);
        default: throw new InvalidEmployeeType(e.type);
    }
}

// After — one switch, in the basement, creating polymorphic objects.
public abstract class Employee {
    public abstract boolean isPayday();
    public abstract Money calculatePay();
    public abstract void deliverPay(Money pay);
}

public interface EmployeeFactory {
    public Employee makeEmployee(EmployeeRecord r) throws InvalidEmployeeType;
}

public class EmployeeFactoryImpl implements EmployeeFactory {
    public Employee makeEmployee(EmployeeRecord r) throws InvalidEmployeeType {
        switch (r.type) {
            case COMMISSIONED: return new CommissionedEmployee(r);
            case HOURLY:       return new HourlyEmployee(r);
            case SALARIED:     return new SalariedEmployee(r);
            default: throw new InvalidEmployeeType(r.type);
        }
    }
}
```

**The hidden side effect** — `checkPassword` also calls `Session.initialize()`:

```java
public boolean checkPassword(String userName, String password) {
    User user = UserGateway.findByName(userName);
    if (user != User.NULL) {
        String codedPhrase = user.getPhraseEncodedByPassword();
        String phrase = cryptographer.decrypt(codedPhrase, password);
        if ("Valid Password".equals(phrase)) {
            Session.initialize();   // <-- the lie: erases session data on a mere check
            return true;
        }
    }
    return false;
}
```
Renaming to `checkPasswordAndInitializeSession` makes the coupling honest — but violates "Do one thing." The real fix is to remove the side effect.

**Error codes vs. exceptions** — the same logic, two shapes:

```java
// Error codes: the caller must handle failure immediately → deep nesting.
if (deletePage(page) == E_OK) {
    if (registry.deleteReference(page.name) == E_OK) {
        if (configKeys.deleteKey(page.name.makeKey()) == E_OK) {
            logger.log("page deleted");
        } else { logger.log("configKey not deleted"); }
    } else { logger.log("deleteReference from registry failed"); }
} else { logger.log("delete failed"); return E_ERROR; }

// Exceptions: happy path separated from error path.
try {
    deletePage(page);
    registry.deleteReference(page.name);
    configKeys.deleteKey(page.name.makeKey());
} catch (Exception e) {
    logger.log(e.getMessage());
}

// Extracted: delete() is all about error processing and can be understood then ignored;
// deletePageAndAllReferences() is all about deleting and can ignore errors.
public void delete(Page page) {
    try {
        deletePageAndAllReferences(page);
    } catch (Exception e) {
        logError(e);
    }
}
private void deletePageAndAllReferences(Page page) throws Exception {
    deletePage(page);
    registry.deleteReference(page.name);
    configKeys.deleteKey(page.name.makeKey());
}
private void logError(Exception e) { logger.log(e.getMessage()); }
```

## Worked Example: Three Refactorings of `testableHtml`

The chapter opens with a long FitNesse function and shrinks it three times.

```java
// Listing 3-2 — after extract-method: nine lines, but still two levels of abstraction.
public static String renderPageWithSetupsAndTeardowns(PageData pageData, boolean isSuite)
        throws Exception {
    boolean isTestPage = pageData.hasAttribute("Test");
    if (isTestPage) {
        WikiPage testPage = pageData.getWikiPage();
        StringBuffer newPageContent = new StringBuffer();
        includeSetupPages(testPage, newPageContent, isSuite);
        newPageContent.append(pageData.getContent());
        includeTeardownPages(testPage, newPageContent, isSuite);
        pageData.setContent(newPageContent.toString());
    }
    return pageData.getHtml();
}

// Listing 3-3 — one level of abstraction. Cannot be meaningfully shrunk further:
// extracting the if would only restate the implementation.
public static String renderPageWithSetupsAndTeardowns(PageData pageData, boolean isSuite)
        throws Exception {
    if (isTestPage(pageData))
        includeSetupAndTeardownPages(pageData, isSuite);
    return pageData.getHtml();
}
```

The final form (Listing 3-7, `SetupTeardownIncluder`) turns the arguments into fields, which lets every method drop to zero or one argument, and names them so the sequence tells a story:

```java
private String render(boolean isSuite) throws Exception {
    this.isSuite = isSuite;
    if (isTestPage())
        includeSetupAndTeardownPages();
    return pageData.getHtml();
}
private void includeSetupAndTeardownPages() throws Exception {
    includeSetupPages();
    includePageContent();
    includeTeardownPages();
    updatePageContent();
}
private void includeSetupPages() throws Exception {
    if (isSuite) includeSuiteSetupPage();
    includeSetupPage();
}
private void includeSuiteSetupPage() throws Exception {
    include(SuiteResponder.SUITE_SETUP_NAME, "-setup");
}
private void includeSetupPage() throws Exception { include("SetUp", "-setup"); }
```

Note the naming: seeing `includeSetupAndTeardownPages`, `includeSetupPages`, `includeSuiteSetupPage`, `includeSetupPage` makes you *ask* where `includeTeardownPage` is — and it is exactly where you expect. That is Ward's "pretty much what you expected" in practice. The original also duplicated one algorithm four times (SetUp / SuiteSetUp / TearDown / SuiteTearDown); the `include` method collapses all four.

## Mental Models
- **Use Descriptive Names, however long.** *"A long descriptive name is better than a short enigmatic name. A long descriptive name is better than a long descriptive comment."* Try several names and read the code with each in place — hunting for a good name often produces a favorable restructuring.
- **Structured programming is for large functions.** Dijkstra's single-entry/single-exit rules matter little when functions are small: an occasional extra `return`, `break`, or `continue` can be more expressive. `goto` only makes sense in large functions, so avoid it entirely.
- **How you actually get there**: nobody writes it clean first. Martin's first drafts are long, deeply indented, badly named, and duplicated — *but covered by unit tests*. Then he massages: split functions, rename, remove duplication, reorder, sometimes extract whole classes, keeping tests green throughout.
- **Programming is language design.** Functions are the verbs, classes the nouns, of a domain-specific language you design. Master programmers think of systems as **stories to be told** rather than programs to be written.

## Key Takeaways
1. Make functions small, then smaller; indent level ≤ 2, and blocks inside control statements are one call.
2. "One thing" is testable: the TO-paragraph test, the extraction test, and the sections test.
3. Keep one level of abstraction per function; mixing levels is always confusing and attracts more detail.
4. Order functions by the Stepdown Rule so the module reads top-down.
5. Fewer arguments is better; zero is ideal. Flag arguments and output arguments are defects, not styles.
6. Separate commands from queries; prefer exceptions to error codes; extract try/catch bodies.
7. Duplication is the root of all evil — hunt for the non-obvious, interleaved kind.
8. Write it dirty under test, then refine. The rules describe the destination, not the first draft.

## Connects To
- **Ch 2**: descriptive names are half the battle; small focused functions make good names easier to find.
- **Ch 7**: error handling as one thing is developed into a full chapter.
- **Ch 9**: "I also have a suite of unit tests that cover every one of those clumsy lines" — refinement is only safe under test.
- **Ch 10**: burying the switch behind a factory is SRP + OCP; the same forces produce small classes.
- **Ch 12**: eliminating duplication is Rule 2 of Emergent Design.
- **Ch 17**: [G23] "Prefer Polymorphism to If/Else or Switch/Case", [G34] "Functions Should Descend Only One Level of Abstraction".
- **SOLID**: SRP and OCP are cited directly here; developed in *PPP* (Martin, 2002).
