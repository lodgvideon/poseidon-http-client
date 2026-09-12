# Chapter 10: Classes

*with Jeff Langr*

## Core Idea
Classes should be small — and size is measured in **responsibilities**, not lines. A system should be composed of many small classes, each with a single reason to change, collaborating with a few others.

## Frameworks Introduced

- **Class Organization** (standard Java convention, top to bottom):
  1. Public static constants
  2. Private static variables
  3. Private instance variables
  4. Public functions — each followed immediately by the private utilities it calls

  *"There is seldom a good reason to have a public variable."* Putting private utilities right after their caller follows the Stepdown Rule and makes the class read like a newspaper article.

- **Encapsulation, with a pragmatic exception** — *"we're not fanatic about it."* If a test in the same package needs a function or variable, make it protected or package scope. **"For us, tests rule."** But first look for a way to maintain privacy: **loosening encapsulation is always a last resort.**

- **Classes Should Be Small! — measured by responsibilities**
  - **The naming test**: the class name should describe the responsibilities it fulfills. *"If we cannot derive a concise name for a class, then it's likely too large."* Weasel words like `Processor`, `Manager`, or `Super` hint at unfortunate aggregation of responsibilities.
  - **The 25-word test**: describe the class in about 25 words **without using "if," "and," "or," or "but."** For `SuperDashboard`: *"provides access to the component that last held the focus, **and** it also allows us to track the version and build numbers"* — the first "and" is the tell.

- **The Single Responsibility Principle (SRP)** — a class or module should have **one, and only one, reason to change.** This gives both a definition of responsibility and a guideline for class size.
  - *"SRP is one of the more important concepts in OO design… Yet oddly, SRP is often the most abused class design principle."*
  - **Why it's abused**: getting software to work and making software clean are two different activities. Focusing on the first is *wholly appropriate* — the failure is thinking you're done when the program works, and never switching to the second concern.

- **Cohesion** — classes should have a small number of instance variables, and each method should manipulate one or more of them. A class where **every** variable is used by **every** method is maximally cohesive.
  - Not the goal: *"it is neither advisable nor possible to create such maximally cohesive classes"* — but keep cohesion high, so methods and variables hang together as a logical whole.
  - **The split signal**: keeping functions small and parameter lists short leads to a proliferation of instance variables used by only a subset of methods. *"When this happens, it almost always means that there is at least one other class trying to get out of the larger class."* **When classes lose cohesion, split them.**

- **The Open-Closed Principle (OCP)** — classes should be **open for extension but closed for modification.** *"In an ideal system, we incorporate new features by extending the system, not by making modifications to existing code."*

- **The Dependency Inversion Principle (DIP)** — classes should depend upon **abstractions, not concrete details.**

## Mental Models
- **The toolbox question**: *"Do you want your tools organized into toolboxes with many small drawers each containing well-defined and well-labeled components? Or do you want a few drawers that you just toss everything into?"*
- **The many-small-classes objection, answered**: developers fear that many small classes make the big picture harder to see. But **a system with many small classes has no more moving parts than a system with a few large ones** — there is just as much to learn either way. The difference is that small classes let a developer *know where to look* and understand only the directly affected complexity; large multipurpose classes force you to wade through what you don't need right now.
- **Private-method scope as a heuristic**: private behavior that applies only to a small subset of a class flags a potential split. **But the primary spur for action should be system change itself.** If a class is logically complete and you won't need the new functionality for the foreseeable future, leave it alone. *"As soon as we find ourselves opening up a class, we should consider fixing our design."*
- **Refactoring is not rewriting.** The `PrintPrimes` transformation used the *same algorithm and mechanics*: a test suite was written to verify the precise behavior of the original, then *"a myriad of tiny little changes were made, one at a time,"* running the program after each to confirm behavior had not changed.

## Worked Example: SuperDashboard → Version

`SuperDashboard` exposes about 70 public methods — what most would call a **God class**. But the point is sharper than size:

```java
// Only five methods. Still too many responsibilities.
public class SuperDashboard extends JFrame implements MetaDataUser {
    public Component getLastFocusedComponent()
    public void setLastFocused(Component lastFocused)
    public int getMajorVersionNumber()
    public int getMinorVersionNumber()
    public int getBuildNumber()
}
```

**Two reasons to change**: (1) it tracks version information, updated every time the software ships; (2) it manages Java Swing components (it derives from `JFrame`). Changing Swing code might prompt a version bump — but the converse doesn't hold: version info can change for reasons anywhere else in the system.

```java
// Extracting the responsibility yields a class with high reuse potential.
public class Version {
    public int getMajorVersionNumber()
    public int getMinorVersionNumber()
    public int getBuildNumber()
}
```

*"Trying to identify responsibilities (reasons to change) often helps us recognize and create better abstractions in our code."*

## Worked Example: Knuth's PrintPrimes, Split by Responsibility

The original (from Knuth's *Literate Programming*, as emitted by his WEB tool) is one function: deeply indented, a plethora of odd variables (`M`, `RR`, `CC`, `WW`, `ORDMAX`, `PAGEOFFSET`, `JPRIME`…), tightly coupled.

The refactoring splits it into **three responsibilities**:

```java
// 1. PrimePrinter — owns the execution environment. Changes if the method of
//    invocation changes (e.g. converting this to a SOAP service).
public class PrimePrinter {
    public static void main(String[] args) {
        final int NUMBER_OF_PRIMES = 1000;
        int[] primes = PrimeGenerator.generate(NUMBER_OF_PRIMES);

        final int ROWS_PER_PAGE = 50;
        final int COLUMNS_PER_PAGE = 4;
        RowColumnPagePrinter tablePrinter =
            new RowColumnPagePrinter(ROWS_PER_PAGE, COLUMNS_PER_PAGE,
                "The First " + NUMBER_OF_PRIMES + " Prime Numbers");
        tablePrinter.print(primes);
    }
}

// 2. RowColumnPagePrinter — knows how to format numbers into pages of rows and
//    columns. Changes if the output format changes.
public class RowColumnPagePrinter {
    public void print(int data[]) {
        int pageNumber = 1;
        for (int firstIndexOnPage = 0;
             firstIndexOnPage < data.length;
             firstIndexOnPage += numbersPerPage) {
            int lastIndexOnPage =
                Math.min(firstIndexOnPage + numbersPerPage - 1, data.length - 1);
            printPageHeader(pageHeader, pageNumber);
            printPage(firstIndexOnPage, lastIndexOnPage, data);
            printStream.println("\f");
            pageNumber++;
        }
    }
    // printPage, printRow, printPageHeader, setOutput ...
}

// 3. PrimeGenerator — knows how to generate primes. Changes if the algorithm changes.
//    Note: not meant to be instantiated. "The class is just a useful scope in which
//    its variables can be declared and kept hidden."
public class PrimeGenerator {
    private static int[] primes;
    private static ArrayList<Integer> multiplesOfPrimeFactors;

    protected static int[] generate(int n) {
        primes = new int[n];
        multiplesOfPrimeFactors = new ArrayList<Integer>();
        set2AsFirstPrime();
        checkOddNumbersForSubsequentPrimes();
        return primes;
    }

    private static boolean isPrime(int candidate) {
        if (isLeastRelevantMultipleOfNextLargerPrimeFactor(candidate)) {
            multiplesOfPrimeFactors.add(candidate);
            return false;
        }
        return isNotMultipleOfAnyPreviousPrimeFactor(candidate);
    }
    // ...
}
```

**The program got longer** — one page to nearly three. Three reasons, all deliberate: longer descriptive names; function and class declarations used *as a way to add commentary*; whitespace and formatting for readability.

## Worked Example: Organizing for Change — the `Sql` Class

```java
// Before — two reasons to change: adding a new statement type, and altering the
// details of an existing one (e.g. supporting subselects). An SRP violation, visible
// from the outline alone: selectWithCriteria relates only to select statements.
public class Sql {
    public Sql(String table, Column[] columns)
    public String create()
    public String insert(Object[] fields)
    public String selectAll()
    public String findByKey(String keyColumn, String keyValue)
    public String select(Column column, String pattern)
    public String select(Criteria criteria)
    public String preparedInsert()
    private String columnList(Column[] columns)
    private String valuesList(Object[] fields, final Column[] columns)
    private String selectWithCriteria(String criteria)
    private String placeholderList(Column[] columns)
}

// After — each public method becomes its own derivative. Private methods move
// directly where they are needed; common behavior isolates into Where and ColumnList.
abstract public class Sql {
    public Sql(String table, Column[] columns)
    abstract public String generate();
}
public class CreateSql extends Sql            { @Override public String generate() }
public class SelectSql extends Sql            { @Override public String generate() }
public class InsertSql extends Sql {
    @Override public String generate()
    private String valuesList(Object[] fields, final Column[] columns)
}
public class SelectWithCriteriaSql extends Sql { @Override public String generate() }
public class SelectWithMatchSql extends Sql    { @Override public String generate() }
public class FindByKeySql extends Sql          { @Override public String generate() }
public class PreparedInsertSql extends Sql {
    @Override public String generate()
    private String placeholderList(Column[] columns)
}
public class Where      { public Where(String criteria)      public String generate() }
public class ColumnList { public ColumnList(Column[] columns) public String generate() }
```

**What this buys**: each class becomes excruciatingly simple; comprehension time drops to almost nothing; the risk that one function breaks another becomes vanishingly small; every bit of logic is isolated and easier to prove. And when `update` support arrives, **none of the existing classes change** — you drop in `UpdateSql`. That is SRP and OCP together.

## Worked Example: Isolating from Change (DIP)

`Portfolio` depending directly on `TokyoStockExchange` makes tests hostage to a live market — *"It's hard to write a test when we get a different answer every five minutes!"*

```java
public interface StockExchange {
    Money currentPrice(String symbol);
}

public class Portfolio {
    private StockExchange exchange;
    public Portfolio(StockExchange exchange) {   // injected, not constructed
        this.exchange = exchange;
    }
}

public class PortfolioTest {
    private FixedStockExchangeStub exchange;
    private Portfolio portfolio;

    @Before
    protected void setUp() throws Exception {
        exchange = new FixedStockExchangeStub();
        exchange.fix("MSFT", 100);
        portfolio = new Portfolio(exchange);
    }

    @Test
    public void GivenFiveMSFTTotalShouldBe500() throws Exception {
        portfolio.add(5, "MSFT");
        Assert.assertEquals(500, portfolio.value());
    }
}
```

*"If a system is decoupled enough to be tested in this way, it will also be more flexible and promote more reuse."* Testability is not a separate goal from good design — it is a symptom of it.

## Key Takeaways
1. Measure class size in responsibilities, not lines — five methods can be too many.
2. If you can't name a class concisely, or can't describe it in 25 words without "and"/"or", it does too much.
3. SRP: one reason to change. Extracting responsibilities usually reveals a reusable abstraction.
4. Keep cohesion high; a subset of variables used by a subset of methods is a class trying to escape.
5. Breaking large functions into small ones naturally produces small classes — follow that pressure rather than resisting it.
6. Organize for change: structure so that new features arrive by extension (OCP), not modification.
7. Depend on abstractions, not concrete details (DIP) — the payoff is isolation, reuse, and testability at once.
8. Working code and clean code are two separate activities. Finishing the first is not finishing.

## Connects To
- **Ch 3**: small functions are the mechanism that produces small classes; the Stepdown Rule orders both.
- **Ch 6**: hiding data behind abstractions is the class-level version of data abstraction.
- **Ch 9**: `FixedStockExchangeStub` is why DIP matters — decoupling *is* testability.
- **Ch 11**: dependency injection and separating construction from use scale this to whole systems.
- **Ch 12**: "many small classes" is the outcome of Rules 3 and 4 of Emergent Design.
- **Ch 17**: [G6] Code at Wrong Level of Abstraction, [G17] Misplaced Responsibility.
- **PPP (Martin, 2002)**: SRP, OCP, DIP. **RDD (Wirfs-Brock)**: responsibility-driven design. **Knuth92**: the `PrintPrimes` source.
