# Chapter 2: Meaningful Names

*by Tim Ottinger*

## Core Idea
A name must answer why the thing exists, what it does, and how it is used — if a name needs a comment, the name has failed. Naming is not decoration; it is the primary mechanism for making context explicit in code.

## Frameworks Introduced

- **Use Intention-Revealing Names**
  - When to use: every declaration, always.
  - How: name what is being measured *and its unit*. `int d;` → `elapsedTimeInDays`, `daysSinceCreation`, `fileAgeInDays`.
  - Test: if the name requires a trailing comment to be understood, replace the name.

- **Avoid Disinformation**
  - How: never use a word whose entrenched meaning differs from yours. Don't call a grouping `accountList` unless it is actually a `List` — use `accounts`. Avoid `hp`, `aix`, `sco` (Unix platforms). Never use lowercase `l` or uppercase `O` — they read as `1` and `0`.
  - Guard: names differing in small ways (`XYZControllerForEfficientHandlingOfStrings` vs `...ForEfficientStorageOfStrings`) are disinformation, because autocomplete picks by shape.

- **Make Meaningful Distinctions**
  - How: if two names must differ, they must *mean* something different. Ban number series (`a1`, `a2`) and noise words (`Info`, `Data`, `Object`, `Variable`, `Table`, `String`).
  - Failure tell: `getActiveAccount()`, `getActiveAccounts()`, `getActiveAccountInfo()` in one API — nobody can tell which to call.

- **Use Pronounceable Names**
  - Why: programming is a social activity; if you can't pronounce it, you can't discuss it. `genymdhms` → `generationTimestamp`.

- **Use Searchable Names**
  - Rule: **the length of a name should correspond to the size of its scope.** Single-letter names are acceptable *only* as local variables inside short methods.
  - How: replace magic numbers with named constants — grepping `WORK_DAYS_PER_WEEK` works; grepping `5` does not.

- **Avoid Encodings** — Hungarian Notation, `m_` member prefixes, and `I`-prefixed interfaces are all obsolete burdens.
  - Interface rule: **leave the interface unadorned.** Prefer `ShapeFactory` + `ShapeFactoryImp` over `IShapeFactory` + `ShapeFactory`. Users should not know they hold an interface.

- **Pick One Word per Concept** — one word per abstract concept, held consistently across the codebase. Don't mix `fetch`/`retrieve`/`get`, or `Manager`/`Controller`/`Driver`, for the same idea.

- **Don't Pun** — the inverse guard. Don't reuse a word when the semantics differ: if existing `add` methods concatenate two values, a method that puts one item into a collection must be `insert` or `append`.

- **Add Meaningful Context** — place names inside well-named classes, functions, or namespaces. Prefixing (`addrState`) is a last resort; a class (`Address`) is the real answer.

- **Don't Add Gratuitous Context** — prefixing every class in "Gas Station Deluxe" with `GSD` fights your IDE. Add no more context than necessary.

## Key Concepts
- **Implicity**: (coined here) the degree to which context is *not* explicit in the code itself. The enemy is not complexity but implicity.
- **Noise words**: `Info`, `Data`, `Object`, `an`, `the`, `Variable`, `Table` — words that differentiate names without differentiating meaning.
- **Mental mapping**: forcing the reader to translate your name into the concept they already hold. `r` for "lowercased url minus host and scheme" is showing off, not clarity.
- **Class names**: nouns or noun phrases — `Customer`, `WikiPage`, `AddressParser`. **Never a verb.** Avoid `Manager`, `Processor`, `Data`, `Info`.
- **Method names**: verbs or verb phrases — `postPayment`, `deletePage`, `save`. Accessors/mutators/predicates take `get`/`set`/`is` per the JavaBean standard.
- **Solution domain names**: use CS terms, pattern names, algorithm names — `AccountVisitor`, `JobQueue`. Readers are programmers.
- **Problem domain names**: when there is no programmer-ese, take the domain's word, so a maintainer can ask a domain expert.

## Mental Models
- **Clarity is king.** The difference between a smart programmer and a professional one: the professional uses their powers for good and writes code others can understand.
- **Write for the paperback, not the journal.** The author is responsible for being clear — not the reader for digging meaning out. Code should be a quick skim, not an intense study.
- **Rename fearlessly.** Modern IDEs make renaming cheap; don't memorize names, use tools. You may surprise someone — that is not a reason to stop.
- **Overloaded constructors → static factory methods** named for their arguments: `Complex.FromRealNumber(23.0)` beats `new Complex(23.0)`. Consider making the constructor private to enforce it.

## Code Examples

The chapter's central demonstration — three passes over one function, each pass adding explicitness without adding complexity:

```java
// Pass 1 — simple but implicit. What is theList? What is [0]? What is 4?
public List<int[]> getThem() {
    List<int[]> list1 = new ArrayList<int[]>();
    for (int[] x : theList)
        if (x[0] == 4)
            list1.add(x);
    return list1;
}

// Pass 2 — same operators, same nesting, same constant count. Names only.
public List<int[]> getFlaggedCells() {
    List<int[]> flaggedCells = new ArrayList<int[]>();
    for (int[] cell : gameBoard)
        if (cell[STATUS_VALUE] == FLAGGED)
            flaggedCells.add(cell);
    return flaggedCells;
}

// Pass 3 — a Cell class hides the magic numbers behind an intention-revealing predicate.
public List<Cell> getFlaggedCells() {
    List<Cell> flaggedCells = new ArrayList<Cell>();
    for (Cell cell : gameBoard)
        if (cell.isFlagged())
            flaggedCells.add(cell);
    return flaggedCells;
}
```
- **What it demonstrates**: the code never got simpler — it got *explicit*. Naming, not restructuring, carried the whole improvement.

## Reference Tables

| Rule | Bad | Good |
|---|---|---|
| Intention-revealing | `int d; // elapsed time in days` | `int elapsedTimeInDays;` |
| No disinformation | `accountList` (not a List) | `accounts` |
| Meaningful distinction | `copyChars(char a1[], char a2[])` | `copyChars(char source[], char destination[])` |
| Pronounceable | `class DtaRcrd102 { Date genymdhms; }` | `class Customer { Date generationTimestamp; }` |
| Searchable | `for (int j=0; j<34; j++) s += (t[j]*4)/5;` | `WORK_DAYS_PER_WEEK`, `NUMBER_OF_TASKS`, `realDaysPerIdealDay` |
| No encoding | `PhoneNumber phoneString;` | `PhoneNumber phoneNumber;` |
| No member prefix | `private String m_dsc;` | `private String description;` |
| Interface naming | `IShapeFactory` / `ShapeFactory` | `ShapeFactory` / `ShapeFactoryImp` |
| No gratuitous context | `GSDAccountAddress` | `Address` (instances: `accountAddress`) |

## Worked Example: Extracting Context into a Class

Variables whose context must be *inferred* from the algorithm are a signal that a class is hiding inside the function.

```java
// Before — number, verb, pluralModifier have no context until you read the whole body.
private void printGuessStatistics(char candidate, int count) {
    String number;
    String verb;
    String pluralModifier;
    if (count == 0) {
        number = "no";  verb = "are"; pluralModifier = "s";
    } else if (count == 1) {
        number = "1";   verb = "is";  pluralModifier = "";
    } else {
        number = Integer.toString(count); verb = "are"; pluralModifier = "s";
    }
    String guessMessage = String.format(
        "There %s %s %s%s", verb, number, candidate, pluralModifier);
    print(guessMessage);
}

// After — the three variables become fields of GuessStatisticsMessage. Context is now
// definitive, which in turn *allows* the algorithm to split into small named functions.
public class GuessStatisticsMessage {
    private String number;
    private String verb;
    private String pluralModifier;

    public String make(char candidate, int count) {
        createPluralDependentMessageParts(count);
        return String.format("There %s %s %s%s", verb, number, candidate, pluralModifier);
    }

    private void createPluralDependentMessageParts(int count) {
        if (count == 0)      { thereAreNoLetters(); }
        else if (count == 1) { thereIsOneLetter(); }
        else                 { thereAreManyLetters(count); }
    }

    private void thereAreManyLetters(int count) {
        number = Integer.toString(count); verb = "are"; pluralModifier = "s";
    }
    private void thereIsOneLetter()  { number = "1";  verb = "is";  pluralModifier = ""; }
    private void thereAreNoLetters() { number = "no"; verb = "are"; pluralModifier = "s"; }
}
```
- **The move**: adding context (a class) unlocked decomposition (small functions). The two disciplines compound.

## Anti-patterns
- **`klass` because `class` was taken** — renaming arbitrarily to satisfy the compiler.
- **Cuteness**: `HolyHandGrenade` for `DeleteItems`, `whack()` for `kill()`, `eatMyShorts()` for `abort()`. Memorable only to those who share the joke, only while they remember it. *Say what you mean. Mean what you say.*
- **`l` and `O` as variables**: `int a = l; if ( O == l ) a = O1; else l = 01;` — the "solution" of changing fonts is passed down as oral tradition; renaming ends it with finality.
- **Type encoded in the name** — `PhoneNumber phoneString;` — the name is not updated when the type changes, so the encoding actively lies.

## Key Takeaways
1. If a name needs a comment, it does not reveal intent — change the name.
2. Name length should track scope size; single letters only inside short methods (and never `l`).
3. Distinctions must carry meaning: number series and noise words satisfy the compiler, not the reader.
4. Replace magic numbers with searchable named constants — searchability beats brevity.
5. Encodings (Hungarian, `m_`, `I`-prefix) are obsolete impediments that make names harder to change and easier to falsify.
6. One word per concept, and never the same word for two concepts.
7. Prefer creating a class over prefixing variables when you need context.
8. Renaming is cheap and good. Fear of objection is not a reason to keep a bad name.

## Connects To
- **Ch 3**: small functions make member prefixes and long scopes unnecessary — the two chapters reinforce each other.
- **Ch 6**: `Address` as a class vs. `addrState` prefixes is the data-abstraction question.
- **Ch 10**: extracting `GuessStatisticsMessage` is the Single Responsibility Principle applied through naming.
- **Ch 17 (N1–N7)**: the naming heuristics restate these rules as a review checklist.
- **N5**: "the length of a name should correspond to the size of its scope."
