# Chapter 5: Formatting

## Core Idea
Formatting is communication, and communication is the professional developer's first order of business. Your functionality will likely change next release; your style and discipline survive long after the code has been changed beyond recognition.

## Frameworks Introduced

- **The Newspaper Metaphor** — the organizing model for a source file.
  - Headline (the file/class name) is simple but explanatory, sufficient by itself to tell you whether you're in the right module.
  - The first paragraph (topmost functions) gives high-level concepts and algorithms, hiding detail.
  - Detail increases as you move downward; the lowest-level functions come last.
  - A newspaper works because it is many small articles — not one long disorganized agglomeration of facts.

- **Vertical Openness Between Concepts** — each group of lines is a complete thought; separate thoughts with blank lines. Each blank line is a visual cue: your eye is drawn to the first line after it. Removing them turns a readable class into "a muddle" — try unfocusing your eyes on both versions.

- **Vertical Density** — lines that are tightly related should be vertically dense. Useless javadocs interleaved between two instance variables destroy their association; without them the class fits in *"an eye-full"* — two variables and a method visible without moving your head.

- **Vertical Distance** — closely related concepts should be kept vertically close [G10]. Concepts that belong together should not be separated into different files without a very good reason. *(This is one reason to avoid protected variables.)*

- **Vertical Ordering** — function call dependencies point **downward**: the callee sits below the caller. This is the exact opposite of Pascal/C/C++, which force declaration before use. The result: you can skim the first few functions and get the gist without immersing yourself in detail.

- **Team Rules** *(a play on words)* — every programmer has favorite formatting rules, but **if he works in a team, then the team rules.** Agree on one style, encode it into the IDE's formatter, and comply — even when they are not the rules you prefer. Martin's FitNesse team did this in about 10 minutes in 2002 and never revisited it.

## Reference Tables

**Vertical sizing (measured across JUnit, FitNesse, testNG, Time and Money, JDepend, Ant, Tomcat):**

| Metric | Value |
|---|---|
| FitNesse average file | ~65 lines |
| FitNesse typical range | 40 – 100+ lines (~⅓ of files) |
| FitNesse largest / smallest | ~400 / 6 lines |
| Whole system built this way | FitNesse ≈ 50,000 lines |
| **Recommended target** | **~200 lines typical, 500 upper limit** |

Junit, FitNesse, and Time and Money use small files; Tomcat and Ant have files several thousand lines long with close to half over 200. *"Small files are usually easier to understand than large files."*

**Where declarations go:**

| Kind | Placement |
|---|---|
| Local variables | Top of the function (functions are short, so this *is* close to use) |
| Loop control variables | Inside the loop statement — `for (Test each : tests)` |
| Rare exception | Top of a block or just before a loop, in a long-ish function |
| Instance variables | **Top of the class**, in one well-known place |

On instance variables: C++ had the "scissors rule" (all at the bottom); Java convention is top. *"The important thing is for the instance variables to be declared in one well-known place. Everybody should know where to go to see the declarations."* JUnit 4.3.1's `TestSuite` hides two of them halfway down the class — *"It would be hard to hide them in a better place."*

**Horizontal formatting:**

| Question | Answer |
|---|---|
| Line width | Programmers clearly prefer short lines — ~40% of lines fall between 20 and 60 chars |
| Hollerith limit of 80 | "a bit arbitrary" |
| Acceptable | 100 or even 120 |
| **Martin's personal limit** | **120** — beyond that "is probably just careless" |
| Shrinking the font to fit 200 across | **Don't do that** |

## Code Examples

**Horizontal openness — spacing that carries meaning:**

```java
private void measureLine(String line) {
    lineCount++;
    int lineSize = line.length();
    totalChars += lineSize;
    lineWidthHistogram.addLine(lineSize, lineCount);
    recordWidestLine(lineSize);
}
```
- Spaces around assignment operators accentuate the two major elements: left side and right side.
- **No** space between a function name and its opening paren — the function and its arguments are closely related; separating them makes them look disjoined.
- Spaces after commas inside the arg list accentuate that arguments are separate.

**White space encoding operator precedence:**

```java
public static double root1(double a, double b, double c) {
    double determinant = determinant(a, b, c);
    return (-b + Math.sqrt(determinant)) / (2*a);
}
private static double determinant(double a, double b, double c) {
    return b*b - 4*a*c;
}
```
- Factors have **no** space (high precedence); terms are separated (addition/subtraction, lower precedence). Caveat: most reformatters are blind to precedence and will destroy this.

**Dependent functions, caller above callee** — from `WikiPageResponder`, the topmost function calls those below it, which call those below them:

```java
public Response makeResponse(FitNesseContext context, Request request) throws Exception {
    String pageName = getPageNameOrDefault(request, "FrontPage");
    loadPage(pageName, context);
    if (page == null)
        return notFoundResponse(context, request);
    else
        return makePageResponse(context);
}

private String getPageNameOrDefault(Request request, String defaultPageName) {
    String pageName = request.getResource();
    if (StringUtil.isBlank(pageName))
        pageName = defaultPageName;
    return pageName;
}
```
- Aside: `"FrontPage"` is passed *down* rather than buried inside `getPageNameOrDefault` — keeping constants at the appropriate level [G35]. Burying it would hide a well-known constant in an inappropriately low-level function.

**Conceptual affinity** — from `Assert` in JUnit 4.3.1:

```java
static public void assertTrue(String message, boolean condition) {
    if (!condition) fail(message);
}
static public void assertTrue(boolean condition) { assertTrue(null, condition); }
static public void assertFalse(String message, boolean condition) {
    assertTrue(message, !condition);
}
static public void assertFalse(boolean condition) { assertFalse(null, condition); }
```
- These want to be adjacent because they share a naming scheme and perform variations of one task. **The fact that they call each other is secondary** — even if they didn't, they'd belong together.

## Anti-patterns
- **Horizontal alignment** — lining up variable names or rvalues in columns. Martin did this as an assembly programmer and abandoned it: alignment *"emphasizes the wrong things and leads my eye away from the true intent."* You read down the names without seeing the types, down the rvalues without seeing the assignment operators. Reformatters destroy it anyway.
  - **The real signal**: if you have a list long enough to need alignment, the problem is **the length of the list, not the lack of alignment** — the class should be split.
- **Breaking indentation** — collapsing scopes onto one line for short `if`s, loops, or functions: `public CommentWidget(ParentWidget parent, String text){super(parent, text);}`. Martin reports he has *"almost always gone back and put the indentation back in."* Expand and indent instead.
- **Dummy scopes** — `while (dis.read(buf, 0, readBufferSize) != -1);`. Avoid them; when unavoidable, put the semicolon on its own indented line with braces. *"I can't tell you how many times I've been fooled by a semicolon silently sitting at the end of a while loop."*
- **Individual styles in one codebase** — a system should read as a set of documents with a consistent, smooth style, not as if written by a bunch of disagreeing individuals.

## Worked Example: Indentation Alone

The chapter presents two syntactically and semantically identical versions of `FitNesseServer` — one indented, one not. The indented one lets you *"almost instantly spot the variables, constructors, accessors, and methods"* and grasp in a few seconds that it is a simple front end to a socket with a timeout. The unindented one is *"virtually impenetrable without intense study."*

The mechanism: a source file is a **hierarchy like an outline** — file, class, method, block, nested block — and each level is a scope. Indentation makes that hierarchy visible, so programmers can visually line up the left edge, hop over irrelevant scopes, and scan for new declarations. *"Without indentation, programs would be virtually unreadable by humans."*

Martin's own rules are not stated as a list — they are demonstrated by `CodeAnalyzer.java` (Listing 5-6): *"Consider this an example of how code makes the best coding standard document."*

## Key Takeaways
1. Formatting is about communication, not aesthetics — and it outlives the code it formats.
2. Aim for files around 200 lines, 500 max; significant systems are built this way.
3. Structure the file like a newspaper article: name, then high-level concepts, then increasing detail.
4. Use blank lines to separate thoughts and density to bind related lines; both are load-bearing.
5. Keep callers above callees and related concepts vertically close; declare instance variables in one well-known place.
6. Keep lines short — 120 is a reasonable ceiling.
7. Drop horizontal alignment; a list long enough to need it is a list that should be split.
8. The team's style beats your style. Encode it in the IDE and stop debating.

## Connects To
- **Ch 3**: the Stepdown Rule is the vertical-ordering rule at the function level; both make the module read top-down.
- **Ch 4**: closing-brace comments, banners, and noisy javadocs are formatting failures as much as comment failures.
- **Ch 10**: "the problem is the length of the list, not the lack of alignment" — file size and class size are the same question in Java.
- **Ch 17**: [G10] Vertical Separation, [G35] Keep Configurable Data at High Levels, [G36] Avoid Transitive Navigation.
