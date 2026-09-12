# Chapter 15: JUnit Internals

## Core Idea
A critique of `ComparisonCompactor` from JUnit — code written by Kent Beck and Erich Gamma, and *already good*. The lesson is that **no module is immune from improvement**, and the Boy Scout Rule applies even to excellent code.

## The Subject

JUnit began with Kent Beck and Erich Gamma on a plane to Atlanta — Kent wanting to learn Java, Erich wanting to learn Kent's Smalltalk testing framework. *"What could be more natural to a couple of geeks in cramped quarters than to pull out our laptops and start coding?"* Three hours of high-altitude work produced the basics of JUnit.

`ComparisonCompactor` identifies string comparison errors: given `ABCDE` and `ABXDE`, it produces `<...B[X]D...>`. The tests define the requirements better than prose can:

```java
public void testStartAndEndContextWithEllipses() {
    String failure = new ComparisonCompactor(1, "abcde", "abfde").compact(null);
    assertEquals("expected:<...b[c]d...> but was:<...b[f]d...>", failure);
}
public void testComparisonErrorOverlapingMatches() {
    String failure = new ComparisonCompactor(0, "abc", "abbc").compact(null);
    assertEquals("expected:<...[]...> but was:<...[b]...>", failure);
}
```

## Worked Example: The Refactoring, Move by Move

Each step names the heuristic it applies — this chapter is Ch 17 in action.

**1. The function name hides a side effect [N7].**

```java
public String compact(String message) {
    if (canBeCompacted()) {
        findCommonPrefix();
        findCommonSuffix();
        String compactExpected = compactString(expected);
        String compactActual = compactString(actual);
        return Assert.format(message, compactExpected, compactActual);
    } else {
        return Assert.format(message, expected, actual);
    }
}
```

*"Although it does compact the strings, it actually might not compact the strings if `canBeCompacted` returns false. So naming this function `compact` hides the side effect of the error check."* And it returns a **formatted message**, not compacted strings. →

```java
public String formatCompactedComparison(String message) {
```

**2. One function, one job [G30].** The `if` body does the actual compacting; the outer function should do all the *formatting*.

```java
private String compactExpected;
private String compactActual;

public String formatCompactedComparison(String message) {
    if (canBeCompacted()) {
        compactExpectedAndActual();
        return Assert.format(message, compactExpected, compactActual);
    } else {
        return Assert.format(message, expected, actual);
    }
}

private void compactExpectedAndActual() {
    findCommonPrefix();
    findCommonSuffix();
    compactExpected = compactString(expected);
    compactActual = compactString(actual);
}
```

**3. Inconsistent conventions inside one function [G11].** *"I don't like the way that the last two lines of the new function return variables, but the first two don't."* → make `findCommonPrefix`/`findCommonSuffix` return their values.

**4. Hidden temporal coupling [G31].** Careful inspection shows `findCommonSuffix` depends on `prefixIndex` having been computed by `findCommonPrefix`. *"If these two functions were called out of order, there would be a difficult debugging session ahead."*

First attempt — pass `prefixIndex` as an argument to expose the dependency:

```java
private int findCommonSuffix(int prefixIndex) { ... }
```

**5. Reject your own fix [G32].** *"I'm not really happy with this. The passing of the `prefixIndex` argument is a bit arbitrary. It works to establish the ordering but does nothing to explain the need for that ordering. Another programmer might undo what we have done because there's no indication that the parameter is really needed."*

Better: fold the ordering into the name and the call.

```java
private void compactExpectedAndActual() {
    findCommonPrefixAndSuffix();
    compactExpected = compactString(expected);
    compactActual = compactString(actual);
}

private void findCommonPrefixAndSuffix() {
    findCommonPrefix();                    // ordering is now structural, not conventional
    int expectedSuffix = expected.length() - 1;
    int actualSuffix = actual.length() - 1;
    for (; actualSuffix >= prefixIndex && expectedSuffix >= prefixIndex;
           actualSuffix--, expectedSuffix--) {
        if (expected.charAt(expectedSuffix) != actual.charAt(actualSuffix))
            break;
    }
    suffixIndex = expected.length() - expectedSuffix;
}
```

*"That establishes the temporal nature of the two functions in a much more dramatic way than the previous solution. It also points out how ugly `findCommonPrefixAndSuffix` is."*

**6. Clean up what the previous step exposed.**

```java
private void findCommonPrefixAndSuffix() {
    findCommonPrefix();
    int suffixLength = 1;
    for (; !suffixOverlapsPrefix(suffixLength); suffixLength++) {
        if (charFromEnd(expected, suffixLength) != charFromEnd(actual, suffixLength))
            break;
    }
    suffixIndex = suffixLength;
}

private char charFromEnd(String s, int i) { return s.charAt(s.length() - i); }

private boolean suffixOverlapsPrefix(int suffixLength) {
    return actual.length()   - suffixLength < prefixLength ||
           expected.length() - suffixLength < prefixLength;
}
```

**7. The name was lying about the concept [N1], [G33].** The cleanup exposes that `suffixIndex` **is really the length of the suffix**. But it is 1-based, not zero-based — *"and so is not a true length. This is also the reason that there are all those `+1`s in `computeCommonSuffix`."*

Fixing the base fixes the name and removes the arithmetic noise:

```java
private int suffixLength;   // renamed, and now genuinely a length

private void findCommonPrefixAndSuffix() {
    findCommonPrefix();
    suffixLength = 0;                                    // was 1
    for (; !suffixOverlapsPrefix(suffixLength); suffixLength++) {
        if (charFromEnd(expected, suffixLength) != charFromEnd(actual, suffixLength))
            break;
    }
}

private char charFromEnd(String s, int i) { return s.charAt(s.length() - i - 1); }  // the -1 moved here

private boolean suffixOverlapsPrefix(int suffixLength) {
    return actual.length()   - suffixLength <= prefixLength ||   // < became <=
           expected.length() - suffixLength <= prefixLength;
}
```

*"We replaced the `+1`s in `computeCommonSuffix` with a `-1` in `charFromEnd`, where it makes perfect sense, and two `<=` operators in `suffixOverlapsPrefix`, where they also make perfect sense. This allowed us to change the name of `suffixIndex` to `suffixLength`, greatly enhancing the readability of the code."*

## Mental Models
- **Refactoring is a dialogue, not a checklist.** Step 4's fix is *rejected* in step 5 for a better one; step 6's cleanup only became visible because of step 5. Each move reveals the next.
- **Push arithmetic to where it makes sense.** Scattered `+1`s are usually one off-by-one convention in the wrong place. Fixing the convention deletes all of them at once.
- **When cleanup exposes a name that was never true, the name was the bug.** `suffixIndex` → `suffixLength` was not cosmetic; it corrected a misconception encoded throughout the class.
- **Structural ordering beats conventional ordering.** Having `findCommonPrefixAndSuffix` call `findCommonPrefix` first states the dependency in a way another programmer cannot accidentally undo.

## Key Takeaways
1. A function's name must cover everything it does, including its guard conditions and its formatting.
2. Split "do the thing" from "format the result" — one function, one job.
3. Use consistent conventions within a function; mixed return-vs-mutate styles are a smell.
4. Expose temporal couplings structurally; an argument that only *encodes* ordering will be removed by someone who doesn't know why.
5. Be willing to undo your own refactoring when it establishes correctness without communicating it.
6. Off-by-one conventions belong in one place; `+1`s scattered across a class point at a mis-based variable.
7. *"No module is immune from improvement, and each of us has the responsibility to leave the code a little better than we found it."*

## Connects To
- **Ch 1**: the Boy Scout Rule — this chapter is its demonstration on already-good code.
- **Ch 2**: [N1] names must be descriptive; [N7] names must describe side effects.
- **Ch 3**: [G30] functions should do one thing; extracting `compactExpectedAndActual` is the Stepdown Rule.
- **Ch 9**: the test list *is* the specification — Martin reads the requirements out of `ComparisonCompactorTest`.
- **Ch 17**: [G11] Inconsistency, [G30] Functions Should Do One Thing, [G31] Hidden Temporal Couplings, [G32] Don't Be Arbitrary, [G33] Encapsulate Boundary Conditions.
