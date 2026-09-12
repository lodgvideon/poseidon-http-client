# Chapter 9: Unit Tests

## Core Idea
**Test code is just as important as production code** — it is not a second-class citizen. Tests are what keep production code flexible, maintainable, and reusable, because tests are what remove the fear of change.

## Frameworks Introduced

- **The Three Laws of TDD**
  1. **First Law**: You may not write production code until you have written a failing unit test.
  2. **Second Law**: You may not write more of a unit test than is sufficient to fail — **and not compiling is failing.**
  3. **Third Law**: You may not write more production code than is sufficient to pass the currently failing test.

  - Effect: these lock you into a cycle **roughly thirty seconds long**. Tests and production code are written together, tests just seconds ahead.
  - Consequence: dozens of tests a day, thousands a year, covering virtually all production code — a bulk that can rival the production code itself and presents a real management problem.

- **Tests Enable the -ilities** — *"It is unit tests that keep our code flexible, maintainable, and reusable."*
  - Mechanism: **if you have tests, you do not fear making changes.** Without tests, every change is a possible bug, and no amount of architectural elegance compensates.
  - The higher the coverage, the less the fear — *"You can make changes with near impunity to code that has a less than stellar architecture and a tangled and opaque design. Indeed, you can improve that architecture and design without fear!"*
  - **Tests enable all the -ilities, because tests enable change.**

- **Build-Operate-Check** — the three-part structure a clean test should make obvious: build the test data, operate on it, check that the operation produced the expected result.

- **Domain-Specific Testing Language** — instead of calling the production APIs directly, build a set of functions and utilities *on top* of them that make tests convenient to write and easy to read.
  - Key point: **this API is not designed up front.** It evolves from the continued refactoring of test code that has gotten too tainted by obfuscating detail.

- **A Dual Standard** — the code within the testing API has different engineering standards than production code. It must still be simple, succinct, and expressive, but **it need not be as efficient**.
  - Boundary: the dual standard covers issues of memory or CPU efficiency. *"But they never involve issues of cleanliness."*

- **One Assert per Test** — a good **guideline**, not a law. Martin tries to create a DSL that supports it, *"but I am not afraid to put more than one assert in a test."*
  - The escape hatches when splitting causes duplication: **Template Method** (given/when in the base class, then in derivatives), or a separate test class with given/when in `@Before`. Martin judges both *"too much mechanism for such a minor issue."*
  - **Single Concept per Test** — the better rule. Test one concept per function; minimize asserts *per concept*.

- **F.I.R.S.T.**

| Letter | Rule | Why |
|---|---|---|
| **Fast** | Tests should run quickly | Slow tests don't get run; problems aren't found early; you stop feeling free to clean up; code rots |
| **Independent** | No test sets up conditions for another; any order | Otherwise the first failure cascades, hiding downstream defects |
| **Repeatable** | Runnable in any environment — prod, QA, laptop on a train with no network | Otherwise you always have an excuse for failures, and can't run them when the environment is unavailable |
| **Self-Validating** | Boolean output: pass or fail | No reading log files, no manually diffing text — otherwise failure becomes subjective |
| **Timely** | Written **just before** the production code that passes them | Write tests after and you may find the code hard to test — or decide some of it is too hard, having never designed it to be testable |

## Worked Example: Refactoring Tests into a Testing Language

```java
// Before — three tests drowning in irrelevant detail: PathParser transformations,
// responder construction, response casting, ham-handed URL assembly. Plus heavy
// duplication [G5] in the repeated addPage / assertSubString calls.
public void testGetPageHieratchyAsXml() throws Exception {
    crawler.addPage(root, PathParser.parse("PageOne"));
    crawler.addPage(root, PathParser.parse("PageOne.ChildOne"));
    crawler.addPage(root, PathParser.parse("PageTwo"));

    request.setResource("root");
    request.addInput("type", "pages");
    Responder responder = new SerializedPageResponder();
    SimpleResponse response =
        (SimpleResponse) responder.makeResponse(new FitNesseContext(root), request);
    String xml = response.getContent();

    assertEquals("text/xml", response.getContentType());
    assertSubString("<name>PageOne</name>", xml);
    assertSubString("<name>PageTwo</name>", xml);
    assertSubString("<name>ChildOne</name>", xml);
}

// After — same behavior, Build-Operate-Check visible at a glance.
public void testGetPageHierarchyAsXml() throws Exception {
    makePages("PageOne", "PageOne.ChildOne", "PageTwo");

    submitRequest("root", "type:pages");

    assertResponseIsXML();
    assertResponseContains(
        "<name>PageOne</name>", "<name>PageTwo</name>", "<name>ChildOne</name>"
    );
}

public void testSymbolicLinksAreNotInXmlPageHierarchy() throws Exception {
    WikiPage page = makePage("PageOne");
    makePages("PageOne.ChildOne", "PageTwo");
    addLinkTo(page, "PageTwo", "SymPage");

    submitRequest("root", "type:pages");

    assertResponseIsXML();
    assertResponseContains(
        "<name>PageOne</name>", "<name>PageTwo</name>", "<name>ChildOne</name>"
    );
    assertResponseDoesNotContain("SymPage");
}
```

*"In the end, this code was not designed to be read. The poor reader is inundated with a swarm of details that must be understood before the tests make any real sense."*

**And the given-when-then variant** used when splitting for one-assert-per-test:

```java
public void testGetPageHierarchyAsXml() throws Exception {
    givenPages("PageOne", "PageOne.ChildOne", "PageTwo");
    whenRequestIsIssued("root", "type:pages");
    thenResponseShouldBeXML();
}
```

## Worked Example: The Dual Standard in Action

```java
// Before — your eye must bounce between the state name and its sense:
// heaterState → glissade left to assertTrue; coolerState → track left to assertFalse.
// "This is tedious and unreliable."
@Test
public void turnOnLoTempAlarmAtThreashold() throws Exception {
    hw.setTemp(WAY_TOO_COLD);
    controller.tic();
    assertTrue(hw.heaterState());
    assertTrue(hw.blowerState());
    assertFalse(hw.coolerState());
    assertFalse(hw.hiTempAlarm());
    assertTrue(hw.loTempAlarm());
}

// After — one line per test. Upper case = on, lower case = off, always in the order
// {heater, blower, cooler, hi-temp-alarm, lo-temp-alarm}.
@Test public void turnOnCoolerAndBlowerIfTooHot()  { tooHot();     assertEquals("hBChl", hw.getState()); }
@Test public void turnOnHeaterAndBlowerIfTooCold() { tooCold();    assertEquals("HBchl", hw.getState()); }
@Test public void turnOnHiTempAlarmAtThreshold()   { wayTooHot();  assertEquals("hBCHl", hw.getState()); }
@Test public void turnOnLoTempAlarmAtThreshold()   { wayTooCold(); assertEquals("HBchL", hw.getState()); }

// The supporting mock — deliberately inefficient string concatenation.
public String getState() {
    String state = "";
    state += heater      ? "H" : "h";
    state += blower      ? "B" : "b";
    state += cooler      ? "C" : "c";
    state += hiTempAlarm ? "H" : "h";
    state += loTempAlarm ? "L" : "l";
    return state;
}
```

Martin flags his own move honestly: the encoded string is *"close to a violation of the rule about mental mapping"* (Ch 2) — but once you know the meaning, your eyes glide across it. And `getState()` should use a `StringBuffer` in production: this is an embedded real-time system with constrained memory. **The test environment is not constrained at all.** That is the dual standard.

## Worked Example: One Concept per Test

```java
/**
 * Miscellaneous tests for the addMonths() method.
 */
public void testAddMonths() {
    SerialDate d1 = SerialDate.createInstance(31, 5, 2004);

    SerialDate d2 = SerialDate.addMonths(1, d1);
    assertEquals(30, d2.getDayOfMonth());
    assertEquals(6, d2.getMonth());
    assertEquals(2004, d2.getYYYY());

    SerialDate d3 = SerialDate.addMonths(2, d1);
    assertEquals(31, d3.getDayOfMonth());
    // ... and a third, unrelated concept
}
```

This is three independent tests fused into one, forcing the reader to work out why each section exists. Restating them as separate given/when/then cases reveals **a general rule hiding amidst the miscellaneous tests**: *when you increment the month, the date can be no greater than the last day of the month.* Which implies incrementing February 28th should yield March 28th — **a test that is missing** and would be useful to write.

*"So it's not the multiple asserts in each section that causes the problem. Rather it is the fact that there is more than one concept being tested."*

## Anti-patterns
- **The dirty-tests decision** — a team explicitly held test code to a lower standard; "quick and dirty" was the watchword. The full failure chain Martin observed:
  1. Tests must change as production code evolves; dirty tests are hard to change.
  2. Cramming new tests into the suite starts costing more than writing the production code.
  3. Old tests fail on modification, and the mess makes them hard to fix.
  4. Tests become viewed as an ever-increasing liability; maintenance cost rises release over release.
  5. Developers blame the tests for growing estimates; the suite is **discarded entirely**.
  6. Without tests, changes can't be verified — defect rate rises.
  7. Fearing change, they stop cleaning production code; **the production code rots.**

  *"Having dirty tests is equivalent to, if not worse than, having no tests."* Their testing effort did fail them — but *"it was their decision to allow the tests to be messy that was the seed of that failure."*
- **Throwaway manual tests** — Martin's 1990s timer test: type a rhythmic melody on the keyboard, wait five seconds, watch it replay. *"That was my test! Once I saw it work and demonstrated it to my colleagues, I threw the test code away."* Today he would mock the timing functions to gain absolute control over time, step it forward, and check flags — and ensure the tests are **convenient for anyone else to run** and **checked in together with the code**.

## Key Takeaways
1. Follow the Three Laws of TDD; the cycle is seconds long, not sprints long.
2. Keep test code as clean as production code — dirty tests get abandoned, and then the production code rots.
3. What makes a clean test: **readability, readability, and readability** — clarity, simplicity, and density of expression.
4. Structure tests as Build-Operate-Check, and refactor them into a domain-specific testing language as detail accumulates.
5. The dual standard permits inefficiency in tests, never uncleanliness.
6. Prefer one *concept* per test over dogmatic one-assert-per-test; minimize asserts per concept.
7. Apply F.I.R.S.T. — especially **Timely**: tests written after the code find code that was never designed to be testable.
8. *"If you let the tests rot, then your code will rot too."*

## Connects To
- **Ch 1**: Big Dave Thomas — *"code without tests is not clean."* This chapter is that claim developed.
- **Ch 2**: the encoded state string tests the limits of "Avoid Mental Mapping" — and is judged worth it.
- **Ch 3**: "I also have a suite of unit tests that cover every one of those clumsy lines" — refactoring is only safe under test.
- **Ch 8**: learning tests are this discipline aimed at third-party code.
- **Ch 10**: DIP + injected interfaces are what make classes testable in isolation (`FixedStockExchangeStub`).
- **Ch 12**: "Runs all the tests" is Rule 1 of Emergent Design.
- **Ch 17**: [G5] Duplication; the T-series heuristics (T1–T9) restate these rules.
