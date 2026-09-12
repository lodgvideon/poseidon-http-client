# Chapter 16: Refactoring SerialDate

## Core Idea
A **professional review** of real open-source code (`org.jfree.date.SerialDate` by David Gilbert), executed in two ordered phases: **First, Make It Work** — then **Make It Right**.

## The Framing: What a Code Review Is

*"For all intents and purposes, this is 'good code.' And I am going to rip it to pieces."*

Martin makes the terms explicit before starting:

> *"This is not an activity of malice. Nor do I think that I am so much better than David that I somehow have a right to pass judgment on his code. Indeed, if you were to find some of my code, I'm sure you could find plenty of things to complain about."*

> *"What I am about to do is nothing more and nothing less than a professional review. It is something that we should all be comfortable doing. **And it is something we should welcome when it is done for us.** It is only through critiques like these that we will learn. Doctors do it. Pilots do it. Lawyers do it. And we programmers need to learn how to do it too."*

And credit where due: *"David had the courage and good will to offer his code to the community at large for free. He placed it out in the open for all to see and invited public usage and public scrutiny. **This was well done!**"*

## Framework: First, Make It Work

**The order is the method.** You cannot safely refactor code whose behavior you cannot verify, so coverage comes before cleanup.

1. **Run the existing tests.** They all pass — but *"a quick inspection of the tests shows that they don't test everything"* [T1]. A "Find Usages" on `MonthCodeToQuarter` shows it is never called [F4], so nothing tests it.
2. **Measure, don't assume.** Clover reports the existing tests execute **91 of 185 executable statements (~50%)** [T2]. *"The coverage map looks like a patchwork quilt, with big gobs of unexecuted code littered all through the class."*
3. **Write an independent suite.** Coverage rises to **170 of 185 (92%)** — even with some tests commented out.
4. **Comment out tests for behavior you believe should exist.** *"They represent behavior that I think `SerialDate` should have. So as I refactor `SerialDate`, I'll be working to make these tests pass too."* Commented-out tests become the to-do list. (Note the tension with [C5] "Commented-Out Code" — here they are a deliberate, temporary work queue, not abandoned cruft.)
5. **Fix the bugs the new tests expose**, then refactor.

## Worked Example: Bugs Found by Coverage, Not by Reading

**Case-sensitivity [G2].** `testWeekdayCodeToString` obviously should not be case sensitive. *"Writing these tests was trivial [T3]. Making them pass was even easier"* — change two lines to `equalsIgnoreCase`. Two tests stay commented out because *"it's not clear to me that the 'tues' and 'thurs' abbreviations ought to be supported"* — an honest open question rather than a silent decision.

**A boundary condition error [T5].** December 25th, 2004 was a Saturday; the following Saturday was January 1st, 2005. But `getFollowingDayOfWeek` returns **December 25th** as the Saturday following December 25th [G3], [T1]. One operator:

```java
// was:  if (baseDOW > targetWeekday) {
if (baseDOW >= targetWeekday) {
```

**The revealing detail** [T6]: the change history shows *"bugs were fixed"* in `getPreviousDayOfWeek`, `getFollowingDayOfWeek`, and `getNearestDayOfWeek` before — the same functions are still wrong. **A history of repairs in one place predicts more defects there.**

**A pattern of failures is itself information** [T7]. `testGetNearestDayOfWeek` grew long and exhaustive *because the initial cases did not all pass.* Looking at *which* cases are commented out reveals the shape: **the algorithm fails if the nearest day is in the future** — another boundary condition error [T5].

**Coverage as a correctness oracle** [T8]. *"Line 719 never gets executed! This means that the `if` statement in line 718 is always false."* Inspection confirms it: `adjust` is always negative, so it can never be ≥ 4. **The algorithm is simply wrong.** The replacement:

```java
int delta = targetDOW - base.getDayOfWeek();
int positiveDelta = delta + 7;
int adjust = positiveDelta % 7;
if (adjust > 3)
    adjust -= 7;
return SerialDate.addDays(adjust, base);
```

**Error strings → exceptions.** Two remaining tests pass simply by throwing `IllegalArgumentException` instead of returning an error string from `weekInMonthToString` and `relativeToString` (Ch 7's "Use Exceptions Rather Than Return Codes").

*"With these changes all the unit tests pass, and I believe `SerialDate` now works. So now it's time to make it 'right.'"*

## Framework: Then Make It Right

**The safety discipline, stated once and then assumed:** *"Although you won't see this in the discussion, I will be running all of the JCommon unit tests, including my improved unit test for `SerialDate`, after every change I make. So rest assured that every change you see here works for all of JCommon."*

Then a top-to-bottom walk, each finding tagged with its heuristic:

| Finding | Heuristic | Verdict |
|---|---|---|
| Header comments: license, copyright, authors, **change history** | [C1] | Copyrights and licenses must stay for legal reasons; *"the change history is a leftover from the 1960s. We have source code control tools that do this for us now. This history should be deleted."* |
| Long import list | [J1] | Shorten with `java.text.*` and `java.util.*` |
| HTML formatting inside the Javadoc | [G1] | *"This comment has four languages in it: Java, English, Javadoc, and html."* The careful source alignment is lost when Javadoc is generated — and who wants `<ul>` and `<li>` in source? Better: wrap the whole comment in `<pre>` |
| The class name `SerialDate` | [N1] | It is named for an implementation detail — a "serial number" of days since December 30th, 1899. *"The term 'serial number' is not really correct… the representation is more of a relative offset."* A more descriptive term would be **ordinal** |

## Mental Models
- **Work then right, in that order.** Coverage and correctness first; structure second. Refactoring without a verifying suite is guessing.
- **Trust the tools over your reading.** Clover found an always-false `if` that careful reading had missed. Coverage gaps and never-executed branches are *evidence about correctness*, not just hygiene metrics.
- **Read the failure pattern, not just the failure.** Which tests fail — and which you had to comment out — describes the shape of the defect.
- **Bug history is a heat map.** Functions repaired before are the functions to test hardest.
- **A name that encodes the implementation will outlive the implementation.** `SerialDate` documents a 1899 epoch, not a concept.
- **Review is a professional act, not a verdict on a person.** State the terms, credit the author, tag every finding with a reason.

## Key Takeaways
1. Make it work, then make it right — never reverse the order.
2. Before refactoring unfamiliar code, build your own test suite and measure coverage; ~50% coverage means half the class is unverified.
3. Use commented-out tests as an explicit queue of behavior the code *should* have, and say which ones you deliberately left undecided.
4. Never-executed lines and always-false conditions are defects, not dead weight.
5. Boundary conditions are where date/index logic breaks — `>` vs `>=` is a bug class, not a typo.
6. Delete change-history comments; source control owns that job.
7. Don't put HTML in Javadoc; four languages in one comment is three too many.
8. Run the full suite after **every** change, not at the end.
9. *"We've checked the code in a bit cleaner than when we checked it out… The next person to look at this code will hopefully find it easier to deal with than we did. That person will also probably be able to clean it up a bit more than we did."*

## Connects To
- **Ch 1**: the Boy Scout Rule, applied across a whole class.
- **Ch 4**: [C1] change-history and HTML comments — this is that chapter's catalog used in the field.
- **Ch 7**: replacing error-string returns with `IllegalArgumentException`.
- **Ch 9**: F.I.R.S.T. and the argument that tests must be comprehensive to be trustworthy.
- **Ch 14**: same discipline (tiny changes, always-green suite) applied to someone else's code instead of your own.
- **Ch 17**: [G1] Multiple Languages in One Source File, [G2] Obvious Behavior Is Unimplemented, [G3] Incorrect Behavior at the Boundaries, [N1] Choose Descriptive Names, [C1] Inappropriate Information, [J1] Avoid Long Import Lists, [T1]–[T8] the test heuristics.
- **Appendix B**: the full `SerialDate` source and the test listings referenced throughout.
