# Chapter 4: Comments

## Core Idea
*"Don't comment bad code — rewrite it."* (Kernighan & Plaugher). Comments are always failures: they compensate for an inability to express intent in code, and because programmers cannot realistically maintain them, they drift into lies.

## Frameworks Introduced

- **Comments Are Failures** — the governing stance.
  - Practice: every time you express intent in code, pat yourself on the back; every time you write a comment, *"grimace and feel the failure of your ability of expression."*
  - Why: **truth can only be found in one place — the code.** Only code can tell you what it does.
  - Consequence: **inaccurate comments are far worse than no comments at all.** They delude, set expectations that will never be fulfilled, and lay down rules that should no longer be followed.

- **Explain Yourself in Code** — the primary substitution technique. In most cases it is simply a matter of creating a function that says what the comment would have said.
  ```java
  // Check to see if the employee is eligible for full benefits
  if ((employee.flags & HOURLY_FLAG) && (employee.age > 65))

  // →
  if (employee.isEligibleForFullBenefits())
  ```

- **Don't Use a Comment When You Can Use a Function or a Variable** — the same move applied to expressions:
  ```java
  // does the module from the global list <mod> depend on the
  // subsystem we are part of?
  if (smodule.getDependSubsystems().contains(subSysMod.getSubSystem()))

  // →
  ArrayList moduleDependees = smodule.getDependSubsystems();
  String ourSubSystem = subSysMod.getSubSystem();
  if (moduleDependees.contains(ourSubSystem))
  ```

- **Comments Do Not Make Up for Bad Code** — the motivation "this is confusing, I'd better comment it" is backwards. *"No! You'd better clean it!"*

## Reference Table: The Only Good Comments

*"The only truly good comment is the comment you found a way not to write."*

| Kind | When justified | Caveat |
|---|---|---|
| **Legal** | Copyright/license headers required by corporate standard | Refer to an external license; don't inline legal tomes. Let the IDE collapse them |
| **Informative** | Explaining a value that can't be named, e.g. what regex a `Pattern` matches | Usually better solved by renaming the function or moving code to a dedicated class |
| **Explanation of Intent** | Documenting *why* a decision was made — `return 1; // we are greater because we are the right type` | You may disagree with the solution, but you know what was attempted |
| **Clarification** | Translating an obscure argument/return of a **standard library or unmodifiable code** | High risk of being wrong — verify carefully before writing |
| **Warning of Consequences** | `// SimpleDateFormat is not thread safe, so we need to create each instance independently` | Prevents an eager programmer "optimizing" into a bug |
| **TODO** | Explaining a degenerate implementation and its future | *Not an excuse to leave bad code in the system.* Scan and eliminate regularly |
| **Amplification** | Marking something that looks inconsequential but isn't — `// the trim is real important` | — |
| **Javadocs in public APIs** | Genuinely valuable — the Java standard library proves it | Javadocs can be just as misleading, nonlocal, and dishonest as any other comment |

## Anti-patterns (the "Bad Comments" catalog)

- **Mumbling** — a comment whose meaning only the author knew. *"Any comment that forces you to look in another module for the meaning of that comment has failed to communicate and is not worth the bits it consumes."*
- **Redundant comments** — the comment takes longer to read than the code and is *less precise* than it. "Like a gladhanding used-car salesman assuring you that you don't need to look under the hood."
- **Misleading comments** — the redundant comment is often also subtly wrong. `// Utility method that returns when this.closed is true` is false: it returns *if* closed, otherwise waits for a blind timeout and throws. A caller who believes the comment ends up debugging why their code is slow.
- **Mandated comments** — a rule that every function needs a javadoc or every variable a comment produces pure clutter and creates the potential for lies.
- **Journal comments** — changelogs at the top of a module. Source control has done this for decades. **Remove them completely.**
- **Noise comments** — `/** Default constructor. */`, `/** The day of the month. */ private int dayOfMonth;`. Our eyes learn to skip them, and then they begin to lie as the code moves.
- **Scary noise** — noisy javadocs that also contain **cut-paste errors** (`/** The version. */ private String info;`). If the author wasn't paying attention writing them, why should the reader profit from reading them?
- **Position markers / banners** — `// Actions //////////////////////`. A banner is startling only if rare; overuse them and they become background noise.
- **Closing brace comments** — `} //while`, `} //try`, `} //main`. If you want to mark closing braces, **shorten your functions instead.**
- **Attributions and bylines** — `/* Added by Rick */`. Source control remembers who and when; bylines stay for years getting less accurate.
- **Commented-out code** — *"Few practices are as odious."* Others won't have the courage to delete it, assuming it matters. **Just delete it. Source control will remember. Promise.**
- **HTML in comments** — an abomination that makes comments unreadable in the editor, the one place they must be readable. Adorning comments with HTML is the *tool's* job, not the programmer's.
- **Nonlocal information** — a comment documenting a default port in a setter that has no control over that default. Nothing guarantees it will be updated when the real default changes.
- **Too much information** — pasting RFC 2045's base64 encoding prose into a base64 test module. The RFC number alone was sufficient.
- **Inobvious connection** — `// plus filter bytes ... and an extra 200 bytes for header info`. What is a filter byte? Does it relate to the `+1` or the `*3`? *"It is a pity when a comment needs its own explanation."*
- **Function headers** — a well-chosen name for a small function that does one thing beats a comment header.
- **Javadocs in nonpublic code** — anathema. The extra formality is cruft and distraction for code not intended for public consumption.

## Worked Example: `GeneratePrimes` → `PrimeGenerator`

Martin wrote this module for the first *XP Immersion* as an example of bad commenting; Kent Beck refactored it live. It was once considered "well documented."

**Before** — the header carries a biography of Eratosthenes; the body is carved into commented *sections* (`// declarations`, `// initialize array to true`, `// sieve`, `// how many primes are there?`) — which is itself the tell from Ch 3 that the function does many things:

```java
/**
 * This class Generates prime numbers up to a user specified maximum.
 * The algorithm used is the Sieve of Eratosthenes.
 * Eratosthenes of Cyrene, b. c. 276 BC, Cyrene, Libya — d. c. 194, Alexandria.
 * The first man to calculate the circumference of the Earth. Also known for
 * working on calendars with leap years and ran the library at Alexandria.
 * ...
 * @author Alphonse
 * @version 13 Feb 2002 atp
 */
public static int[] generatePrimes(int maxValue) {
    if (maxValue >= 2) {           // the only valid case
        // declarations
        int s = maxValue + 1;      // size of array
        boolean[] f = new boolean[s];
        int i;
        // initialize array to true.
        for (i = 0; i < s; i++) f[i] = true;
        // get rid of known non-primes
        f[0] = f[1] = false;
        // sieve
        int j;
        for (i = 2; i < Math.sqrt(s) + 1; i++) {
            if (f[i])              // if i is uncrossed, cross its multiples.
                for (j = 2 * i; j < s; j += i) f[j] = false;  // multiple is not prime
        }
        // ... count, collect, return
    } else                         // maxValue < 2
        return new int[0];         // return null array if bad input.
}
```

**After** — each comment-marked section became a named function; **two** comments remain in the whole module:

```java
/**
 * This class Generates prime numbers up to a user specified maximum.
 * The algorithm used is the Sieve of Eratosthenes.
 * Given an array of integers starting at 2:
 * Find the first uncrossed integer, and cross out all its multiples.
 * Repeat until there are no more multiples in the array.
 */
public class PrimeGenerator {
    private static boolean[] crossedOut;
    private static int[] result;

    public static int[] generatePrimes(int maxValue) {
        if (maxValue < 2)
            return new int[0];
        else {
            uncrossIntegersUpTo(maxValue);
            crossOutMultiples();
            putUncrossedIntegersIntoResult();
            return result;
        }
    }

    private static int determineIterationLimit() {
        // Every multiple in the array has a prime factor that
        // is less than or equal to the root of the array size,
        // so we don't have to cross out multiples of numbers
        // larger than that root.
        double iterationLimit = Math.sqrt(crossedOut.length);
        return (int) iterationLimit;
    }

    private static void crossOutMultiplesOf(int i) {
        for (int multiple = 2*i; multiple < crossedOut.length; multiple += i)
            crossedOut[multiple] = true;
    }
    private static boolean notCrossed(int i) { return crossedOut[i] == false; }
}
```

**Martin's own audit of the two survivors:** the header is arguably redundant with `generatePrimes` itself, but eases the reader into the algorithm — he leaves it. The square-root comment is *almost certainly necessary*: no variable name or structure made the rationale clear. And then he questions the code instead of the comment — is the square-root optimization even worth the time everyone will spend understanding it? *"Using the square root as the iteration limit satisfies the old C and assembly language hacker in me, but I'm not convinced it's worth the time and effort that everyone else will expend."*

That last move is the chapter's real lesson: when a comment is genuinely needed, suspect the code.

## Mental Models
- **Comments rot because code moves.** Chunks bifurcate, reproduce, and recombine; comments can't follow. They become orphaned blurbs of ever-decreasing accuracy — as when instance variables get interposed between `HTTP_DATE_REGEXP` and its explanatory example.
- **A noisy comment is displaced frustration.** The programmer who wrote `//Give me a break!` inside a nested `catch` should have extracted that block into `addExceptionAndCloseResponse(e)`. *"Replace the temptation to create noise with the determination to clean your code."*
- **Discipline argument, answered.** Yes, programmers *should* keep comments accurate — but that energy is better spent making the code so clear it needs no comment.

## Key Takeaways
1. Treat every comment you write as a small failure of expression, and look for the code that would replace it.
2. Never comment bad code — clean it.
3. Inaccurate comments are worse than none; the code is the only source of truth.
4. The good-comment list is short: legal, informative, intent, clarification, warning, TODO, amplification, public-API javadocs.
5. Delete commented-out code, journal comments, bylines, and closing-brace comments outright — source control and small functions replace all of them.
6. Mandated comments and required javadocs manufacture lies at scale.
7. A comment that needs its own explanation, or that describes a distant part of the system, has already failed.
8. When a comment seems genuinely necessary, ask whether the *code* should change instead.

## Connects To
- **Ch 3**: a function divided into commented sections is doing more than one thing; extraction removes both the sections and the comments.
- **Ch 2**: renaming (`responderInstance` → `responderBeingTested`) makes informative comments redundant.
- **Ch 5**: banners, closing-brace comments, and journals are also formatting failures.
- **Ch 7**: `// normal. someone stopped the request.` is a legitimate comment on an intentionally empty catch — Ch 7 argues for the exception design that avoids needing it.
- **Ch 17 (C1–C5)**: Inappropriate Information, Obsolete Comment, Redundant Comment, Poorly Written Comment, Commented-Out Code.
