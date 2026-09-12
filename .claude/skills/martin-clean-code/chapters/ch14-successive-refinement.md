# Chapter 14: Successive Refinement

*Case Study of a Command-Line Argument Parser*

## Core Idea
**"To write clean code, you must first write dirty code and then clean it."** This chapter shows a module that started well, did not scale, and was then refactored — and the discipline that made the refactoring safe.

## Frameworks Introduced

- **Successive Refinement** — the writing analogy, taken literally.
  - *"We learned this truth in grade school when our teachers tried (usually in vain) to get us to write rough drafts of our compositions… Writing clean compositions, they tried to tell us, is a matter of successive refinement."*
  - The failure mode: freshman programmers believe the primary goal is to get the program working, then move on, *"leaving the 'working' program in whatever state they finally got it to 'work.'"* **"Most seasoned programmers know that this is professional suicide."**

- **Stop When You See the Cliff** — the pivotal judgment call of the chapter.
  - The situation: two argument types (String, integer) had already been added, two more remained. *"If I bulldozed my way forward, I could probably get them to work, but I'd leave behind a mess that was too large to fix."*
  - **"If the structure of this code was ever going to be maintainable, now was the time to fix it. So I stopped adding features and started refactoring."**

- **The Rule of Three Places** — the diagnostic that revealed the missing abstraction. Each new argument type required new code in exactly three places:
  1. Parsing its schema element to select the right `HashMap`
  2. Parsing it in the command-line strings and converting to its true type
  3. A `getXXX` method returning it as its true type

  *"Many different types, all with similar methods — that sounds like a class to me. And so the `ArgumentMarshaler` concept was born."*

- **On Incrementalism** — *"One of the best ways to ruin a program is to make massive changes to its structure in the name of improvement. Some programs never recover from such 'improvements.'"*
  - The discipline: **TDD's central doctrine is to keep the system running at all times.** *"I am not allowed to make a change to the system that breaks that system. Every change I make must keep the system working as it worked before."*
  - The safety net built *while making the mess*: unit tests in JUnit plus acceptance tests as FitNesse wiki pages.
  - The method: **a large number of very tiny changes**, each moving the structure toward `ArgumentMarshaler`, each keeping the system working.

## Worked Example: The Destination

```java
// Args.java (final) — read top to bottom without jumping around.
public class Args {
    private Map<Character, ArgumentMarshaler> marshalers;
    private Set<Character> argsFound;
    private ListIterator<String> currentArgument;

    public Args(String schema, String[] args) throws ArgsException {
        marshalers = new HashMap<Character, ArgumentMarshaler>();
        argsFound = new HashSet<Character>();
        parseSchema(schema);
        parseArgumentStrings(Arrays.asList(args));
    }

    private void parseSchemaElement(String element) throws ArgsException {
        char elementId = element.charAt(0);
        String elementTail = element.substring(1);
        validateSchemaElementId(elementId);
        if (elementTail.length() == 0)
            marshalers.put(elementId, new BooleanArgumentMarshaler());
        else if (elementTail.equals("*"))
            marshalers.put(elementId, new StringArgumentMarshaler());
        else if (elementTail.equals("#"))
            marshalers.put(elementId, new IntegerArgumentMarshaler());
        else if (elementTail.equals("##"))
            marshalers.put(elementId, new DoubleArgumentMarshaler());
        else if (elementTail.equals("[*]"))
            marshalers.put(elementId, new StringArrayArgumentMarshaler());
        else
            throw new ArgsException(INVALID_ARGUMENT_FORMAT, elementId, elementTail);
    }

    public boolean getBoolean(char arg) {
        return BooleanArgumentMarshaler.getValue(marshalers.get(arg));
    }
    public int getInt(char arg) {
        return IntegerArgumentMarshaler.getValue(marshalers.get(arg));
    }
}

// The abstraction that made it possible — one method.
public interface ArgumentMarshaler {
    void set(Iterator<String> currentArgument) throws ArgsException;
}

public class StringArgumentMarshaler implements ArgumentMarshaler {
    private String stringValue = "";

    public void set(Iterator<String> currentArgument) throws ArgsException {
        try {
            stringValue = currentArgument.next();
        } catch (NoSuchElementException e) {
            throw new ArgsException(MISSING_STRING);
        }
    }

    public static String getValue(ArgumentMarshaler am) {
        if (am != null && am instanceof StringArgumentMarshaler)
            return ((StringArgumentMarshaler) am).stringValue;
        else
            return "";
    }
}
```

Usage stays trivial — construct with a schema string, then query:

```java
Args arg = new Args("l,p#,d*", args);
boolean logging  = arg.getBoolean('l');
int port         = arg.getInt('p');
String directory = arg.getString('d');
```

## Worked Example: The Rough Draft, and How It Got That Way

**The Boolean-only version** — *"it's really not that bad. It's compact and simple and easy to understand."* But the seeds are visible: `schema`, `args`, `valid`, `unexpectedArguments`, `booleanArgs`, `numberOfArguments` as fields, with `parse`/`set`/`get` triplets per type.

**Adding just two more types (String, integer) produced this:**

```java
public class Args {
    private String schema;
    private String[] args;
    private boolean valid = true;
    private Set<Character> unexpectedArguments = new TreeSet<Character>();
    private Map<Character, Boolean> booleanArgs = new HashMap<Character, Boolean>();
    private Map<Character, String>  stringArgs  = new HashMap<Character, String>();
    private Map<Character, Integer> intArgs     = new HashMap<Character, Integer>();
    private Set<Character> argsFound = new HashSet<Character>();
    private int currentArgument;
    private char errorArgumentId = '\0';
    private String errorParameter = "TILT";
    private ErrorCode errorCode = ErrorCode.OK;

    private void setIntArg(char argChar) throws ArgsException {
        currentArgument++;
        String parameter = null;
        try {
            parameter = args[currentArgument];
            intArgs.put(argChar, new Integer(parameter));
        } catch (ArrayIndexOutOfBoundsException e) {
            valid = false; errorArgumentId = argChar;
            errorCode = ErrorCode.MISSING_INTEGER;
            throw new ArgsException();
        } catch (NumberFormatException e) {
            valid = false; errorArgumentId = argChar; errorParameter = parameter;
            errorCode = ErrorCode.INVALID_INTEGER;
            throw new ArgsException();
        }
    }
}
```

Martin's own verdict: *"Actually 'rough draft' is probably the kindest thing you can say about this code… The sheer number of instance variables is daunting. The odd strings like 'TILT,' the `HashSet`s and `TreeSet`s, and the try-catch-catch blocks all add up to a festering pile."*

**The honest note**: *"I had not wanted to write a festering pile. Indeed, I was trying to keep things reasonably well organized… But, clearly, I had let the problem get away from me."*

**And the warning to the reader**: *"I hope your initial reaction to this mass of code is 'I'm certainly glad he didn't leave it like that!' If you feel like this, then remember that's how other people are going to feel about code that you leave in rough-draft form."*

## Worked Example: Tiny Steps, System Always Working

**Step 1 — add the skeleton where it breaks nothing:**

```java
private class ArgumentMarshaler {
    private boolean booleanValue = false;
    public void setBoolean(boolean value) { booleanValue = value; }
    public boolean getBoolean() { return booleanValue; }
}
private class BooleanArgumentMarshaler extends ArgumentMarshaler { }
private class StringArgumentMarshaler  extends ArgumentMarshaler { }
private class IntegerArgumentMarshaler extends ArgumentMarshaler { }
```

**Step 2 — change one map's value type, fix the compile errors it causes** (in exactly the parse/set/get places identified earlier).

**Step 3 — a test fails.** `booleanArgs.get('y')` returns null for an absent argument; the change made the old `falseIfNull` guard irrelevant, so a `NullPointerException` appears. *"Incrementalism demanded that I get this working quickly before making any other changes."*

The repair, itself in three micro-steps:

```java
// (a) Remove the now-useless falseIfNull. Tests still fail the same way —
//     confirming no NEW errors were introduced.
public boolean getBoolean(char arg) {
    return booleanArgs.get(arg).getBoolean();
}

// (b) Extract the marshaler into a variable. The long name was "badly redundant
//     and cluttered up the function," so it is shortened to `am` [N5].
public boolean getBoolean(char arg) {
    Args.ArgumentMarshaler am = booleanArgs.get(arg);
    return am.getBoolean();
}

// (c) Add the null detection.
public boolean getBoolean(char arg) {
    Args.ArgumentMarshaler am = booleanArgs.get(arg);
    return am != null && am.getBoolean();
}
```

Note the technique in (a): **removing a suspected-dead guard and confirming the failure is unchanged** proves the guard was irrelevant, not that the fix worked.

## Key Concepts
- **Rough draft**: working code that has not yet been refined. Legitimate as a *stage*, professional suicide as a *destination*.
- **Keep the system running at all times**: the TDD doctrine that makes large restructurings survivable.
- **Automated tests as the license to refactor**: unit tests (JUnit) plus acceptance tests (FitNesse), runnable on a whim.

## Key Takeaways
1. Write it dirty, then clean it — nobody produces clean and elegant code in one pass.
2. Recognize the moment before the mess becomes unfixable, and **stop adding features then**.
3. When each new variant requires edits in the same N places, those N places are an abstraction waiting to be extracted.
4. Never make a massive structural change in one move; make many tiny ones, each keeping the system working.
5. Build the test suite *while* making the mess — it is what makes the cleanup possible later.
6. Fix each break immediately before proceeding; a failing test is a stop-the-line event.
7. *"Nothing has a more profound and long-term degrading effect upon a development project than bad code."* Bad schedules can be redone, bad requirements redefined, bad team dynamics repaired — **bad code rots and ferments.**
8. Cleanup cost grows superlinearly: *"If you made a mess in a module in the morning, it is easy to clean it up in the afternoon. Better yet, if you made a mess five minutes ago, it's very easy to clean it up right now."*

## Connects To
- **Ch 3**: "How Do You Write Functions Like This?" — the same confession, stated as a rule.
- **Ch 9**: the Three Laws of TDD are what "keep the system running at all times" means operationally.
- **Ch 10**: `ArgumentMarshaler` is SRP + OCP discovered through refactoring pressure, not designed up front.
- **Ch 1**: LeBlanc's Law — this chapter is what "later equals never" looks like when *avoided*.
- **Ch 15–16**: the same discipline applied to someone else's code.
- **Ch 17**: [N5] name length should match scope size.
