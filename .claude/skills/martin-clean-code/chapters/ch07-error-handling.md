# Chapter 7: Error Handling

*by Michael Feathers*

## Core Idea
**"Error handling is important, but if it obscures logic, it's wrong."** Robustness and readability are not conflicting goals — they are reconciled by treating error handling as a separate concern you can reason about independently of the main algorithm.

## Frameworks Introduced

- **Use Exceptions Rather Than Return Codes**
  - Why: return codes and error flags **clutter the caller** — the caller must check immediately after the call, and it is easy to forget.
  - Effect: two tangled concerns (the algorithm and the error handling) become separable and independently understandable.

- **Write Your Try-Catch-Finally Statement First**
  - Insight: **exceptions define a scope.** A `try` block is like a transaction — the `catch` must leave the program in a consistent state no matter what happened inside.
  - How: when writing code that could throw, start with the `try-catch-finally`. This defines what the caller should expect before you write the logic.
  - TDD form: write a test that forces the exception, make it pass, then narrow the caught type, then build the rest of the logic *inside* the established scope — pretending nothing goes wrong.

- **Use Unchecked Exceptions** — *"The debate is over."*
  - The price of checked exceptions is an **Open/Closed Principle violation**: a `throws` clause added at a low level cascades signature changes up every level to the `catch`, forcing rebuild and redeploy of modules that care about nothing that changed.
  - Encapsulation breaks: every function in the path of a throw must know about a low-level exception's details. *"Given that the purpose of exceptions is to allow you to handle errors at a distance, it is a shame that checked exceptions break encapsulation in this way."*
  - Evidence: C#, C++, Python, and Ruby have no checked exceptions, and robust software is written in all of them.
  - **Exception to the exception**: they can be useful when writing a *critical library* where the caller must catch. In general application development, the dependency costs outweigh the benefits.

- **Provide Context with Exceptions** — a stack trace tells you where, never **the intent of the operation that failed**. Create informative messages naming the operation and the type of failure; pass enough to log from the `catch`.

- **Define Exception Classes in Terms of a Caller's Needs** — errors can be classified by source or by type, but *"our most important concern should be how they are caught."*
  - Observation: in most situations the work is standard regardless of cause — record the error, make sure you can proceed. So one exception class per area of code is often right; the information carried distinguishes the cases.
  - **Use different classes only if there are times when you want to catch one and let the other pass through.**

- **The Special Case Pattern** [Fowler] — create a class or configure an object that handles the special case, so client code never deals with exceptional behavior. The behavior is encapsulated in the special case object.

- **Don't Return Null** — returning null creates work for yourself and foists problems on callers; one missing check sends the application spinning out of control. Prefer throwing an exception or returning a Special Case object. When a third-party API returns null, **wrap it**.

- **Don't Pass Null** — *"Returning null from methods is bad, but passing null into methods is worse."* In most languages there is no good way to handle a null passed accidentally, so **the rational approach is to forbid passing null by default.** Then a null in an argument list is itself the indication of a problem.

## Code Examples

**Return codes → exceptions:**

```java
// Before — the algorithm is buried in checks; nesting encodes error handling.
public void sendShutDown() {
    DeviceHandle handle = getHandle(DEV1);
    // Check the state of the device
    if (handle != DeviceHandle.INVALID) {
        retrieveDeviceRecord(handle);
        // If not suspended, shut down
        if (record.getStatus() != DEVICE_SUSPENDED) {
            pauseDevice(handle);
            clearDeviceWorkQueue(handle);
            closeDevice(handle);
        } else {
            logger.log("Device suspended.  Unable to shut down");
        }
    } else {
        logger.log("Invalid handle for: " + DEV1.toString());
    }
}

// After — shutdown algorithm and error handling are two separate, readable concerns.
public void sendShutDown() {
    try {
        tryToShutDown();
    } catch (DeviceShutDownError e) {
        logger.log(e);
    }
}

private void tryToShutDown() throws DeviceShutDownError {
    DeviceHandle handle = getHandle(DEV1);
    DeviceRecord record = retrieveDeviceRecord(handle);
    pauseDevice(handle);
    clearDeviceWorkQueue(handle);
    closeDevice(handle);
}
```

**Wrapping a third-party API to collapse duplicated handling:**

```java
// Before — three catch blocks doing essentially the same work.
ACMEPort port = new ACMEPort(12);
try {
    port.open();
} catch (DeviceResponseException e) {
    reportPortError(e); logger.log("Device response exception", e);
} catch (ATM1212UnlockedException e) {
    reportPortError(e); logger.log("Unlock exception", e);
} catch (GMXError e) {
    reportPortError(e); logger.log("Device response exception");
} finally { … }

// After — one exception type, one handler.
LocalPort port = new LocalPort(12);
try {
    port.open();
} catch (PortDeviceFailure e) {
    reportError(e);
    logger.log(e.getMessage(), e);
} finally { … }

// The wrapper: catches and translates.
public class LocalPort {
    private ACMEPort innerPort;
    public LocalPort(int portNumber) { innerPort = new ACMEPort(portNumber); }

    public void open() {
        try {
            innerPort.open();
        } catch (DeviceResponseException e)  { throw new PortDeviceFailure(e); }
          catch (ATM1212UnlockedException e) { throw new PortDeviceFailure(e); }
          catch (GMXError e)                 { throw new PortDeviceFailure(e); }
    }
}
```
**Why wrapping third-party APIs is a best practice:** it minimizes dependency (you can switch libraries later), makes third-party calls easy to mock in tests, and frees you from a vendor's API design choices.

## Worked Example: Special Case Pattern

```java
// Awkward — the exception clutters the business logic, and the "exceptional" branch
// is actually a normal business rule: no meal expenses means a per diem.
try {
    MealExpenses expenses = expenseReportDAO.getMeals(employee.getID());
    m_total += expenses.getTotal();
} catch(MealExpensesNotFound e) {
    m_total += getMealPerDiem();
}

// Clean — the DAO always returns a MealExpenses. There is no special case at the call site.
MealExpenses expenses = expenseReportDAO.getMeals(employee.getID());
m_total += expenses.getTotal();

// The special case object encapsulates the rule.
public class PerDiemMealExpenses implements MealExpenses {
    public int getTotal() {
        // return the per diem default
    }
}
```

**The same move applied to null returns:**

```java
// Before
List<Employee> employees = getEmployees();
if (employees != null) {
    for (Employee e : employees) { totalPay += e.getPay(); }
}

// After — getEmployees() returns an empty list instead of null.
List<Employee> employees = getEmployees();
for (Employee e : employees) { totalPay += e.getPay(); }

public List<Employee> getEmployees() {
    if ( .. there are no employees .. )
        return Collections.emptyList();   // predefined immutable list
}
```

## Anti-patterns
- **Null-check thickets** — code where "nearly every other line was a check for null." The diagnosis is counter-intuitive: *"It's easy to say that the problem with the code above is that it is missing a null check, but in actuality, the problem is that it has too many."* And such code is still unsafe — a missed check on `persistentStore` yields a `NullPointerException` from the depths of the application, which nobody can meaningfully respond to at the top level.
- **Guarding against passed nulls** — the two obvious defenses both fail:
  - Throwing `InvalidArgumentException` requires a handler, and *"is there any good course of action?"*
  - `assert p1 != null : "p1 should not be null";` is good documentation, *"but it doesn't solve the problem"* — a null still yields a runtime error.

  The only real fix is the policy: don't pass null.
- **Error handling that dominates a codebase** — not that it is all the code does, but that *"it is nearly impossible to see what the code does because of all of the scattered error handling."*

## Key Takeaways
1. Throw exceptions instead of returning error codes — the caller's logic stays visible.
2. Write the `try-catch-finally` first; treat the `try` as a transaction scope and let TDD fill it in.
3. Prefer unchecked exceptions in application code; checked exceptions violate OCP and break encapsulation.
4. Give every exception enough context to identify the failed operation and its intent — a stack trace is not enough.
5. Define exception classes by how they will be **caught**, not by their source or type; one class per area is usually enough.
6. Wrap third-party APIs at the boundary and translate their exceptions into your own.
7. Use the Special Case Pattern to keep "exceptional" business rules out of the client's control flow.
8. Don't return null; don't pass null. Make null in an argument list a defect by policy.

## Connects To
- **Ch 3**: "Error Handling Is One Thing" and Extract Try/Catch Blocks — this chapter is the full development of that rule.
- **Ch 8**: wrapping third-party APIs is the shared move; Ch 8 generalizes it to all boundaries.
- **Ch 9**: learning tests and exception-forcing tests are the same technique applied to different problems.
- **Ch 17**: [G33] Encapsulate Boundary Conditions.
- **Refactoring (Fowler)**: Special Case Pattern; **PPP (Martin)**: Open/Closed Principle.
