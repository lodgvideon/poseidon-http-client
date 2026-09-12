# Chapter 6: Objects and Data Structures

## Core Idea
Objects hide their data behind abstractions and expose functions that operate on it; data structures expose their data and have no meaningful functions. These are virtual opposites, and choosing the wrong one for the axis of change you expect is a design error — *"the idea that everything is an object is a myth."*

## Frameworks Introduced

- **Data Abstraction** — hiding implementation is not putting a layer of functions between the variables; **it is about abstractions.**
  - The test: can a reader tell *how* the data is stored? If yes, you exposed implementation — even through getters and setters.
  - How: express data in abstract terms that let users manipulate its *essence*. Serious thought is required; *"the worst option is to blithely add getters and setters."*

- **Data/Object Anti-Symmetry** — the chapter's central law, stated in two halves:
  > Procedural code (code using data structures) makes it easy to add new *functions* without changing the existing data structures. OO code makes it easy to add new *classes* without changing existing functions.

  > Procedural code makes it hard to add new *data structures* because all the functions must change. OO code makes it hard to add new *functions* because all the classes must change.

  - **"The things that are hard for OO are easy for procedures, and the things that are hard for procedures are easy for OO."**
  - How to choose: ask which axis will grow. New *types* coming → objects. New *operations* coming → data structures + procedures.

- **The Law of Demeter** — a module should not know about the innards of the objects it manipulates. A method `f` of class `C` should only call methods of:
  - `C` itself
  - an object created by `f`
  - an object passed as an argument to `f`
  - an object held in an instance variable of `C`

  It must **not** invoke methods on objects returned by any of those. **"Talk to friends, not to strangers."**
  - **Critical qualifier**: whether a chain violates Demeter depends on whether the participants are *objects* or *data structures*. Data structures naturally expose their internals, so **Demeter does not apply to them.** Accessor functions are what confuse the issue — nobody would ask about Demeter if the code read `ctxt.options.scratchDir.absolutePath`.

- **Hiding Structure** — the resolution when the participants really are objects: **tell the object to do something; don't ask it about its internals.**

## Reference Table: Object vs. Data Structure

| | Object | Data Structure |
|---|---|---|
| Data | Hidden behind abstractions | Exposed |
| Functions | Operate on hidden data | None meaningful |
| Add a new **type** | Easy — existing functions untouched | Hard — every function must change |
| Add a new **function** | Hard — all classes must change | Easy — data structures untouched |
| Law of Demeter | Applies | Does not apply |
| Typical forms | Domain classes with behavior | DTO, bean, Active Record |

## Code Examples

**Concrete vs. abstract Point:**

```java
// Listing 6-1 — Concrete. Clearly rectangular; forces independent manipulation
// of coordinates. Would still expose implementation with private vars + getters/setters.
public class Point {
    public double x;
    public double y;
}

// Listing 6-2 — Abstract. You cannot tell whether the implementation is rectangular
// or polar. It might be neither. The methods also *enforce an access policy*:
// read coordinates independently, but set them together as an atomic operation.
public interface Point {
    double getX();
    double getY();
    void setCartesian(double x, double y);
    double getR();
    double getTheta();
    void setPolar(double r, double theta);
}
```

**Concrete vs. abstract Vehicle** — the same move on a simpler interface:

```java
// Concrete — you can be pretty sure these are just variable accessors.
public interface Vehicle {
    double getFuelTankCapacityInGallons();
    double getGallonsOfGasoline();
}

// Abstract — no clue at all about the form of the data.
public interface Vehicle {
    double getPercentFuelRemaining();
}
```

## Worked Example: The Shape Problem, Both Ways

**Procedural** — data structures with no behavior, all logic in `Geometry`:

```java
public class Square    { public Point topLeft; public double side; }
public class Rectangle { public Point topLeft; public double height; public double width; }
public class Circle    { public Point center;  public double radius; }

public class Geometry {
    public final double PI = 3.141592653589793;

    public double area(Object shape) throws NoSuchShapeException {
        if (shape instanceof Square) {
            Square s = (Square)shape;
            return s.side * s.side;
        }
        else if (shape instanceof Rectangle) {
            Rectangle r = (Rectangle)shape;
            return r.height * r.width;
        }
        else if (shape instanceof Circle) {
            Circle c = (Circle)shape;
            return PI * c.radius * c.radius;
        }
        throw new NoSuchShapeException();
    }
}
```

**Object-oriented** — polymorphic `area()`, no `Geometry` class needed:

```java
public class Square implements Shape {
    private Point topLeft;
    private double side;
    public double area() { return side*side; }
}
public class Rectangle implements Shape {
    private Point topLeft;
    private double height;
    private double width;
    public double area() { return height * width; }
}
public class Circle implements Shape {
    private Point center;
    private double radius;
    public final double PI = 3.141592653589793;
    public double area() { return PI * radius * radius; }
}
```

**The trade, stated precisely:**

| Change | Procedural version | OO version |
|---|---|---|
| Add `perimeter()` | Shape classes **unaffected**; dependents unaffected | **Every shape must change** |
| Add a new shape | **Every function in `Geometry` must change** | Existing functions **unaffected** |

*"Object-oriented programmers might wrinkle their noses at this and complain that it is procedural — and they'd be right. But the sneer may not be warranted."* Visitor and dual-dispatch work around the OO side, but carry costs and generally return the structure to that of a procedural program.

## Worked Example: Untangling a Train Wreck

```java
// A "train wreck" — coupled cars. Calls getScratchDir() on the return of getOptions(),
// then getAbsolutePath() on the return of getScratchDir(). [G36]
final String outputDir = ctxt.getOptions().getScratchDir().getAbsolutePath();

// Splitting it up is better style — but does NOT resolve the Demeter question.
// The module still knows ctxt has options, which have a scratch dir, which has a path.
Options opts = ctxt.getOptions();
File scratchDir = opts.getScratchDir();
final String outputDir = scratchDir.getAbsolutePath();
```

If they are objects, both forms violate Demeter. The two obvious repairs both fail:

```java
ctxt.getAbsolutePathOfScratchDirectoryOption();   // → explosion of methods on ctxt
ctxt.getScratchDirectoryOption().getAbsolutePath(); // → presumes the option is a data structure
```

*"Neither option feels good."* So ask **why** the path was wanted. Many lines further down:

```java
String outFile = outputDir + "/" + className.replace('.', '/') + ".class";
FileOutputStream fout = new FileOutputStream(outFile);
BufferedOutputStream bos = new BufferedOutputStream(fout);
```

The intent was to create a scratch file of a given name. So **tell `ctxt` to do that**:

```java
BufferedOutputStream bos = ctxt.createScratchFileStream(classFileName);
```

*"That seems like a reasonable thing for an object to do!"* `ctxt` keeps its internals hidden, and the caller no longer navigates objects it shouldn't know about. Note also the admixture of levels in the original — dots, slashes, file extensions, and `File` objects mixed with enclosing code [G34][G6].

## Anti-patterns
- **Hybrids** — half object, half data structure: real behavior *plus* public variables or accessors/mutators that effectively make private variables public. *"They make it hard to add new functions but also make it hard to add new data structures. They are the worst of both worlds."* Indicative of a muddled design whose authors are unsure whether they need protection from functions or types. (Related: **Feature Envy**, from *Refactoring*.)
- **Reflexive getters and setters** — adding them automatically exposes private variables as if they were public, defeating the reason variables were private: keeping the freedom to change type or implementation.
- **Business rules inside an Active Record** — treating a data structure as an object creates a hybrid. **The fix**: keep the Active Record as a data structure and create separate objects holding the business rules, hiding their internal data (probably instances of the Active Record).

## Key Concepts
- **DTO (Data Transfer Object)**: a class with public variables and no functions — the quintessential data structure. Genuinely useful for database communication and socket message parsing; often the first of a series of translation stages.
- **Bean**: private variables with getters and setters. *"The quasi-encapsulation of beans seems to make some OO purists feel better but usually provides no other benefit."*
- **Active Record**: a DTO with navigational methods like `save` and `find`, typically a direct translation of a database table.
- **Train wreck**: a chain of calls that looks like coupled train cars [G36].

## Key Takeaways
1. Objects hide data and expose behavior; data structures do the reverse. Know which one you are writing.
2. Choose by the axis of expected change: new types → objects; new operations → data structures and procedures.
3. Abstraction is not a layer of accessors — if a reader can infer the storage form, you have exposed implementation.
4. Law of Demeter: talk to friends, not strangers — but it applies to objects, not data structures.
5. When a train wreck appears, ask what the caller actually wanted and give the object a method that does it.
6. Never build hybrids; they inherit the costs of both models and the benefits of neither.
7. Keep Active Records as data structures; put business rules in separate objects.
8. Good developers understand this dichotomy *without prejudice* and choose per situation.

## Connects To
- **Ch 3**: burying a `switch` behind a factory is the OO side of this same trade-off.
- **Ch 10**: hidden data and small focused classes are the same discipline seen from the class level.
- **Ch 11**: DTOs at system boundaries; keeping persistence concerns out of business objects.
- **Ch 17**: [G6] Code at Wrong Level of Abstraction, [G34] Functions Should Descend Only One Level of Abstraction, [G36] Avoid Transitive Navigation.
- **Refactoring (Fowler)**: Feature Envy; **GoF**: Visitor / dual dispatch as the escape hatch for the OO side.
