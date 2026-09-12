# Chapter 5: Object-Oriented Programming

*Part II: Starting with the Bricks: Programming Paradigms*

## Core Idea
**"OO is the ability, through the use of polymorphism, to gain absolute control over every source code dependency in the system."** Not encapsulation, not inheritance, not "modeling the real world" — the architect's definition is dependency inversion made safe and convenient.

## Framework: Demolishing the Three Magic Words

Martin scores the standard definition — encapsulation, inheritance, polymorphism — item by item.

**First, two answers dismissed outright:**
- *"The combination of data and function"* — *"a very unsatisfying answer because it implies that `o.f()` is somehow different from `f(o)`. This is absurd. Programmers were passing data structures into functions long before 1966."*
- *"A way to model the real world"* — *"an evasive answer at best… It does not tell us what OO is."*

### Encapsulation? — **no points**

C had **perfect** encapsulation:

```c
/* point.h */
struct Point;
struct Point* makePoint(double x, double y);
double distance(struct Point *p1, struct Point *p2);

/* point.c */
#include "point.h"
#include <stdlib.h>
#include <math.h>

struct Point {
    double x, y;
};

struct Point* makepoint(double x, double y) {
    struct Point* p = malloc(sizeof(struct Point));
    p->x = x;
    p->y = y;
    return p;
}

double distance(struct Point* p1, struct Point* p2) {
    double dx = p1->x - p2->x;
    double dy = p1->y - p2->y;
    return sqrt(dx*dx + dy*dy);
}
```
*"The users of `point.h` have no access whatsoever to the members of `struct Point`… absolutely no knowledge of the implementation."*

**Then C++ broke it** — the compiler needs each class's instance size, so member variables must appear in the header:

```cpp
// point.h
class Point {
public:
    Point(double x, double y);
    double distance(const Point& p) const;
private:
    double x;   // ← the client now knows these exist
    double y;
};
```
*"The compiler will prevent access to them, but the client still knows they exist… if those member names are changed, the `point.cc` file must be recompiled! Encapsulation has been broken."*

`public`/`private`/`protected` are described as **"a hack necessitated by the technical need for the compiler to see those variables in the header file."** Java and C# *"abolished the header/implementation split altogether, thereby weakening encapsulation even more."* Many OO languages (Smalltalk, Python, JavaScript, Lua, Ruby) have *"little or no enforced encapsulation."*

### Inheritance? — **half a point**

C programmers faked it by field-ordering:

```c
/* namedPoint.h */
struct NamedPoint;
struct NamedPoint* makeNamedPoint(double x, double y, char* name);
void setName(struct NamedPoint* np, char* name);
char* getName(struct NamedPoint* np);

/* namedPoint.c */
struct NamedPoint {
    double x, y;     /* same order as struct Point */
    char* name;
};

/* main.c */
int main(int ac, char** av) {
    struct NamedPoint* origin     = makeNamedPoint(0.0, 0.0, "origin");
    struct NamedPoint* upperRight = makeNamedPoint(1.0, 1.0, "upperRight");
    printf("distance=%f\n",
        distance((struct Point*) origin, (struct Point*) upperRight));
}
```
*"`NamedPoint` can masquerade as `Point` because `NamedPoint` is a pure superset of `Point` and maintains the ordering of the members."* **This is how C++ implements single inheritance.** But the cast is explicit (a real OO language upcasts implicitly), and multiple inheritance is *"considerably more difficult to achieve by such trickery."* Verdict: OO made masquerading *"significantly more convenient"* — half a point.

### Polymorphism? — **nothing new, but made safe**

```c
#include <stdio.h>
void copy() {
    int c;
    while ((c = getchar()) != EOF)
        putchar(c);
}
```

`getchar()` reads from `STDIN` — but *which device*? UNIX requires every IO driver to supply five functions with identical signatures: `open`, `close`, `read`, `write`, `seek`.

```c
struct FILE {
    void (*open)(char* name, int mode);
    void (*close)();
    int  (*read)();
    void (*write)(char);
    void (*seek)(long index, int mode);
};

/* the console driver loads its versions into a FILE */
struct FILE console = {open, close, read, write, seek};

extern struct FILE* STDIN;
int getchar() {
    return STDIN->read();
}
```

> *"This simple trick is the basis for all polymorphism in OO. In C++, every virtual function within a class has a pointer in a table called a **vtable**, and all calls to virtual functions go through that table."*

**So what did OO add?** Safety. *"Pointers to functions are **dangerous**… driven by a set of manual conventions. You have to remember to follow the convention to initialize those pointers… to call all your functions through those pointers. If any programmer fails to remember these conventions, the resulting bug can be devilishly hard to track down."* OO eliminates the conventions and therefore the danger — **"OO imposes discipline on indirect transfer of control."**

## Framework: The Power of Polymorphism — Plugin Architecture

What must change in `copy()` to support a handwriting-recognition input and a speech-synthesizer output?

> **"We don't need any changes at all! Indeed, we don't even need to recompile the copy program."** Because *"the source code of the copy program does not depend on the source code of the IO drivers."*

**The IO devices have become plugins.** The historical driver: in the late 1950s programs were device-*dependent* — written for decks of punched cards, then customers switched to reels of magnetic tape, forcing large rewrites. *"The plugin architecture was invented to support this kind of IO device independence, and has been implemented in almost every operating system since."*

But most programmers never extended it to their own programs, *"because using pointers to functions was dangerous."*

> **"OO allows the plugin architecture to be used anywhere, for anything."**

## Worked Example: Dependency Inversion

**Before polymorphism** (Figure 5.1): `main` calls high-level functions, which call mid-level, which call low-level. **Source code dependencies inexorably follow the flow of control** — every caller must name the module containing the callee (`#include` in C, `import` in Java, `using` in C#).

*"This requirement presented the software architect with few, if any, options. The flow of control was dictated by the behavior of the system, and the source code dependencies were dictated by that flow of control."*

**With polymorphism** (Figure 5.2): `HL1` calls `F()` in `ML1` **through an interface `I`**. *"The fact that it calls this function through an interface is a source code contrivance. At runtime, the interface doesn't exist. `HL1` simply calls `F()` within `ML1`."*

But the **source code dependency** — `ML1`'s inheritance relationship to `I` — **points opposite to the flow of control.** That is **dependency inversion**.

> *"The fact that OO languages provide safe and convenient polymorphism means that **any source code dependency, no matter where it is, can be inverted**."*

> *"Software architects working in systems written in OO languages have **absolute control** over the direction of all source code dependencies in the system… **That is power! That is the power that OO provides.**"*

**The concrete payoff** (Figure 5.3) — invert so that the **database and UI depend on the business rules**:

| Consequence | What it means |
|---|---|
| **UI and database become plugins** | Business rules' source code never mentions either |
| **Three separate deployment units** | jar files, DLLs, Gem files, with the same dependencies as the source |
| **Independent deployability** | Change the UI or DB → business rules unaffected, no redeployment |
| **Independent developability** | Independently deployable modules can be built by different teams |

## Anti-patterns
- **Defining OO by encapsulation** — C had it perfectly; OO languages weakened it.
- **Defining OO by "modeling the real world"** — evasive and unfalsifiable.
- **Letting source dependencies follow the call graph** — this is the pre-OO default, and it hands architectural control to the runtime behavior.

## Key Takeaways
1. To an architect, **OO = absolute control over the direction of every source code dependency.**
2. Encapsulation earns OO no points, inheritance half a point; polymorphism is the whole value — and only because it is *safe*.
3. Polymorphism is function pointers made conventional-free; a vtable is a `struct FILE` of function pointers.
4. Any dependency can be inverted by inserting an interface — the architect chooses the direction regardless of who calls whom.
5. Invert so that low-level details (UI, database) depend on high-level policy, never the reverse.
6. That inversion buys the **plugin architecture**, and with it independent deployability and independent developability.

## Connects To
- **Ch 11 (DIP)**: this chapter's mechanism, stated as a formal principle.
- **Ch 12–14**: independent deployability is what components and the component principles are built on.
- **Ch 17–22**: crossing boundaries is polymorphism plus dependency inversion, applied structurally.
- **Ch 30–32**: database, web, and frameworks as plugins — the payoff of Figure 5.3.
- **Dahl & Nygaard, 1966**: moving the stack frame to the heap.
