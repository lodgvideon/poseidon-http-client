# Chapter 23: Presenters and Humble Objects

*Part V: Architecture*

## Core Idea
**The Humble Object pattern** splits behaviors into a hard-to-test *humble* part stripped to its barest essence, and an easy-to-test part containing everything else. **That split usually *is* an architectural boundary.**

## Framework: The Humble Object Pattern

*(Meszaros, ***xUnit Patterns***, Addison-Wesley, 2007, p. 695)*

> *"Originally identified as a way to help unit testers to separate behaviors that are hard to test from behaviors that are easy to test. The idea is very simple: **Split the behaviors into two modules or classes.** One of those modules is **humble**; it contains all the hard-to-test behaviors **stripped down to their barest essence**. The other module contains all the testable behaviors that were stripped out."*

**The canonical case**: *"GUIs are hard to unit test because it is very difficult to write tests that can see the screen and check that the appropriate elements are displayed there. **However, most of the behavior of a GUI is, in fact, easy to test.**"*

## Reference Table: Presenter vs. View

| | **View** (humble) | **Presenter** (testable) |
|---|---|---|
| Testability | Hard to test | Easy to test |
| Job | *"Moves data into the GUI but **does not process** that data"* | *"Accept data from the application and **format it for presentation** so that the View can simply move it to the screen"* |
| Complexity | *"Kept as simple as possible"* | Holds all formatting logic |

**What the Presenter actually does — worked out concretely:**

| Application hands the Presenter… | Presenter puts into the View Model… |
|---|---|
| A `Date` object | *"An appropriate **string**"* |
| A `Currency` object | A string *"with the appropriate **decimal places and currency markers**"* |
| A negative currency value that must show red | *"A simple **boolean flag**… set appropriately"* |
| Every button on screen | Its **name as a string**; plus a boolean flag if it should be **grayed out** |
| Every menu item, radio button, check box, text field | Names loaded *"into appropriate strings and booleans"* |
| Tables of numbers | *"Tables of **properly formatted strings**"* |

> *"**Anything and everything that appears on the screen, and that the application has some kind of control over, is represented in the View Model as a string, or a boolean, or an enum.** Nothing is left for the View to do other than to load the data from the View Model into the screen. **Thus the View is humble.**"*

## Framework: The Pattern Recurs at Every Boundary

> *"It has long been known that **testability is an attribute of good architectures**. The Humble Object pattern is a good example, because **the separation of the behaviors into testable and non-testable parts often defines an architectural boundary**."*

**Database Gateways** *(Fowler,* Patterns of Enterprise Application Architecture*, p. 466)*
- Gateways sit between use case interactors and the database: *"polymorphic interfaces that contain methods for **every create, read, update, or delete operation**."*
- Concrete example: if the application needs the last names of all users who logged in yesterday, `UserGateway` gets a method **`getLastNamesOfUsersWhoLoggedInAfter`** taking a `Date` and returning a list of last names.
- **The humble object is the implementation**, in the database layer: *"It simply uses SQL, or whatever the interface to the database is."*
- **The interactors are not humble** — *"they encapsulate application-specific business rules"* — but *"those interactors are **testable**, because the gateways can be replaced with appropriate stubs and test-doubles."*

**Data Mappers** — and a claim worth quoting in full:
> *"**There is no such thing as an object relational mapper (ORM).** The reason is simple: **Objects are not data structures.** At least, they are not data structures from their users' point of view. The users of an object cannot see the data, since it is all private. Those users see only the public methods… **from the user's point of view, an object is simply a set of operations.**"*

> *"A data structure, in contrast, is a set of public data variables that have no implied behavior. **ORMs would be better named 'data mappers,'** because they load data into data structures from relational database tables."*

Where do they belong? *"**In the database layer of course.** Indeed, ORMs form another kind of Humble Object boundary between the gateway interfaces and the database."*

**Service Listeners** — same shape at the service boundary:
- **Outbound**: *"The application will load data into simple data structures and then pass those structures across the boundary to modules that properly format the data and send it to external services."*
- **Inbound**: *"The service listeners will receive data from the service interface and **format it into a simple data structure** that can be used by the application."*

## Mental Models
- **"Hard to test" is a boundary detector.** When you find code you can't test, don't fight it — split it, shrink the untestable half to a movement of data, and put a line between them.
- **Push formatting outward until the humble side is trivial.** If the View has to decide anything — a date format, a color, whether to gray a button — the Presenter didn't finish its job.
- **An ORM is a data mapper, and data mappers belong outside.** Naming it correctly makes its layer obvious.
- **The un-humble side of every boundary is testable by construction**, because the humble side can be stubbed.

## Key Takeaways
1. Split hard-to-test behavior from easy-to-test behavior; make the hard part humble and nearly empty.
2. The View moves data; the Presenter formats it into strings, booleans, and enums in the View Model.
3. Everything the application controls on screen — including button names and gray-out flags — lives in the View Model.
4. Database gateways are polymorphic CRUD interfaces; their SQL-using implementations are the humble objects.
5. Interactors aren't humble but *are* testable, because gateways can be stubbed.
6. There is no such thing as an ORM — objects aren't data structures. They're data mappers, and they live in the database layer.
7. Service listeners and senders are Humble Objects at the service boundary.
8. *"The use of this pattern at architectural boundaries **vastly increases the testability of the entire system**."*

## Connects To
- **Ch 22**: the ViewModel full of formatted Strings and flags is this pattern in the typical scenario.
- **Ch 6 / Clean Code Ch 6**: "objects are not data structures" is the same dichotomy that defines Data/Object Anti-Symmetry.
- **Ch 28 (The Test Boundary)**: testability as an architectural property, developed.
- **Ch 30 (The Database Is a Detail)**: gateways are what keep SQL out of the use cases.
- **Meszaros**, *xUnit Patterns*; **Fowler**, *Patterns of Enterprise Application Architecture*.
