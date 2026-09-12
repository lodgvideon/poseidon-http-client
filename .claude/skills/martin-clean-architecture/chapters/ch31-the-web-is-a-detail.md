# Chapter 31: The Web Is a Detail

*Part VI: Details*

## Core Idea
**"The GUI is a detail. The web is a GUI. So the web is a detail."** The web is one swing of a pendulum that has been oscillating since the 1960s — *"the web didn't change anything at all."*

## Framework: The Endless Pendulum

> *"Actually the web **didn't change anything**. Or, at least, **it shouldn't have**. The web is just the latest in a series of oscillations that our industry has gone through since the 1960s. These oscillations move back and forth between putting all the computer power in **central servers** and putting all computer power out at the **terminals**."*

**Just within the web era:**

| Swing | Where the power went |
|---|---|
| 1 | *"All the computer power would be in **server farms**, and the browsers would be stupid"* |
| 2 | *"Then we started putting **applets** in the browsers"* |
| 3 | *"But we didn't like that, so we moved **dynamic content back to the servers**"* |
| 4 | *"But then we didn't like that, so we invented **Web 2.0** and moved lots of processing back into the browser with **Ajax and JavaScript**"* — *"whole huge applications written to execute in the browsers"* |
| 5 | *"And now we're all excited about pulling that JavaScript **back into the server with Node**."* **"(Sigh.)"** |

**And before the web**: client-server architecture → central minicomputers with arrays of dumb terminals → mainframes with smart green-screen terminals *"(that were very much analogous to modern-day browsers)"* → computer rooms and punched cards.

> *"**We can't seem to figure out where we want the computer power.** We go back and forth between centralizing it and distributing it. And, I imagine, those oscillations will continue for some time to come."*

> *"As architects, though, we have to look at the long term. **Those oscillations are just short-term issues that we want to push away from the central core of our business rules.**"*

## Worked Example: Company Q and Company A — the marketing genius problem

**Company Q** built a very popular personal finance **desktop app** with a useful GUI. Then came the web.

> *"In its next release, company Q **changed the GUI to look, and behave, like a browser**. I was thunderstruck! **What marketing genius decided that personal finance software, running on a desktop, should have the look and feel of a web browser?**"*

> *"Of course, I hated the new interface. **Apparently everyone else did, too** — because after a few releases, company Q **gradually removed the browser-like feel** and turned its personal finance system back into a regular desktop GUI."*

**The question to sit with:**
> *"Now imagine you were a software architect at Q… **What should you have done *before* this point** to protect your application from that marketing genius?"*

> *"**You should have decoupled your business rules from your UI**… I certainly would have lobbied very hard to isolate the business rules from the GUI, **because you never know what the marketing geniuses will do next**."*

**Company A** — a smartphone maker whose OS upgrade *"completely changed the look and feel of all the applications. Why? **Some marketing genius said so, I suppose.**"*

> *"I do hope the architects at A, and the architects of the apps, keep their UI and business rules isolated from each other, because **there are always marketing geniuses out there just waiting to pounce on the next little bit of coupling you create**."*

## Framework: The Honest Objection, and the Answer

**The objection Martin raises against himself:**
> *"The argument can be made that a GUI, like the web, is **so unique and rich** that it is absurd to pursue a device-independent architecture. When you think about the intricacies of **JavaScript validation or drag-and-drop AJAX calls**, or any of the plethora of other widgets and gadgets you can put on a web page, it's easy to argue that device independence is **impractical**."*

**And he concedes it, partially:**
> *"**To some extent, this is true.** The interaction between the application and the GUI is **'chatty'** in ways that are quite specific to the kind of GUI you have. **The dance between a browser and a web application is different from the dance between a desktop GUI and its application.** Trying to abstract out *that* dance, the way devices are abstracted out of UNIX, **seems unlikely to be possible**."*

**The boundary that *can* be abstracted** — this is the chapter's real technique:

> *"**But another boundary between the UI and the application can be abstracted.** The business logic can be thought of as a **suite of use cases**, each of which performs some function on behalf of a user. Each use case can be described based on the **input data, the processing performed, and the output data**."*

> *"**At some point in the dance** between the UI and the application, **the input data can be said to be complete**, allowing the use case to be executed. Upon completion, the resultant data can be fed back into the dance."*

> *"The complete input data and the resultant output data can be placed into **data structures** and used as the input values and output values for a process that executes the use case. With this approach, **we can consider each use case to be operating the IO device of the UI in a device-independent manner.**"*

**In short**: don't try to abstract the *interaction*; abstract the *transaction*. The chatty, GUI-specific dance stays outside; the moment the input is complete is the boundary.

**The device-independence lineage**: *"Think about it this way: **The WEB is an IO device.** In the 1960s, we learned the value of writing applications that were **device independent**. The motivation for that independence **has not changed. The web is not an exception to that rule.**"*

## Mental Models
- **Treat every "everything has changed" technology as a pendulum swing.** Ask where the compute is moving and note that it will move back.
- **Abstract the transaction, not the dance.** Trying to make the browser conversation device-independent fails; identifying "input is now complete" succeeds.
- **Assume a marketing genius will demand a UI overhaul you consider absurd.** Your protection must be in place *before* the demand arrives.
- **"The web is an IO device" is a decision aid, not a slogan.** It tells you which layer the web belongs in: the outermost.

## Key Takeaways
1. The web changed nothing architecturally; it's one oscillation in a decades-long centralize/distribute cycle.
2. The GUI is a detail, the web is a GUI, therefore the web is a detail — keep it behind a boundary.
3. Decouple business rules from the UI *before* someone demands a UI change you didn't anticipate.
4. The GUI-specific "dance" genuinely resists abstraction — concede that honestly.
5. Abstract at the point where input data is complete: use cases take input data structures and return output data structures.
6. That makes each use case operate the UI as an IO device, device-independently.
7. *"This kind of abstraction is not easy, and it will likely take several iterations to get just right. **But it is possible.**"*

## Connects To
- **Ch 15**: device independence in the 1960s — the same lesson, the same motivation.
- **Ch 17 (Boundaries)**: *"The IO is irrelevant"* and the video-game model argument.
- **Ch 20 (Business Rules)**: use cases defined by input data, processing, and output data — exactly the abstraction used here.
- **Ch 22–23**: controllers, presenters, and view models as the concrete implementation.
- **Ch 30, 32**: the database and frameworks as the sibling details.
