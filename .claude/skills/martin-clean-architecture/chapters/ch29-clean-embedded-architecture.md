# Chapter 29: Clean Embedded Architecture

*by James Grenning — Part V: Architecture*

## Core Idea
> *"Although software does not wear out, **it can be destroyed from within by unmanaged dependencies on firmware and hardware**."*

**Firmware is not defined by where it lives.** It is defined by **what it depends on** — and most embedded code becomes firmware by accident, forfeiting a long useful life.

## Framework: Redefining Firmware

Doug Schmidt's claim, from *"The Growing Importance of Sustaining Software for the DoD"*:
> *"Although software does not wear out, **firmware and hardware become obsolete**, thereby requiring software modifications."*

**The accepted definitions — all judged wrong or obsolete:**

| Source | Definition |
|---|---|
| Wikipedia | *"Held in non-volatile memory devices such as ROM, EPROM, or flash memory"* |
| techterms.com | *"A software program or set of instructions programmed on a hardware device"* |
| lifewire.com | *"Software that is embedded in a piece of hardware"* |
| webopedia.com | *"Software (programs or data) that has been written onto read-only memory (ROM)"* |

> **"Firmware does not mean code lives in ROM. It's not firmware because of *where it is stored*; rather, it is firmware because of *what it depends on* and *how hard it is to change* as hardware evolves."**

**And the accusation is not limited to embedded engineers:**
> *"**Non-embedded engineers also write firmware!** You non-embedded developers essentially write firmware whenever you **bury SQL in your code** or when you **spread platform dependencies throughout your code**. **Android app developers write firmware** when they don't separate their business logic from the Android API."*

> *"What we really need is **less firmware and more software**. Actually, I am disappointed that firmware engineers write so much firmware!"*

## Worked Example: Two Ways Software Becomes Firmware

**1. The TDM → VOIP redesign (late 1990s).** Whenever the team asked the systems engineer how a call should behave in a given situation, *"he would disappear and a little later emerge with a very detailed answer. 'Where did he get that answer?' we asked. **'From the current product's code,'** he'd answer."*

> *"**The tangled legacy code was the spec for the new product!** The existing implementation had no separation between TDM and the business logic of making calls. The whole product was hardware/technology dependent from top to bottom and could not be untangled. **The whole product had essentially become firmware.**"*

**2. The message dispatcher.** A message processor knows the message format, parses it, and dispatches to a handler — *"None of this is surprising, **except that the message processor/dispatcher resides in the same file as code that interacts with a UART hardware**. The message processor is polluted with UART details."*

> *"The message processor **could have been software** with a potentially long useful life, **but instead it is firmware**. The message processor is denied the opportunity to become software — and that is just not right!"*

## Framework: The App-titude Test

**Kent Beck's three activities**, with Grenning's commentary in italics:
1. **"First make it work."** *You are out of business if it doesn't work.*
2. **"Then make it right."** *Refactor so that you and others can understand it and evolve it as needs change or are better understood.*
3. **"Then make it fast."** *Refactor for "needed" performance.*

> *"Much of the embedded systems software that I see in the wild seems to have been written with **'Make it work'** in mind — and perhaps also with an **obsession for 'Make it fast,'** achieved by adding micro-optimizations at every opportunity."*

Fred Brooks in *The Mythical Man-Month* says *"plan to throw one away"* — *"Kent and Fred are giving virtually the same advice: **Learn what works, then make a better solution.**"*

> *"Getting an app to work is what I call the **App-titude test** for a programmer. Programmers, **embedded or not**, who just concern themselves with getting their app to work are doing their products and employers a disservice."*

## Worked Example: A File That Passed the App-titude Test

The functions, **in the order found in one source file**:

```c
ISR(TIMER1_vect)                    { ... }
ISR(INT2_vect)                      { ... }
void  btn_Handler(void)             { ... }
float calc_RPM(void)                { ... }
static char Read_RawData(void)      { ... }
void  Do_Average(void)              { ... }
void  Get_Next_Measurement(void)    { ... }
void  Zero_Sensor_1(void)           { ... }
void  Zero_Sensor_2(void)           { ... }
void  Dev_Control(char Activation)  { ... }
char  Load_FLASH_Setup(void)        { ... }
void  Save_FLASH_Setup(void)        { ... }
void  Store_DataSet(void)           { ... }
float bytes2float(char bytes[4])    { ... }
void  Recall_DataSet(void)          { ... }
void  Sensor_init(void)             { ... }
void  uC_Sleep(void)                { ... }
```

Regrouped by concern, the mixture becomes visible:

| Concern | Functions |
|---|---|
| **Domain logic** | `calc_RPM`, `Do_Average`, `Get_Next_Measurement`, `Zero_Sensor_1`, `Zero_Sensor_2` |
| **Hardware platform setup** | `ISR(TIMER1_vect)`, `ISR(INT2_vect)`, `uC_Sleep` |
| **Button press reaction** | `btn_Handler`, `Dev_Control` |
| **A/D input from hardware** | `Read_RawData` |
| **Persistent storage** | `Load_FLASH_Setup`, `Save_FLASH_Setup`, `Store_DataSet`, `bytes2float`, `Recall_DataSet` |
| **Doesn't do what its name implies** | `Sensor_init` |

> *"I also found a file structure that implied that **the only way to test any of this code is in the embedded target**. Virtually every bit of this code **knows it is in a special microprocessor architecture**, using 'extended' C constructs that tie the code to a particular tool chain and microprocessor. **There is no way for this code to have a long useful life** unless the product never needs to be moved to a different hardware environment."*

> *"This application works: **the engineer passed the App-titude test. But the application can't be said to have a clean embedded architecture.**"*

## Framework: The Target-Hardware Bottleneck

**What is genuinely special about embedded**: limited memory, real-time constraints and deadlines, limited IO, unconventional user interfaces, sensors and connections to the real world. *"Most of the time the hardware is **concurrently developed** with the software… you may have **no place to run the code**. If that's not bad enough, once you get the hardware, it is likely that **the hardware will have its own defects**."*

> *"Yes, embedded is special. Embedded engineers are special. **But embedded development is not so special that the principles in this book are not applicable.**"*

> *"**The target-hardware bottleneck**: when embedded code is structured without applying clean architecture principles, you will often face the scenario in which **you can test your code only on the target**. If the target is the only place where testing is possible, the target-hardware bottleneck will slow you down."*

## Reference Table: The Layers and Their Abstraction Boundaries

| Layer | Boundary below it | Purpose |
|---|---|---|
| **Software** | **OSAL** (OS Abstraction Layer) | Isolates software from the operating system |
| **Operating System** | — | *"A layer separating the software from firmware"* |
| **Firmware** | **HAL** (Hardware Abstraction Layer) | *"The name of the boundary between the software and the firmware"* |
| **Hardware** | — | *"Due to technology advances and Moore's law, the hardware **will** change"* |

**On the software/firmware line specifically**: *"The line between software and firmware is typically **not so well defined** as the line between code and hardware. **One of your jobs as an embedded software developer is to firm up that line.**"*

**Software and firmware intermingling is an anti-pattern:**
> *"Code exhibiting this anti-pattern will **resist changes**. In addition, changes will be **dangerous**, often leading to unintended consequences. **Full regression tests of the whole system will be needed for minor changes.** If you have not created externally instrumented tests, **expect to get bored with manual tests — and then you can expect new bug reports**."*

## Framework: The HAL Serves the Software, Not the Hardware

> *"**The HAL exists for the software that sits on top of it, and its API should be tailored to that software's needs.**"*

**Example 1 — persistence.** *"The firmware can store **bytes and arrays of bytes into flash memory**. In contrast, the application needs to store and read **name/value pairs**… The software should not be concerned that the name/value pairs are stored in flash memory, a spinning disk, the cloud, or core memory. **The flash implementation is a detail that should be hidden.**"*

**Example 2 — an LED, and raising the abstraction level.** An LED is tied to a GPIO bit.
- Firmware/BSP level: `Led_TurnOn(5)` — *"a pretty low-level hardware abstraction layer"*
- **Product level**: what is the LED *indicating*? Low battery power. So the HAL provides **`Indicate_LowBattery()`**

> *"You can see the HAL **expressing services needed by the application**. You can also see that **layers may contain layers**. It is more of a **repeating fractal pattern** than a limited set of predefined layers. **The GPIO assignments are details that should be hidden from the software.**"*

> **"Don't Reveal Hardware Details to the User of the HAL."** *"A clean embedded architecture's software is **testable off the target hardware**. A successful HAL provides that **seam or set of substitution points** that facilitate off-target testing."*

## Worked Example: The Processor Is a Detail

**Vendor tool chains extend C** — *"adding new keywords to access their processor features. **The code will look like C, but it is no longer C.**"* Vendor compilers may expose *"what look like global variables to give access directly to processor registers, IO ports, clock timers, IO bits, interrupt controllers."*

**The `acmetypes.h` trap** — a vendor header defining `Uint_32`, `Uint_16`, `Int_32`… differently per DSP variant (`_ACME_X42` vs `_ACME_A42`):

> *"You can't compile your code unless you include this header. If you use it and define `_ACME_X42` or `_ACME_A42`, **your integers will be the wrong size if you try to test your code off-target**. If that is not bad enough, one day you'll want to port your application to another processor, and you will have made that task much more difficult."*

**The fix — write your own `stdint.h`** that adapts the vendor header:

```c
#ifndef _STDINT_H_
#define _STDINT_H_

#include <acmetypes.h>

typedef Uint_32 uint32_t;
typedef Uint_16 uint16_t;
typedef Uint_8  uint8_t;
typedef Int_32  int32_t;
typedef Int_16  int16_t;
typedef Int_8   int8_t;

#endif
```

**Real code from the wild** — printing "hi" to a serial port:

```c
void say_hi() {
    IE = 0b11000000;              // <- binary notation isn't C either
    SBUF0 = (0x68); while(TI_0 == 0); TI_0 = 0;
    SBUF0 = (0x69); while(TI_0 == 0); TI_0 = 0;
    SBUF0 = (0x0a); while(TI_0 == 0); TI_0 = 0;
    SBUF0 = (0x0d); while(TI_0 == 0); TI_0 = 0;
    IE = 0b11010000;
}
```

| Symbol | What it is |
|---|---|
| `IE` | Interrupt enable bits |
| `SBUF0` | Serial output buffer |
| `TI_0` | Serial transmit buffer empty interrupt — *"Reading a 1 indicates the buffer is empty"* |

> *"The uppercase variables actually access micro-controller built-in peripherals… **Yes, this is convenient — but it's not C.**"*

> *"A clean embedded architecture would use these device access registers **directly in very few places and confine them totally to the firmware**. **Anything that knows about these registers becomes firmware and is consequently bound to the silicon.**"*

**Introduce a PAL** (processor abstraction layer): *"Firmware above the PAL could be tested off-target, **making it a little less firm**."*

## Framework: The Operating System Is a Detail

**Why an OSAL** — the risks named concretely: *"What if your RTOS supplier is **bought by another company and the royalties go up**, or the **quality goes down**? What if your needs change and your RTOS does not have the capabilities you now require?"*

> *"These won't just be simple **syntactical** changes due to the new OS's API, but will likely have to adapt **semantically** to the new OS's different capabilities and primitives."*

**The choice, framed as a question that answers itself:**
> *"If your software depended on an OSAL instead of the OS directly, you would largely be writing a **new OSAL that is compatible with the old OSAL**. Which would you rather do: **modify a bunch of complex existing code, or write new code to a defined interface and behavior?** This is not a trick question. I choose the latter."*

**On the code-bloat objection**: *"the layer becomes the place where **much of the duplication around using an OS is isolated**. This duplication does not have to impose a big overhead. If you define an OSAL, you can also **encourage your applications to have a common structure** — you might provide message passing mechanisms, rather than having **every thread handcraft its concurrency model**."*

## Framework: Interfaces, Substitutability, and DRY Conditionals

**Header files as interface definitions** — with a specific discipline:
> *"**Limit header file contents to function declarations as well as the constants and struct names that are needed by the function.** Don't clutter the interface header files with data structures, constants, and typedefs that are needed by **only the implementation**. It's not just a matter of clutter: **That clutter will lead to unwanted dependencies.**"*

> *"Expect the implementation details to change. **The fewer places where code knows the details, the fewer places where code will have to be tracked down and modified.**"*

*(Substitutability example: many readers have written their own small `printf` for the target. *"As long as the interface to your `printf` is the same as the standard version, you can override the service one for the other."*)*

**DRY conditional compilation** — the war story:
> *"I recall one especially problematic case where the statement **`#ifdef BOARD_V2` was mentioned several thousand times** in a telecom application… **If I see `#ifdef BOARD_V2` once, it's not really a problem. Six thousand times is an extreme problem.**"*

**The fix**: *"What if there is a hardware abstraction layer? **The hardware type would become a detail hidden under the HAL.** If the HAL provides a set of interfaces, instead of using conditional compilation, we could use **the linker or some form of runtime binding** to connect the software to the hardware."*

## Mental Models
- **Firmware is a dependency property, not a storage location.** Ask what a module knows about, not where it's flashed.
- **You write firmware too.** Buried SQL and un-separated Android API calls are the same defect in different clothes.
- **Design the HAL from the application's vocabulary downward.** `Indicate_LowBattery()` beats `Led_TurnOn(5)` because it names the *product's* concern.
- **Layers nest fractally.** HAL, PAL, OSAL, BSP — the pattern repeats at every level of detail-hiding.
- **Thousands of `#ifdef`s are a missing abstraction, not a build-config style.**
- **Off-target testability is the acceptance criterion** for every abstraction layer in this chapter.

## Key Takeaways
1. Software doesn't wear out — unmanaged hardware and firmware dependencies destroy it from within.
2. Firmware is defined by what it depends on and how hard it is to change, not by ROM.
3. Non-embedded developers write firmware whenever they bury SQL or spread platform dependencies.
4. Passing the App-titude test ("it works") is not the same as having an architecture.
5. The target-hardware bottleneck comes from structure, not from embedded's genuine constraints.
6. Insert a HAL between software and firmware, tailored to the software's needs — hide GPIO, flash, and registers.
7. Confine vendor C extensions and register access to very few firmware files; add a PAL and use your own `stdint.h`.
8. Treat the OS as a detail behind an OSAL; porting then means writing a new OSAL, not editing complex code.
9. Keep interface headers minimal — implementation details in headers become unwanted dependencies.
10. Replace mass conditional compilation with HAL interfaces plus linker or runtime binding.

## Connects To
- **Ch 15**: policy vs. details — the hardware, processor, and OS are all details.
- **Ch 22**: HAL/OSAL/PAL are the Dependency Rule applied to embedded layers.
- **Ch 28 (The Test Boundary)**: off-target testability is the same "don't depend on volatile things" rule.
- **Ch 30–32**: database, web, and frameworks as details — this chapter is the embedded instance of the same argument.
- **Ch 16 (Independence)**: `#ifdef BOARD_V2` × 6000 is a DRY violation at deployment scale.
- **Doug Schmidt**, *"The Growing Importance of Sustaining Software for the DoD"*; **Kent Beck** (make it work / right / fast); **Brooks**, *The Mythical Man-Month*; **Hunt & Thomas**, *The Pragmatic Programmer* (DRY).
