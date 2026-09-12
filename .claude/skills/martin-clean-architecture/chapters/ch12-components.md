# Chapter 12: Components

*Part IV: Component Principles*

## Core Idea
**Components are the units of deployment** — the smallest entities that can be deployed as part of a system. Well-designed components always retain the ability to be **independently deployable and therefore independently developable**.

## Reference Table: What a Component Is, Per Language

| Language / platform | Component |
|---|---|
| Java | `.jar` files |
| Ruby | gem files |
| .NET | DLLs |
| Compiled languages generally | Aggregations of binary files |
| Interpreted languages | Aggregations of source files |

*"In all languages, they are the granule of deployment."*

They may be linked into a single executable, aggregated into an archive (`.war`), or deployed as independent dynamically loaded plugins (`.jar`, `.dll`, `.exe`). **Regardless of deployment form, the independent-deployability property is what makes them well designed.**

## Worked Example: 50 Years of Getting to Plugins

The chapter is a history told as a repeating problem-and-fix cycle. The point of the history is the destination.

**Stage 1 — Absolute addresses.** The first lines of a program declared where it loaded. A PDP-8 program began with `*200`, telling the compiler to generate code loaded at address 200₈.

> *"This kind of programming is a foreign concept for most programmers today… But in the early days, this was one of the first decisions a programmer needed to make. **In those days, programs were not relocatable.**"*

**Libraries lived in source, not binary.** Programmers included the library's source with their application and compiled it all as one program. (Martin's first employer *"kept several dozen decks of the subroutine library source code on a shelf. When you wrote a new program, you simply grabbed one of those decks and slapped it onto the end of your deck."*)

**Stage 2 — Separate compilation, fixed addresses.** Devices were slow, memory expensive; compilers needed several passes but couldn't hold all source resident. *"Compiling a large program could take hours."* So the library was compiled separately and loaded at a known address — say 2000₈ — with a symbol table compiled into the application.

**The failure**: this worked only while the application fit between 0000₈ and 1777₈. When applications outgrew it, programmers **split their applications into two address segments, jumping around the function library**. As the library grew, it exceeded *its* bounds and needed more space (near 7000₈). *"This fragmentation of programs and libraries necessarily continued as computer memory grew. Clearly, something had to be done."*

**Stage 3 — Relocatability.** The compiler emits binary that a **smart loader** can relocate: the code is instrumented with flags telling the loader which loaded data must be altered — *"usually this just meant adding the starting address to any memory reference addresses."*

The compiler was also changed to emit function names as metadata: a call became an **external reference**, a definition became an **external definition**, and *"the loader could link the external references to the external definitions once it had determined where it had loaded those definitions. **And the linking loader was born.**"*

**Stage 4 — Linkers.** By the late 1960s–70s, programs grew and linking loaders became too slow — libraries sat on magnetic tape, and *"a linking loader could take more than an hour just to load the program."* So loading and linking were split: the slow part became a separate application, **the linker**, producing a *linked relocatable* that a relocating loader could load quickly.

**Stage 5 — The 1980s treadmill.** `.c` → `.o` → linker → executable. Individual modules compiled fast; all of them took time; the linker took more. *"Turnaround had again grown to an hour or more in many cases."*

> **Murphy's law of program size: "Programs will grow to fill all available compile and link time."**

**Stage 6 — Moore wins.** *"Along came Moore, and in the late 1980s, the two battled it out. **Moore won that battle.**"* Disks shrank and sped up; RAM became cheap enough to cache much of the disk; clock rates went from 1 MHz to 100 MHz.

> *"By the mid-1990s, the time spent linking had begun to shrink faster than our ambitions could make programs grow. In many cases, link time decreased to a matter of **seconds**. For small jobs, the idea of a linking loader became feasible again."*

**Stage 7 — The plugin architecture.** Active-X, shared libraries, the beginnings of `.jar` files. *"We could link together several `.jar` files, or several shared libraries in a matter of seconds, and execute the resulting program. **And so the component plugin architecture was born.**"*

Today: drop a custom `.jar` in a folder to mod Minecraft; drop a DLL in to plug Resharper into Visual Studio.

## Mental Models
- **Deployment granularity is an architectural choice, not a build artifact.** What you can deploy separately is what you can develop separately — and that determines team structure.
- **Every era's speedup was consumed by ambition.** Murphy's law of program size is the reason no tooling improvement is permanent relief; only structure is.
- **The ability to plug things in was hard-won, not free.** *"It has taken 50 years, but we have arrived at a place where component plugin architecture can be the casual default as opposed to the herculean effort it once was."*

## Key Takeaways
1. Components are the granule of deployment — jars, gems, DLLs, or aggregated source.
2. The defining property is **independent deployability**, which enables independent developability.
3. The history is a chain of forced fixes: absolute addresses → separate compilation → relocatable binaries → linking loader → linker → fast linking again.
4. Murphy's law of program size: programs grow to fill all available compile and link time.
5. Moore's law eventually outran that growth, making load-time linking feasible again.
6. Runtime-pluggable dynamically linked files **are** the software components of modern architectures — treat that capability as the default, because it now is.

## Connects To
- **Ch 5**: OO gave us safe polymorphism; this chapter gave us the deployment mechanism. Together they make plugins practical *anywhere, for anything*.
- **Ch 13–14**: which classes go in which component, and how components may depend on each other.
- **Ch 28 (The Test Boundary)** and **Ch 32 (Frameworks Are Details)**: plugin thinking applied to tests and frameworks.
- **Moore's law**: doubling every 18 months — held from the 1950s to 2000, *"but then, at least for clock rates, stopped cold."*
