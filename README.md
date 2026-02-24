# Mpop — Cache Aware Parallel Worker Models (Python)

**Experimental Version**

Language: Python\
Requires: Python >= 3.10 (tested on 3.12 and 3.14)\
Dependencies: psutil

Mpop is a single-producer, multiple-consumer task queue for parallel processing in Python.

The main goal is simple: keep workers busy and use memory efficiently.

It does this by packing task data into fixed-size 128-byte slots that sit in shared memory, where workers batch-claim ranges using a bitmap coordination layer to reduce lock contention.

The framework uses a typed handler system where all function parameters must have type annotations.\
Types are inspected once at registration time and pre-compiled into pack/unpack functions, so the hot path during task execution has zero per-argument branching.

This version is an experimental rewrite. The previous version supported legacy untyped handlers and multiple slot sizes; all of that has been removed.\
Everything is now typed-only dispatch with a single universal 128-byte slot.

---

## Process Model

The model is simply a (supervisor, set of workers) pair.\
Supervisor is responsible for spawning, administrating, and logging worker processes. Workers execute tasks.\
For efficiency the supervisor can run in minimal mode (display=False).

Workers fetch tasks from a shared queue in batches and copy them into a local queue.\
The rationale behind this is:
 - To support dynamic request handling and administrative operations
 - To let the supervisor efficiently block incoming requests when the shared queue is full
 - Reduce lock contention since workers claim ranges atomically instead of dequeueing one by one

The supervisor introduces a potential single point of failure. If supervisor is terminated, workers detect this via periodic health checks and also terminate.

---

## Slot Layout

Single universal slot. No variants.

```
┌──────────┬──────────┬──────────────────┬──────────────┐
│ tsk_id 8B│ fn_id 8B │   args 80B       │   meta 32B   │ = 128B
│ uint64   │ uint64   │  10 × int64      │              │
└──────────┴──────────┴──────────────────┴──────────────┘
```

Each argument occupies exactly 8 bytes regardless of type.\
`int`, `float`, `bool` are packed directly into the int64 slot using struct.\
`str`, `list`, `dict`, `tuple`, `set`, `bytes` and other types that dont fit in 8 bytes go through an ArgsPool (multiprocessing.Manager dict) and the slot stores the pool_id reference.

This means you get up to 10 arguments per handler function.

### Type System

DIRECT types (stored in slot, zero overhead):
 - `int` → int64
 - `float` → float64  
 - `bool` → bool (as int64)
 - Extended: `int8`, `uint8`, `int16`, `uint16`, `int32`, `uint32`, `float32`

REFERENCE types (stored in ArgsPool, retrieved by workers):
 - `str`, `list`, `dict`, `tuple`, `set`, `bytes`, any custom class

The ArgsPool uses lazy initialization. If your handlers only use DIRECT types, no Manager process is ever spawned and there's zero overhead from it.

---

## Basic Usage

```python
from api import MpopApi, TaskResult


def multiply(a: int, b: int) -> int:
    return a * b

def power(base: int, exp: int) -> int:
    return base ** exp

HANDLERS = {
    0x8000: multiply,
    0x8001: power,
}

if __name__ == "__main__":
    app = MpopApi(
        workers=4,
        display=True,
        handler_module=__name__,
        debug_delay=0.05,
    )

    app.register_handlers(handlers_dict=HANDLERS)
    app.validate()

    for i in range(100):
        app.enqueue(multiply, a=i, b=10, tsk_id=i)

    for i in range(2, 6):
        app.enqueue(fn_id=0x8001, args=(i, 3), tsk_id=i + 100)

    app.run()
```

### Decorator Style

```python
if __name__ == "__main__":
    app = MpopApi(workers=4, display=True)

    @app.task(fn_id=0x8000)
    def multiply(a: int, b: int) -> int:
        return a * b

    app.enqueue(multiply, a=5, b=10)
    app.run()
```

### Enqueue Styles

Two ways to enqueue:

```python
# Pythonic — pass function reference + keyword args
app.enqueue(multiply, a=10, b=3.14)

# Positional — pass fn_id + args tuple
app.enqueue(fn_id=0x8000, args=(10, 3.14))
```

Both are zero-validation at enqueue time. Type checking already happend at registration.

---

## Configuration

```python
app = MpopApi(
    workers=4,              # number of worker processes
    queue_slots=4096,       # shared queue size (MUST be power of 2)
    display=True,           # TTY supervisor display
    auto_terminate=True,    # auto send TERMINATE when idle
    debug=False,            # debug mode
    debug_delay=0.0,        # artificial delay per task (seconds)
    handler_module=None,    # module name containing HANDLERS dict
    worker_batch_size=16,   # max tasks per batch claim
    poll_interval=0.05,     # supervisor poll rate
    idle_check_interval=10, # polls before checking idle state
    queue_name="mpop",      # shared memory name prefix
)
```

`queue_slots` must be a power of 2. The supervisor will refuse to allocate otherwise.

---

## REFERENCE Type Example

When your handler takes `str`, `list`, or other non-primitive types, the data flows through ArgsPool automatically:

```python
def process(name: str, items: list, scale: float) -> str:
    return f"{name}: {sum(items) * scale}"

HANDLERS = {0x9000: process}

if __name__ == "__main__":
    app = MpopApi(workers=2, handler_module=__name__)
    app.register_handlers(handlers_dict=HANDLERS)

    app.enqueue(process, name="batch", items=[10, 20, 30], scale=2.5)
    app.run()
```

The `name` and `items` params are REFERENCE type — they get stored in ArgsPool in the main process and retrived by the worker process that picks up the task. `scale` is DIRECT (float64) so it sits in the slot directly.

Workers automatically clean up pool entries after retrieval.

---

## Important Notes and Limitations

**This is experimental.** Things work but there are rough edges. Read this section before using.

### 1. Must run under `if __name__ == "__main__"`

macOS uses the `spawn` start method for multiprocessing. This means the entire module gets re-imported in each child process.\
If `MpopApi()` is at module level without the guard, child processes will re-create it and you get an infinite spawn loop.

This is not mpop-specific, its a Python multiprocessing thing on macOS.\
Linux uses `fork` so it generally works without the guard, but you should always use it anyway.

### 2. Handlers must exist before enqueue and run

All handlers need to be registered **before** you call `app.enqueue()` or `app.run()`.

The decorator style (`@app.task()`) works but its somewhat fragile:
 - The decorated function must be defined before any enqueue calls
 - If you define handlers in a different module and import them, the decorator might not have ran yet depending on import order
 - For anything serious, the HANDLERS dict + `register_handlers()` pattern is more reliable

If you enqueue a task with an fn_id that workers dont know about, you'll see `[Error] tsk_id=N: Unknown fn_id 0xNNNN` in the log. The task is silently dropped.

### 3. All handler parameters must have type annotations

No exceptions. If even one parameter is missing a type hint, registration will fail with `TypeResolutionError`.

This is intentional — the type system compiles pack/unpack at registration and needs to know every parameter's type.

```python
# Good
def multiply(a: int, b: int) -> int:
    return a * b

# Bad — will fail at registration
def multiply(a, b):
    return a * b
```

### 4. Maximum 10 arguments per handler

The slot has 10 int64 argument slots. If your function has more than 10 parameters it will error at registration.\
If you need more arguments, pack related data into a dict or list (those go through ArgsPool as REFERENCE type).

### 5. ArgsPool overhead for REFERENCE types

REFERENCE types (`str`, `list`, `dict`, etc.) require inter-process communication through `multiprocessing.Manager`.\
This is noticeably slower than DIRECT types which just get packed into the slot.

For performance critical paths, try to stick to `int`, `float`, `bool` parameters when possible.

The Manager is lazily initialized — it only starts when you first enqueue a task with REFERENCE type argument. If you never use REFERENCE types, no Manager process is created.

### 6. Python 3.14 annotation changes

Python 3.14 changed how annotations work (PEP 749). The type resolver has multiple fallback chains to handle this, but if you run into issues with type resolution on 3.14, try using explicit type hints instead of relying on `from __future__ import annotations`.

### 7. Return values are not collected

Handlers can return values and they get wrapped into `TaskResult` internally. But there is currently no mechanism to send results back to the main process. Return values exist mainly for error handling — if a handler raises, the worker logs it and continues.

```python
# Both work fine
def add(a: int, b: int) -> int:
    return a + b

def add_v2(a: int, b: int):
    result = a + b
    return TaskResult(success=True, value=result)
```

### 8. handler_module vs handlers_map

There are two ways handlers reach worker processes:

`handlers_map` — Built from your registry at `run()` time, pickled into each Process object. This is how `@app.task()` and `register_handlers()` work.

`handler_module` — Workers import the module by name and look for a `HANDLERS` dict. This is the `handler_module=__name__` parameter.

Both can coexist. If the same fn_id shows up in both, handlers_map takes priority.\
For the most reliable setup, use both:
```python
app = MpopApi(handler_module=__name__)
app.register_handlers(handlers_dict=HANDLERS)
```

This way, workers have two chances to discover each handler.

---

## Shared Queue

The shared queue is a circular buffer in shared memory. Default size is 4096 slots.

Scheduling uses batching and range-claiming to reduce lock contention.\
Workers atomically claim a range of slots from the shared queue, copy them to their local queue, then process everything without holding any locks.

A bitmap (`active_batches`) tracks which workers currently hold claimed but uncommited ranges.\
When the last worker finishes its batch, committed occupancy is updated so the producer can reclaim space.

To change queue size:
```python
app = MpopApi(queue_slots=4096*2)  # must be power of 2
```

---

## File Structure

```
api/
├── __init__.py       — exports
├── slot.py           — 128B TaskSlot definition
├── types.py          — type classification, pack/unpack codegen
├── errors.py         — error codes and exceptions
├── registry.py       — ArgsPool + FunctionRegistry
├── tasks.py          — TaskResult, TaskDispatcher
├── queues.py         — SharedTaskQueue, LocalTaskQueue
├── worker.py         — worker process entry point
├── allocation.py     — static allocation
├── supervisor.py     — supervisor controller + TTY display
└── mpop.py           — MpopApi user-facing class
```

---

## Road Map
 - Result Collection: Returning task results back to the main process via shared memory or queue
 - Argument Pool: Better cleanup strategy, capacity monitoring, maybe replacing Manager with SharedMemory-based pool
 - Error Recovery: Worker failure detection, health check improvements, respawning
 - Supervisor Recovery: Reconnecting a new supervisor with existing workers after failure. This has been challenging because recovery requires reliable method to re-link the supervisor with workers which introduces coordination and state-reconciliation problems.
 - Meta Data: Building the 32B meta section to support timestamping, flexible logging levels
 - Benchmarking: Comparative analysis against multiprocessing.Pool and concurrent.futures
## Road Map
- Argument Pool: Implementing a pool for storing lengthy arguments and user-facing functions for integrating handler validation and creating a registry.
- Meta Data: Building a meta section to support timestamping, flexible logging such as no log, detailed log, and better control.
- Error Recovery: Handling worker failures, health check, respawning.
- Supervisor Recovery: Implementing mechanisms to respawn and reconnect the supervisor process after failure.
