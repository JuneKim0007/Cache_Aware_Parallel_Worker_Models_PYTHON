# Multiprocessing of Python (MPOP)- Documentation

## Overview

The Mpop API provides a single-producer multiple-consumer task-queue model mainly desigend for high-throughput, CPU-bound workloads in Python.

It focuses on making multiprocessing faster, simpler, and more predictable, while still supporting dynamic task enqueuing and basic orchestration tools.

Mpop offers a clean, familiar user facing API that handles the hard parts internally to support better development. 

### Why Mpop?

Mpop is mainly built to assist developers to have better experiences in Python multiprocessing while providing competetive performance.\
The specific benefits Mpop provides are:
 - Higher throughput than Python’s standard multiprocessing module even with additional coordination layer!
 - Simpler process management through a unified shared queue
 - Easier customization of flow-control features.
 - Supervisor process support for logging, monitoring, and administration for each processes.

### Core Design Principles and How It Works

Mpop's design principle involves two things:
 - No unncessary complication
 - Keep static allocation heavy, make runtime light.

That is Mpop is designed to support developability while moving as much work as possible to initialization time to reduce runtime overhead.

How did Mpop acheive that?

 - A custom shared queue designed to maximize process utilization time
 - Reduced reliance on Python’s dynamic features where performance matters
 - A user-facing API that stays simple, while complexity is handled internally

The result is a multiprocessing system that is both fast and intuitive to use! 
To see this please refer to Benchmark section

--- 

## Introduction

The process model is simply (Supervisor, a set of Workers) pair.


[image_place_holder]



Supervisor is mainly responsible for mainly:
 - spawning/despawning Workers.
 - Creating/clearing shared memeory view on data among Workers.
 - Logging and Administrating Workers.
 - Receiving work requests both dynamically and statically.

Workers, on the other hands, as their name suggests are the executions for running the actual code. 

Once Mpop() is called, the process that calls Mpop turns into Supervisor, creating essential features as well as spawning Workers.

One of the important feature that gets allocated during the initial period is Shared Queue.

--- 
## Shared Queue and Local Queue

### Shared Queue:

Shared Queue is simply an array shared among Supervisor and Workers that stores data about how Workers should run the code. \
_So Shared Queue is nothing really fancy!_

Shared Queue by default has 65536 elements called Slot\
Each Slot stores data on each Task containing how Workers should run the code.

The Shared Queue is circular, providing dynamic advantages in handling/dequeuing job requests with relatively simple logic.

During the allocation, you can simply write:

`
    app = MpopApi(
       queue_slots: int = 4096*2
    )
`
to changed the size of Shared Queue.

However, one important thing about Shared Queue is that **The size of Shared Queue MUST BE POWER OF 2**\
Otherwise, the supervisor will refuse to allocate the shared queue.\
__Note__: the ristrictions rooted from how Shared Queue moves its tail and head for efficiency _

### Local Queue:

Another Queue that are created during the initial period is Local Queue(s).\
Local Queue is really nothing different from Shared Queue except it's only accessible to each assigend Worker and have much smaller size.\

During the Runtime, each worker fetch a bunch of data at once from Shared Queue and store into their Local Queue.\
So Local Queue essentially holds the same information that was once existed in Shared Queue.

Keen Reader may see that that would be unncessary and hurt performance. 
While that could be true, that in idle situation, Workers would execute tasks faster if they directly read data from Shared Queue and execute task,\
There are a few real-world limitations to that model:

 - Worker has to hold the Lock each time they access Shared Queue.
 - Frequent access to Shared Queue can slow down performance, especially on multi-core systems.
 - As the number of workers grows, the overhead of coordinating access to Shared Queue costs more.

and a few others!

### Scheduling Method

Many of the concurrency problems of shared Data structure that attempts to ensure consistency of Shared Queue arise from Lock Contention.\

To mitigate this problem, instead of using a naive Shared Queue Model, Mpop does the following:

- **Batch-Dequeuing:** instead of dequeuing each time, Workers put tasks into a batch and dequeue at once.
- **Range-Claiming:** Workers do not even dequeue in the critical section, but rather they only mark the range telling other workers the part of Shared Queue is reserved.

In this way, Workers barely hold the lock and the cost from Lock contention becomes much insiginificant (~3~1% in total Worker CPU time)!

There are few other methods implemented in Mpop to maintain high performance while handling dynamic work requests, mainly:
 - **Copying-At-Once:** Workers do copy each element in Shared Array; instead Workers copy the range of data at once using `ctypes.memmove`
 - **Cache-Awareness:** Each Data is designed to be aligend with 64 bytes matching typical cache line boundaries in modern CPU.s
 - **Compacted Data:** Each Queue element is designed to be very small, typically around 256 bytes, while still efficiently storing the necessary data.

However for curious readers, you may refer to this [link]() documenting in details how Mpop's Scheduling method works under the hood.

--- 

### Slots:

Slots are elements of both Shared Queue and Local Queue. You can think of it like a C array, for example:
`
Slot arr[50];
`
which creates 50 Slot elements within an array.

Slots use the same class structure in both Shared queue and Local queue, ensuring consistency while remaining compact.

Each slot primarily stores four categories of variables:
 - tsk_id – Identifier variable used for debugging purposes \
 - fn_id – identifies which function to execute\
 - args – the argument values for the function\
 - meta – additional metadata to handle information not related to function logic (e.g., timestamps, logs)\

There are four different slot classes: TaskSlot128, TaskSlot196, TaskSlot256, TaskSlot512. 
The numbering behinds each name indicates the size of the slot;\
and generally bigger size means to more or lengthier argument.
_What if I have a really long lengthy argument that won't be fit into the slot?\ 
In that case Mpop  automatically creates a mapping between the argument and arg value that would be passed into the slot, handling lengthy data flexibly_

Why Mpop uses strict fixed size? Well, the reasonings are concerned on two main factors:
 - 64 bytes are generally aligned with CPU cache lines.
 - Python's dynamism would cost much memory space without explicitly fixing data size.


If we create Shared queue of TaskSlot256 in 65536 size,\
there would be total around 256*65536 ≈ 268MB memory allocation. 

So choosing the size of slot and number of Slots for the Shared Queue sometimes require careful thinking;\
Though in most of cases default size and slot classes would be more than small enough to fit into modern CPUSs.

During the allocation period, Mpop automatically ensures both Shared Queue's and Local Queue's slot classes are identical and unchangable.\

By default Mpop uses Taskslot196 to store information. However, you can always change it by telling Mpop to create a queue of different slot types.
For instance:

```
    app = MpopApi(
       queue_slots: int = 4096*2,
       slot_class: Type[ctypes.Structure] = TaskSlot128
    )
```

### Slot data (tsk_id, fn_id, c_args, meta)

The structure of the default slot is:
```class TaskSlot196_cargs(ctypes.Structure):
    196-byte task slot with char+int arguments.
    _align_ = 196
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),
        ("fn_id", ctypes.c_uint32),
        ("args", ctypes.c_int64 * 5),
        ("c_args", ctypes.c_char * 92),  
        ("meta", ctypes.c_uint8 * 56),
    ]
```
Right now you may want to directly refer to slot.py to see the structure
However, later I will document different slot types and structure.

## Worker

A worker is a single process spawned by the supervisor (or an entrypoint process).\
Each worker is bound to a shared task queue and maintains its own local queue, which stores tasks fetched from the shared queue.

Workers fetch tasks from the shared queue and store the duplicate information into its local queue by design.\
The rationales behind this design are simple:
 - To support dynamic request handling and administrative operations
 - To allow the supervisor to efficiently block incoming requests when the shared queue is full

This additional abstraction layer not only simplifies the supervisor’s blocking efficiency but also improve blocking logic:\
Rather than continuously polling individual workers to allocate tasks, the supervisor only needs to enqueue tasks 
into the shared queue when space becomes available in the shared queue.

The worker implicitly receives three pieces of information to process tasks: Function ID, Arguments, and Meta Data.\
I will unpack them shortly but keep in mind that those three informations are: 
 - Function ID – a mapping value that tells the worker which function to execute.
 - Arguments – the values to pass to the function when it runs.
 - Meta Data – intended to carry timestamps, logging information, etc. Though it is currently unimplemented. (Refer to Road Map)
---



### Creating handler and Enqueuing Task:

The multi processing models are often complex to implement 
To manage the complexity created due to managing multi process, this API uses advantage of handler functions that are somewhat indirect way to pass tasks to workers.
For example:

```
def some_function_handler(slot: TaskSlot196):
    arg1 = slot.args[0]
    arg2 = slot.args[1] if len(slot.args) > 1 else 1
    <Function LOGIC>
    return TaskResult(success=True, value=result)
```

Once you've created the handler function you can simply create a registry
```
HANDLERS = {
    0xB000: some_function_handler
}
```
Registry is simply a set of key, value pairs (fn_id, function()).
Remember Workers take fn_id to figure out which functions to actually run.
This design was intentional to reduce the size of the slot for memory efficiency:\
while Queues can store fn_id which are only 4 Hexadecimal values (2 bytes), the actual function definition will be stored only once in the memory.

Once you've created a registry, you can simply let shared queue to enqueue tasks.\
For instance:
```
app.enqueue(fn_id=0xB002, args=(i, 0), tsk_id=i)
```

Now the shared queue allocate tasks and the task is available for the worker to use:

However, due to the slot being fixed size (multiples of 64 bytes), in general arguments are not going to fit; 
in this case consider using bigger slots, or using argument pools that store reference to the lengthy argument.

** Due to the slot being fixed size (multiples of 64 bytes), in general not all arguments are not going to fit; 
I’m currently designing an argument pool that stores mappings to lengthy variables.

## Supervisor
The supervisor process is responsible for managing and administering worker processes while handling incoming requests to enqueue tasks into the Shared Queue.\

The design principles are:
- Providing a user-facing process that exposes visibility into the internal state of the system
- Providing user-facing interfaces for debugging and logging
- Providing user-facing administrative controls, including spawning and terminating workers
- Internally handling both static and dynamic request for enqueuing the task into the Shared Queue.

However, the supervisor introduces a potential single point of failure. If the supervisor is abruptly terminated, workers perform periodic health checks; upon detecting that the supervisor is no longer alive, they also terminate.\
I am currently designing a recovery mechanism to handle supervisor failure, but this has been quite challenging because recovery requires a reliable method to re-link a newly created supervisor with existing workers, which introduces coordination and state-reconciliation problems. Please refer to the Roadmap.


### Basic Usage
```python
  def main():
    app = MpopApi(
      debug=True,
      debug_delay=0.05,
    )
    app.print_status(config=True, queue=True, workers=True)

    for i in range(100):
      app.enqueue(
      fn_id=ProcTaskFnID.INCREMENT,
      args=(i * 10, 1),
      tsk_id=i + 1,
      )
    app.run()
    return

  if name == "main":
    exit(main())
```
---
## Bench Mark

Benchmark Mechanism:

- The benchmark compares mpop against Python’s standard multiprocessing module under two conditions: statically allocated jobs and dynamic job request incoming.\
- A baseline implementation that was being compared was written using only Python standard libraries.\
- Spawning worker time were excluded from both benchmarks. (because Mpop is to reduce dynamic time while static allocation is heavy)\That is the benchmark start time is recorded when all worker processes are signalled to run for both files.\
- Mpop was run in max_performance mode, which disables optional layers such as logging to ensure a fair comparison.
- Both ran on virtual machine hosted by Cloud service and on my local machine


### Result


**Uniform Workloads**

- **900 tasks:** Standard multi-threading completed in ~0.00845s s, dynamic scheduling ~0.00890s s, Mpop: ~0.049 s. Slightly slower for very small tasks.\
- **9,000 tasks:** Standard ~0.08842s s, dynamic ~0.07862s, Mpop ~0.08267s s. Performance matches dynamic scheduling. \
- **90,000 tasks:** Standard ~.84855s s, dynamic ~0.80830s, Mpop ~0.81982s s. Nearly identical to dynamic scheduling.  
- **1,800,000 tasks:** Standard ~4.51 s, dynamic ~4.24 s, Mpop ~4.26 s. Nearly identical to dynamic scheduling.  

**Takeaways:**  
- Traditional multi-threading is slightly faster for very small workloads.  
- Our supervisor-managed system avoids workers blocking each other, keeping performance predictable.  
- Scales well to tens of thousands of tasks while maintaining near-optimal efficiency.  

      
### Interpretation
Mpop design is to maximum process to stay in a core that was spawned. 

Overall, mpop demonstrates at least 97% of 
### Interpretation and Concerns:

---
## Road Map
- Argument Pool: Implementing a pool for storing lengthy arguments and user-facing functions for integrating handler validation and creating a registry.
- Meta Data: Building a meta section to support timestamping, flexible logging such as no log, detailed log, and better control.
- Error Recovery: Handling worker failures, health check, respawning.
- Supervisor Recovery: Implementing mechanisms to respawn and reconnect the supervisor process after failure.
