# Multiprocessing of Python (MPOP)- Documentation

## Overview

The Mpop API provides a single-producer multiple-consumer task-queue model mainly desigend for high-throughput, CPU-bound workloads in Python.

It focuses on making multiprocessing faster, simpler, and more predictable, while still supporting dynamic task enqueuing and basic orchestration tools.

Mpop offers a clean, familiar user facing API that handles the hard parts internally to support better development. 

### Why mpop?

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

 - A custom shared queue designed to maximize process utilization time \
 - Reduced reliance on Python’s dynamic features where performance matters\
 - A user-facing API that stays simple, while complexity is handled internally

The result is a multiprocessing system that is both fast and intuitive to use! 
To see this please refer to Benchmark section

--- 

## OverView

The process model is simply (supervisor, a set of workers) pair.\
Supervisor is mainly responsible for spawning/despawning, adminstrating, and logging worker processes, while workes are mainly responsible for executing tasks.\
For efficiency supervisor could run in minimal mode, in that case refer to supervisor section.

[image]


### Scheduling Method
Scheduling method is designed to increase each worker utilization time mainly by reducing lock contention cost among worker.\
The scheduling method primarly uses batching and range-claiming dequeuing tactics using bitmap(bitset) are coordination layers.\
The method is completely internal but for anyone curious, please refer to this [link]().

--- 
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

## Shared Queue and Local Queue

### Shared Queue:
Shared queue is the queue shared among multiple workers (Note: worker can be singular but there would be almost no pratical needs for singular worker).\
The Shared Queue is really nothing fancy but simply an array that stores each data describing how workers should execute tasks.

Mainly, the shared queue has by default 4096 slots for the shared queue. 

However to change the size of the shared queue, however, one can simply write:

`
    app = MpopApi(
       queue_slots: int = 4096*2
    )
`

**WARNING** THE SLOT SIZE MUST BE POWER OF 2; Otherwise, the supervisor will refuse to allocate the shared queue.

### Local Queue:
Local queue is a queue only accessible by the worker process unless worker process fail and the supervisor gets noticed.
Since the local queue is not shared among any other processes, worker would be more likely to have better access to 

### Slots:
Slots are elements of a fixed-size array. You can think of it like a C array, for example:
`
Slot arr[49];
`
which creates 50 Slot elements within an array.

Each slot primarily stores four categories of variables:
 - tsk_id – Identifier variable used for debugging purposes (the value can be ignored and that case by default will be put 0)\
 - fn_id – identifies which function to execute\
 - args – the argument values for the function\
 - meta – additional metadata (e.g., timestamps, logs)
Once Mpop creates a slot, both worker's local and shared queue share the same slot type which are fixed and unchangable.
this memory inflexibility is primarly due to the trade off between making worker and task processing to be cpu-aware by trying to control memory layout by 64 bytes.

By default Mpop uses 196_cargs slot element to store information. However, you can always change it by telling Mpop to create a queue of different slot types.
For instance:

```
    app = MpopApi(
       queue_slots: int = 4096*2,
       slot_class: Type[ctypes.Structure] = TaskSlot128
    )
```

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
