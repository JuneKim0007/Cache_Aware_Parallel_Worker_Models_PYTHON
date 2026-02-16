# Multiprocessing Task Queue System - Documentation

The API provides a **single-producer, multiple-consumer** task queue parallel worker model in Python.

The main goal of this is simple: to keep workers busy and use memory efficiently.

The Core design components are:
 - Enhancing worker Utilization time.
 - Tightly compacting any meta, coordiation data rooted from managing multi processes
 - Monitor and detect issues using supervisor process. 

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

Workers may fetch duplicate tasks from the shared queue by design.\
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
### Shared Queue and Local Queue

## Shared Queue:
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
