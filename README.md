# Multiprocessing Task Queue System - Documentation

## Table of Contents
1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Core Components](#3-core-components)
4. [Data Structures](#4-data-structures)
5. [Workflow](#5-workflow)
6. [Memory Model](#6-memory-model)
8. [Error Handling](#7-error-handling)
9. [API Reference](8-api-reference)
10. [Usage Examples](#9-usage-examples)

---

## 1. Overview

This API provides a **single-producer, multiple-consumer** task queue parallel worker model in Python. 
It bypasses Python's GIL limitation by using separate processes with shared memory for communication.

### Key Features
- Process-level parallelism (bypasses Python GIL)
- Batch range-claiming for shared Queue
- Cache-line aligned structures (64 bytes) for cache-awareness
- Configurable slot types (INT_ARGS vs CHAR_ARGS)
- Supervisor Process

---

## 2. Architecture

```
#################################################################
# SUPERVISOR[EntryPoint] (main.py)                              #
#  Creates shared memory regions                                #
#  Spawns worker processes via fork()                           #
#  Enqueues tasks (producer role)                               #
#  Reads worker status for display                              #
#################################################################
                              │ fork()
                              ▼
┌────────────────────────────────────────────────────────────
│                    SHARED MEMORY                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │Shared Queues │  │  Supervisor  │  │ Worker Stats │     │
│  │              │  │  References  │  |              │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└────────────────────────────────────────────────────────────
                              │ 
        ---------------------------------------------
        ▼                     ▼                     ▼
┌───────────────┐    ┌───────────────┐       ┌───────────────┐
│   Worker 0    │    │   Worker 1    │  ...  │   Worker N    │
└───────────────┘    └───────────────┘       └───────────────┘
```

---

## 3. Core Components

### File Structure
```
project/
├── slot.py          # Slot definitions
├── queues.py        # LocalTaskQueue, SharedTaskQueue definitions
├── worker.py        # Worker class definitions
├── allocation.py    # allocation for better usages
├── errors.py        # Error codes, validation helpers
├── main.py          # Example usage; right now the supervisor is not separated.
└── README.md        # You are here.
```

### slot.py - Slot Types
| Structure | Size | Variant | Description |
|-----------|------|---------|-------------|
| TaskSlot128 | 128B | INT_ARGS | Numeric arguments |
| TaskSlot196 | 196B | INT_ARGS | More args |
| TaskSlot128_cargs | 128B | CHAR_ARGS | String arguments |
| TaskSlot196_cargs | 196B | CHAR_ARGS | Larger strings |

---

## 4. Data Structures

### SharedQueueState (64 bytes)
| Field | Type | Description |
|-------|------|-------------|
| head | uint32 | Consumer read position |
| tail | uint32 | Producer write position |
| _num_slots | uint32 | Queue capacity |
| mask | uint32 | For modular arithmetic |
| logical_occupancy | uint32 | Producer's view of occupancy |
| active_batches | uint64 | Bitmap of active workers |
| batch_accumulation_counter | uint32 | Slots in current batch cycle |
| committed_accumulation | uint32 | Ready to reclaim |
| is_committed | uint8 | Batch cycle complete flag |
| num_batch_participants | uint8 | Current batch count |

### WorkerStatusStruct (64 bytes, cache-line aligned)
| Field | Type | Description |
|-------|------|-------------|
| state | uint8 | 0=INIT, 1=RUNNING, 2=IDLE, 3=TERMINATED |
| local_queue_count | uint32 | Tasks in local buffer |
| completed_tasks | uint32 | Total completed |

---

## 5. Shared Queue Workflow

### Batch Claiming Protocol
For more detailed information refer to this [link](https://avc-1.gitbook.io/ringqueuebitmapbatchingamongmultipleconsumer/)

**Claim (with lock):**
1. Set bit in active_batches
2. Check available slots
3. Advance head, record batch range
4. Increment accumulation counter

**Process (no lock):**
1. Copy to local buffer
2. Process each task
3. Update status

**Finish (with lock):**
1. Clear bit in active_batches
2. If bitmap=0: commit accumulation

**Producer Reclaim:**
- On enqueue, if is_committed: reclaim committed slots

---

## 6. Memory Model

### Cache Line Strategy
- Each WorkerStatusStruct occupies exactly one cache line (64 bytes)
- Workers possesses a dedicated local queue of slots that are aligned to multiples of 64 bytes
- Example: a 128-byte task slot is aligned to 128 bytes (2 cache lines)
- Currently implementing more slot types.
- Each slot starts at a cache-line boundary (typically 64byte), ensuring no two slots share a cache line
- Workers only write to their own slot to avoid cache evictions and concurrency problems
- Worker periodically write its state to a small shared buffer for supervisor to display and check.

---

## 7. Error Handling

### Error Format
```
[Error: E{code:03d}][{component}] {message}
```

### Error Codes
| Code | Description |
|------|-------------|
| E001 | Queue full |
| E002 | Invalid num_slots |
| E003 | num_workers > 64 |
| E011 | Invalid consumer_id |
| E021 | Shared memory failed |
| E022 | Process spawn failed |
| E031 | Unknown slot_class |

---

## 8. API References 
### allocate_system()
```python
allocate_system(
    num_workers: int,           # 1-64
    queue_slots: int = 256,     # Power of 2
    slot_class = TaskSlot128,
    debug_task_delay: float = 0.0
) -> (queue, status_shm, processes, log_queue, lock, batch_lock)
```

### MPSharedTaskQueue
```python
enqueue(slot) -> bool       # False if full
get_actual_occupancy() -> int
cleanup() -> None
```

---

## 9. Usage Examples

### Basic Usage
```python

##TODO: have to define handler logic!!!
##      ALSO READ ME UPDATE ONCE DONE.

from slot import TaskSlot128, ProcTaskFnID
from allocation import allocate_system, start_workers, join_workers

#Allocate
queue, status_shm, procs, log_q, _, _ = allocate_system(num_workers=4)

#Enqueue tasks
for i in range(100):
    t = TaskSlot128()
    t.tsk_id = i
    t.fn_id = ProcTaskFnID.INCREMENT
    queue.enqueue(t)

#Terminate
for i in range(4):
    t = TaskSlot128()
    t.fn_id = ProcTaskFnID.TERMINATE
    queue.enqueue(t)
#Init
start_workers(procs)
join_workers(procs)
```

---
