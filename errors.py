# ============================================================
# ERRORS.PY
# ============================================================

from enum import IntEnum
from typing import Optional
from multiprocessing import Queue


import sys
#============================================================
# COMPONENT NAMES
#============================================================
class Component:
    SHARED_QUEUE = "SharedQueue"
    LOCAL_QUEUE = "LocalQueue"
    WORKER = "Worker"
    ALLOCATION = "Allocation"
    SLOT = "Slot"
    SUPERVISOR = "Supervisor"


#============================================================
# ERROR CODES
#============================================================
class ErrorCode(IntEnum):
    E001_QUEUE_FULL = 1
    E002_INVALID_NUM_SLOTS = 2
    E003_NUM_WORKERS_EXCEEDED = 3
    E004_INVALID_BATCH_SIZE = 4
    E005_QUEUE_EMPTY = 5
    
    #Worker errors
    E011_INVALID_CONSUMER_ID = 11
    E012_SHARED_QUEUE_NONE = 12
    E013_WORKER_SPAWN_FAILED = 13
    E014_WORKER_CRASHED = 14
    
    #Allocation errors
    E021_SHM_ALLOC_FAILED = 21
    E022_PROCESS_SPAWN_FAILED = 22
    E023_INVALID_SLOT_CLASS = 23
    
    #Slot errors
    E031_UNKNOWN_SLOT_CLASS = 31
    E032_SLOT_ALREADY_REGISTERED = 32


#============================================================
# ERROR FORMATTING
#============================================================
def format_error(code: ErrorCode, component: str, message: str) -> str:
    '''
    Format error message according to standard format.
    
    Format: [Error: E{code:03d}][{component}] {message}
    '''
    return f"[Error: E{code:03d}][{component}] {message}"


def log_error(code: ErrorCode, component: str, message: str, 
              log_queue=None, print_stderr: bool = True) -> str:
    
    formatted = format_error(code, component, message)
    
    if log_queue is not None:
        try:
            log_queue.put((-1, formatted))
        except:
            pass
    
    if print_stderr:
        print(formatted, file=sys.stderr)
    
    return formatted


#============================================================
# FOR ALLOCATIONS
#============================================================
def is_power_of_two(n: int) -> bool:
    return n > 0 and (n & (n - 1)) == 0


def validate_num_slots(num_slots: int) -> Optional[str]:
    if num_slots <= 0:
        return f"num_slots must be positive, got {num_slots}"
    if not is_power_of_two(num_slots):
        return f"num_slots must be power of 2, got {num_slots}"
    return None


def validate_num_workers(num_workers: int) -> Optional[str]:
    if num_workers <= 0:
        return f"num_workers must be positive, got {num_workers}"
    if num_workers > 64:
        return f"num_workers must be <= 64 (bitmap limit), got {num_workers}"
    return None


def validate_consumer_id(consumer_id: int) -> Optional[str]:
    if consumer_id < 0:
        return f"consumer_id must be non-negative, got {consumer_id}"
    if consumer_id >= 64:
        return f"consumer_id must be < 64 (bitmap limit), got {consumer_id}"
    return None