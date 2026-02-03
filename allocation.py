# ============================================================
# ALLOCATION.PY
# ============================================================
# User-facing API for multiprocessing task system.
# 
# Usage:
#   queue = allocate(num_workers=4, queue_slots=256)
#   queue.enqueue(task)
#   supervisor = SupervisorController(queue, ...)
#   supervisor.run()
# ============================================================

import os
import ctypes
from multiprocessing import Process, Lock, Queue
from multiprocessing.shared_memory import SharedMemory
from typing import Type, Optional

from slot import TaskSlot128
from queues import SharedTaskQueue
from worker import (WorkerContext, WorkerStatusStruct, worker_process_entry,
                    STATE_INIT, STATE_RUNNING, STATE_IDLE, STATE_TERMINATED, 
                    STATE_NAMES, WORKER_STATUS_SIZE)
from errors import ErrorCode, Component, format_error, validate_num_slots, validate_num_workers


#============================================================
# RE-EXPORT (for user convenience)
#============================================================
__all__ = [
    # Main API
    'allocate',
    'cleanup',
    # Queue class (returned by allocate)
    'SharedTaskQueue',
    # Worker status (for supervisor)
    'WorkerStatusStruct',
    'WORKER_STATUS_SIZE',
    'STATE_INIT',
    'STATE_RUNNING', 
    'STATE_IDLE',
    'STATE_TERMINATED',
    'STATE_NAMES',
]


#============================================================
# ALLOCATION RESULT
#============================================================
class AllocationResult:
    '''
    Result of allocate() - contains everything needed to run the system.
    
    Attributes:
        queue: SharedTaskQueue for enqueueing tasks
        status_shm: Shared memory for worker status (for supervisor)
        processes: List of worker processes (not started)
        log_queue: Queue for log messages
        num_workers: Number of workers allocated
    '''
    def __init__(self,
                 queue: SharedTaskQueue,
                 status_shm: SharedMemory,
                 processes: list,
                 log_queue: Queue,
                 num_workers: int):
        self.queue = queue
        self.status_shm = status_shm
        self.processes = processes
        self.log_queue = log_queue
        self.num_workers = num_workers
        
        #For backward compatibility
        self.lock = queue.lock
        self.batch_lock = queue.batch_commit_lock
    
    def __iter__(self):
        '''Allow tuple unpacking for backward compatibility.'''
        return iter((self.queue, self.status_shm, self.processes, 
                     self.log_queue, self.lock, self.batch_lock))


#============================================================
# MAIN ALLOCATION API
#============================================================
def allocate(num_workers: int,
             queue_slots: int = 256,
             slot_class: Type[ctypes.Structure] = TaskSlot128,
             queue_name: str = "shared_queue",
             debug_task_delay: float = 0.0,
             admin_frequency: int = 5) -> AllocationResult:
    '''
    Allocate a shared queue and worker processes.
    
    This is the main entry point for users.
    
    Args:
        num_workers: Number of worker processes (1-64)
        queue_slots: Number of slots in queue (must be power of 2)
        slot_class: Task slot type (TaskSlot128, TaskSlot196, etc.)
        queue_name: Queue identifier string
        debug_task_delay: Delay per task in seconds (0 = no delay)
        admin_frequency: How often workers check supervisor (every N tasks)
        
    Returns:
        AllocationResult containing queue, status_shm, processes, log_queue
        
    Raises:
        ValueError: If num_workers or queue_slots invalid
        
    Example:
        result = allocate(num_workers=4, queue_slots=256)
        result.queue.enqueue(task)
        
        # Or with tuple unpacking:
        queue, status_shm, processes, log_queue, lock, batch_lock = allocate(...)
    '''
    #Validate
    err = validate_num_workers(num_workers)
    if err:
        raise ValueError(format_error(ErrorCode.E003_NUM_WORKERS_EXCEEDED, Component.ALLOCATION, err))
    
    err = validate_num_slots(queue_slots)
    if err:
        raise ValueError(format_error(ErrorCode.E002_INVALID_NUM_SLOTS, Component.ALLOCATION, err))
    
    #Get supervisor PID
    supervisor_pid = os.getpid()
    
    #Create locks
    lock = Lock()
    batch_lock = Lock()
    
    #Create log queue
    log_queue = Queue()
    
    #Create shared queue
    queue = SharedTaskQueue(
        queue_id=1,
        _num_slots=queue_slots,
        num_workers=num_workers,
        slot_class=slot_class,
        queue_name=queue_name,
        lock=lock,
        batch_lock=batch_lock,
        log_queue=log_queue
    )
    
    #Initialize consumer IDs
    all_available = (1 << 64) - 1
    used_mask = (1 << num_workers) - 1
    queue._state.available_consumer_ids = all_available & ~used_mask
    queue._state.active_worker_count = num_workers
    
    #Create worker status shared memory
    status_size = WORKER_STATUS_SIZE * 64
    status_shm = SharedMemory(create=True, size=status_size)
    
    #Initialize status array
    StatusArray = WorkerStatusStruct * 64
    status_array = StatusArray.from_buffer(status_shm.buf)
    for i in range(64):
        status_array[i].state = STATE_INIT
        status_array[i].local_queue_count = 0
        status_array[i].completed_tasks = 0
    
    #Create worker processes
    processes = []
    slot_class_name = slot_class.__name__
    
    for i in range(num_workers):
        ctx = WorkerContext(
            worker_id=i,
            consumer_id=i,
            shm_slots_name=queue.shm_slots_name,
            shm_state_name=queue.shm_state_name,
            shm_status_name=status_shm.name,
            slot_class_name=slot_class_name,
            _num_slots=queue_slots,
            slot_size=queue.slot_size,
            lock=lock,
            batch_lock=batch_lock,
            log_queue=log_queue,
            supervisor_pid=supervisor_pid,
            debug_task_delay=debug_task_delay,
            admin_frequency=admin_frequency
        )
        
        p = Process(target=worker_process_entry, args=(ctx,))
        processes.append(p)
    
    return AllocationResult(
        queue=queue,
        status_shm=status_shm,
        processes=processes,
        log_queue=log_queue,
        num_workers=num_workers
    )


#============================================================
# CLEANUP API
#============================================================
def cleanup(queue: SharedTaskQueue, status_shm: SharedMemory):
    '''
    Cleanup shared memory after use.
    
    Args:
        queue: The SharedTaskQueue
        status_shm: Worker status shared memory
    '''
    #Cleanup queue
    queue.cleanup()
    
    #Cleanup status shared memory
    try:
        status_shm.unlink()
    except FileNotFoundError:
        pass
    except:
        pass
    
    #Prevent __del__ from trying to close
    status_shm._mmap = None
    status_shm._buf = None
    status_shm._name = None


#============================================================
# BACKWARD COMPATIBILITY
#============================================================
def allocate_system(num_workers: int,
                    queue_slots: int = 256,
                    slot_class: Type[ctypes.Structure] = TaskSlot128,
                    queue_name: str = "shared_queue",
                    debug_task_delay: float = 0.0,
                    admin_frequency: int = 5):
    '''
    Backward compatible function - returns tuple instead of AllocationResult.
    
    Returns:
        (queue, status_shm, processes, log_queue, lock, batch_lock)
    '''
    result = allocate(
        num_workers=num_workers,
        queue_slots=queue_slots,
        slot_class=slot_class,
        queue_name=queue_name,
        debug_task_delay=debug_task_delay,
        admin_frequency=admin_frequency
    )
    return tuple(result)


def cleanup_system(queue: SharedTaskQueue, status_shm: SharedMemory):
    '''Backward compatible alias for cleanup().'''
    cleanup(queue, status_shm)