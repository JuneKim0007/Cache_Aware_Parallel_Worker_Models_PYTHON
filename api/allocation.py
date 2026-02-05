# ============================================================
# API/ALLOCATION.PY
# ============================================================
# User-facing API for allocation.
# ============================================================

import os
import ctypes
from multiprocessing import Process, Lock, Queue
from multiprocessing.shared_memory import SharedMemory
from typing import Type

from .slot import TaskSlot128
from .queues import SharedTaskQueue
from .worker import (WorkerContext, WorkerStatusStruct, worker_process_entry,
                     STATE_INIT, WORKER_STATUS_SIZE)
from .errors import ErrorCode, Component, format_error, validate_num_slots, validate_num_workers


#============================================================
# ALLOCATION RESULT
#============================================================
class AllocationResult:
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
        self.lock = queue.lock
        self.batch_lock = queue.batch_commit_lock
    
    def __iter__(self):
        return iter((self.queue, self.status_shm, self.processes,
                     self.log_queue, self.lock, self.batch_lock))


#============================================================
# ALLOCATE
#============================================================
def allocate(num_workers: int,
             queue_slots: int = 256,
             slot_class: Type[ctypes.Structure] = TaskSlot128,
             queue_name: str = "shared_queue",
             debug_task_delay: float = 0.0,
             admin_frequency: int = 5) -> AllocationResult:
    
    err = validate_num_workers(num_workers)
    if err:
        raise ValueError(format_error(ErrorCode.E003_NUM_WORKERS_EXCEEDED, Component.ALLOCATION, err))
    
    err = validate_num_slots(queue_slots)
    if err:
        raise ValueError(format_error(ErrorCode.E002_INVALID_NUM_SLOTS, Component.ALLOCATION, err))
    
    supervisor_pid = os.getpid()
    lock = Lock()
    batch_lock = Lock()
    log_queue = Queue()
    
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
    
    all_available = (1 << 64) - 1
    used_mask = (1 << num_workers) - 1
    queue._state.available_consumer_ids = all_available & ~used_mask
    queue._state.active_worker_count = num_workers
    
    status_size = WORKER_STATUS_SIZE * 64
    status_shm = SharedMemory(create=True, size=status_size)
    
    StatusArray = WorkerStatusStruct * 64
    status_array = StatusArray.from_buffer(status_shm.buf)
    for i in range(64):
        status_array[i].state = STATE_INIT
        status_array[i].local_queue_count = 0
        status_array[i].completed_tasks = 0
    
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
# CLEANUP
#============================================================
def cleanup(queue: SharedTaskQueue, status_shm: SharedMemory):
    queue.cleanup()
    try:
        status_shm.close()
        status_shm.unlink()
    except Exception:
        pass