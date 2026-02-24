# ============================================================
# API/ALLOCATION.PY — Static allocation before workers start
# ============================================================
# Allocates SharedMemory segments and builds WorkerContexts.
# Process objects are NOT created here — mpop.run() builds them
# after injecting handlers_map and pool_proxy.
# ============================================================

import os
import ctypes
from multiprocessing import Lock, Queue
from multiprocessing.shared_memory import SharedMemory

from .slot import TaskSlot, SLOT_SIZE
from .queues import SharedTaskQueue
from .worker import (WorkerContext, WorkerStatusStruct,
                     STATE_INIT, WORKER_STATUS_SIZE)
from .errors import (ErrorCode, Component, format_error,
                     validate_num_slots, validate_num_workers)


# ============================================================
# ALLOCATION RESULT
# ============================================================
class AllocationResult:
    __slots__ = ("queue", "status_shm", "log_queue",
                 "num_workers", "lock", "batch_lock", "worker_contexts")

    def __init__(self, queue, status_shm, log_queue, num_workers,
                 worker_contexts):
        self.queue = queue
        self.status_shm = status_shm
        self.log_queue = log_queue
        self.num_workers = num_workers
        self.lock = queue.lock
        self.batch_lock = queue.batch_commit_lock
        self.worker_contexts = worker_contexts


# ============================================================
# ALLOCATE
# ============================================================
def allocate(num_workers, queue_slots=4096, queue_name="shared_queue",
             debug_task_delay=0.0, admin_frequency=5,
             handler_module=None, worker_batch_size=64):

    err = validate_num_workers(num_workers)
    if err:
        raise ValueError(format_error(
            ErrorCode.E003_NUM_WORKERS_EXCEEDED, Component.ALLOCATION, err))

    err = validate_num_slots(queue_slots)
    if err:
        raise ValueError(format_error(
            ErrorCode.E002_INVALID_NUM_SLOTS, Component.ALLOCATION, err))

    supervisor_pid = os.getpid()
    lock = Lock()
    batch_lock = Lock()
    log_queue = Queue()

    queue = SharedTaskQueue(
        queue_id=1, _num_slots=queue_slots,
        num_workers=num_workers, queue_name=queue_name,
        lock=lock, batch_lock=batch_lock, log_queue=log_queue,
    )

    all_available = (1 << 64) - 1
    used_mask = (1 << num_workers) - 1
    queue._state.available_consumer_ids = all_available & ~used_mask
    queue._state.active_worker_count = num_workers

    # Worker status shared memory
    status_size = WORKER_STATUS_SIZE * 64
    status_shm = SharedMemory(create=True, size=status_size)

    # Initialize status slots — temporary view, released immediately
    StatusArray = WorkerStatusStruct * 64
    _tmp = StatusArray.from_buffer(status_shm.buf)
    for i in range(64):
        _tmp[i].state = STATE_INIT
        _tmp[i].local_queue_count = 0
        _tmp[i].completed_tasks = 0
    del _tmp

    # Build worker contexts (Process objects created at run() time)
    contexts = []
    for i in range(num_workers):
        wctx = WorkerContext(
            worker_id=i, consumer_id=i,
            shm_slots_name=queue.shm_slots_name,
            shm_state_name=queue.shm_state_name,
            shm_status_name=status_shm.name,
            _num_slots=queue_slots,
            lock=lock, batch_lock=batch_lock,
            log_queue=log_queue,
            supervisor_pid=supervisor_pid,
            debug_task_delay=debug_task_delay,
            admin_frequency=admin_frequency,
            handler_module=handler_module,
            worker_batch_size=worker_batch_size,
            pool_proxy=None,      # injected at run()
            handlers_map=None,    # injected at run()
        )
        contexts.append(wctx)

    return AllocationResult(
        queue=queue, status_shm=status_shm,
        log_queue=log_queue, num_workers=num_workers,
        worker_contexts=contexts,
    )