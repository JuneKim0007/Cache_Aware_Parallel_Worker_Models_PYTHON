# ============================================================
# API/ALLOCATION.PY
# ============================================================

import os
import ctypes
from multiprocessing import Process
from multiprocessing.shared_memory import SharedMemory

from .config import MpopConfig, SyncGroup, SharedRefs
from .queues import SharedTaskQueue
from .worker import (WorkerContext, WorkerStatusStruct, worker_process_entry,
                     STATE_INIT, WORKER_STATUS_SIZE,
                     STATUS_ARRAY_TYPE, STATUS_ARRAY_SIZE)


class AllocationResult:
    __slots__ = ("queue", "status_shm", "processes", "sync", "cfg")

    def __init__(self,
                 queue: SharedTaskQueue,
                 status_shm: SharedMemory,
                 processes: list,
                 sync: SyncGroup,
                 cfg: MpopConfig):
        self.queue = queue
        self.status_shm = status_shm
        self.processes = processes
        self.sync = sync
        self.cfg = cfg

    @property
    def num_workers(self) -> int:
        return self.cfg.num_workers

    def __iter__(self):
        return iter((self.queue, self.status_shm, self.processes, self.sync))

    def create_supervisor(self):
        from .supervisor import SupervisorController
        return SupervisorController(
            shared_queue=self.queue,
            status_shm=self.status_shm,
            processes=self.processes,
            sync=self.sync,
            num_workers=self.cfg.num_workers,
            config=self.cfg.supervisor,
        )

def _create_status_shm() -> SharedMemory:
    shm = SharedMemory(create=True, size=STATUS_ARRAY_SIZE)

    arr = STATUS_ARRAY_TYPE.from_buffer(shm.buf)
    for i in range(64):
        arr[i].state = STATE_INIT
        arr[i].local_queue_count = 0
        arr[i].completed_tasks = 0

    return shm


def _init_consumer_ids(queue: SharedTaskQueue, num_workers: int):
    all_available = (1 << 64) - 1
    used_mask = (1 << num_workers) - 1
    queue._state.available_consumer_ids = all_available & ~used_mask
    queue._state.active_worker_count = num_workers


def _create_worker_processes(cfg: MpopConfig,
                             shared_refs: SharedRefs,
                             sync: SyncGroup) -> list:
    processes = []
    for i in range(cfg.num_workers):
        ctx = WorkerContext(
            worker_id=i,
            consumer_id=i,
            shared_refs=shared_refs,
            cfg=cfg,
            sync=sync,
        )
        processes.append(Process(target=worker_process_entry, args=(ctx,)))
    return processes


# ============================================================
# ALLOCATE
# ============================================================
def allocate(cfg: MpopConfig) -> AllocationResult:
    cfg.check()

    # Stamp supervisor pid before spawning workers
    cfg.worker.supervisor_pid = os.getpid()

    sync = cfg.create_sync()

    queue = cfg.create_queue(sync)
    _init_consumer_ids(queue, cfg.num_workers)
    status_shm = _create_status_shm()
    shared_refs = SharedRefs(
        slots_name=queue.shm_slots_name,
        state_name=queue.shm_state_name,
        status_name=status_shm.name,
    )
    processes =_create_worker_processes(cfg, shared_refs, sync)

    return AllocationResult(
        queue=queue, status_shm=status_shm, processes=processes, sync=sync,
        cfg=cfg,
    )

def cleanup(queue: SharedTaskQueue, status_shm: SharedMemory):
    queue.cleanup()
    try:
        status_shm.close()
        status_shm.unlink()
    except Exception:
        pass