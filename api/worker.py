# ============================================================
# API/WORKER.PY
# ============================================================
# Worker process entry and status structures.
# ============================================================

import os
import sys
import ctypes
import time
import warnings
from multiprocessing.shared_memory import SharedMemory

from .slot import ProcTaskFnID, SLOT_CLASS_MAP
from .config import SyncGroup, SharedRefs
from .queues import SharedQueueState, LocalTaskQueue
from .tasks import TaskDispatcher, TaskContext


# ============================================================
# WORKER STATUS
# ============================================================
class WorkerStatusStruct(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("state", ctypes.c_uint8),
        ("_pad0", ctypes.c_uint8 * 3),
        ("local_queue_count", ctypes.c_uint32),
        ("completed_tasks", ctypes.c_uint32),
        ("_padding", ctypes.c_uint8 * 52),
    ]

WORKER_STATUS_SIZE = ctypes.sizeof(WorkerStatusStruct)
STATUS_ARRAY_TYPE = WorkerStatusStruct * 64
STATUS_ARRAY_SIZE = WORKER_STATUS_SIZE * 64

STATE_INIT = 0
STATE_RUNNING = 1
STATE_IDLE = 2
STATE_TERMINATED = 3

STATE_NAMES = {
    STATE_INIT: "INIT",
    STATE_RUNNING: "RUNNING",
    STATE_IDLE: "IDLE",
    STATE_TERMINATED: "TERMINATED"
}


# ============================================================
# WORKER CONTEXT
# ============================================================
class WorkerContext:
    """Picklable context shipped to each worker process.

    Fields:
        worker_id, consumer_id  → identity
        shared_refs             → SharedRefs  (shm names for attachment)
        cfg                     → MpopConfig  (slot, queue_slots, worker config)
        sync                    → SyncGroup   (lock, batch_lock, log_queue)

    Worker reads:
        cfg.slot         → SlotConfig  (slot class + metadata)
        cfg.queue_slots  → int         (shared queue capacity)
        cfg.worker       → WorkerConfig(batch_size, admin_freq, delay, handler, supervisor_pid)
    """
    __slots__ = (
        "worker_id", "consumer_id",
        "shared_refs", "cfg", "sync",
    )

    def __init__(self,
                 worker_id: int,
                 consumer_id: int,
                 shared_refs: SharedRefs,
                 cfg,
                 sync: SyncGroup):
        self.worker_id = worker_id
        self.consumer_id = consumer_id
        self.shared_refs = shared_refs
        self.cfg = cfg
        self.sync = sync


# ============================================================
# SUPERVISOR CHECK
# ============================================================
def is_supervisor_alive(pid: int) -> bool:
    if pid <= 0:
        return True
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


# ============================================================
# HANDLER LOADING
# ============================================================
def _load_handlers(dispatcher: TaskDispatcher, handler_module: str, log_queue, worker_id: int):
    """Load user handlers into dispatcher. Cold-path, runs once per worker."""
    if not handler_module:
        return
    try:
        import importlib
        mod = importlib.import_module(handler_module)

        if hasattr(mod, 'HANDLERS'):
            for fn_id, handler in mod.HANDLERS.items():
                dispatcher.register(fn_id, handler)
            log_queue.put((worker_id, f"Loaded {len(mod.HANDLERS)} handlers from {handler_module}"))
        elif hasattr(mod, 'register_handlers'):
            mod.register_handlers(dispatcher)
            log_queue.put((worker_id, f"Registered handlers via {handler_module}.register_handlers()"))
        else:
            log_queue.put((-1, f"[Warning] {handler_module} has no HANDLERS dict or register_handlers()"))
    except Exception as e:
        log_queue.put((-1, f"[Error] Failed to load {handler_module}: {e}"))


# ============================================================
# SHARED MEMORY ATTACHMENT
# ============================================================
def _attach_shared_memory(shared_refs, slot_cfg, num_slots):
    """Attach to all shared memory segments.
    Returns (shm_slots, shm_state, shm_status, slots, state, status_array).
    """
    shm_slots = SharedMemory(name=shared_refs.slots_name)
    shm_state = SharedMemory(name=shared_refs.state_name)
    shm_status = SharedMemory(name=shared_refs.status_name)

    slot_class = slot_cfg.cls
    if slot_class is None:
        slot_class = SLOT_CLASS_MAP.get(slot_cfg.cls_name)

    SlotArray = slot_class * num_slots
    slots = SlotArray.from_buffer(shm_slots.buf)
    state = SharedQueueState.from_buffer(shm_state.buf)

    status_array = STATUS_ARRAY_TYPE.from_buffer(shm_status.buf)

    return shm_slots, shm_state, shm_status, slots, state, status_array


# ============================================================
# SHARED MEMORY DETACH (worker-side cleanup)
# ============================================================
def _detach_shared_memory(slots, state, status_array, my_status,
                          shm_slots, shm_state, shm_status):
    """Release buffer references and close (NOT unlink) shared memory."""
    del slots
    del state
    del status_array
    del my_status

    for shm in (shm_slots, shm_state, shm_status):
        try:
            shm.close()
        except Exception:
            pass

    try:
        sys.stderr = open(os.devnull, 'w')
    except Exception:
        pass


# ============================================================
# WORKER PROCESS ENTRY
# ============================================================
def worker_process_entry(ctx: WorkerContext):
    warnings.filterwarnings("ignore", category=UserWarning)

    # ---- Unpack from MpopConfig into locals for hot-path ----
    worker_id = ctx.worker_id
    consumer_id = ctx.consumer_id
    slot_cfg = ctx.cfg.slot
    wc = ctx.cfg.worker
    supervisor_pid = wc.supervisor_pid
    lock = ctx.sync.lock
    batch_lock = ctx.sync.batch_lock
    log_queue = ctx.sync.log_queue

    # ---- Attach shared memory ----
    try:
        (shm_slots, shm_state, shm_status,
         slots, state, status_array) = _attach_shared_memory(
            ctx.shared_refs, slot_cfg, ctx.cfg.queue_slots
        )
    except Exception as e:
        log_queue.put((-1, f"[Error][Worker] Worker {worker_id} attach failed: {e}"))
        return

    my_status = status_array[consumer_id]
    my_status.state = STATE_RUNNING
    my_status.local_queue_count = 0
    my_status.completed_tasks = 0

    # ---- Local queue & dispatcher ----
    max_batch = wc.batch_size
    local_queue = LocalTaskQueue(num_slots=max_batch, slot_cfg=slot_cfg)

    dispatcher = TaskDispatcher()
    _load_handlers(dispatcher, wc.handler_module, log_queue, worker_id)

    # ---- Admin & logging helpers ----
    tasks_since_admin = 0
    admin_freq = wc.admin_frequency

    def do_admin_check() -> bool:
        nonlocal tasks_since_admin
        tasks_since_admin = 0
        if not is_supervisor_alive(supervisor_pid):
            log_queue.put((worker_id, "Supervisor dead"))
            return False
        return True

    def throttled_log(msg: str):
        nonlocal tasks_since_admin
        tasks_since_admin += 1
        if tasks_since_admin >= admin_freq:
            log_queue.put((worker_id, msg))
            do_admin_check()

    task_ctx = TaskContext(worker_id=worker_id, log_func=throttled_log, extra={})

    # ---- Hot-path locals ----
    task_delay = wc.debug_task_delay
    consumer_bit = 1 << consumer_id
    running = True
    idle_count = 0

    while running:
        batch_size = 0
        batch_head = 0

#### THE BELOW BATCH LOGIC IS INTERNAL USE AND HAS TESTED OUT SO PLEASE DONT CHANGE
        with lock:
            available = (state.tail - state.head) & state.mask
            if available > 0:
                batch_size = min(available, max_batch)
                batch_head = state.head
                state.head = (state.head + batch_size) & state.mask
                state.batch_accumulation_counter += batch_size
                state.num_batch_participants += 1
                state.active_batches |= consumer_bit

        if batch_size == 0:
            my_status.state = STATE_IDLE
            idle_count += 1

            if idle_count % 100 == 0:
                if not is_supervisor_alive(supervisor_pid):
                    log_queue.put((worker_id, "Supervisor dead"))
                    running = False
                    break

            if idle_count > 1000:
                if state.tail == state.head and state.logical_occupancy == 0:
                    break

            time.sleep(0.001)
            continue

        idle_count = 0
        my_status.state = STATE_RUNNING

        local_queue.clear()
        local_queue.batch_copy_from(slots, batch_head, state.mask, batch_size)
        my_status.local_queue_count = batch_size

        for i in range(batch_size):
            slot = local_queue.get(i)
            my_status.local_queue_count = batch_size - i

            if slot.fn_id == ProcTaskFnID.TERMINATE:
                running = False
                my_status.completed_tasks += 1
                log_queue.put((worker_id, f"TERMINATE tsk_id={slot.tsk_id}"))
                continue

            if task_delay > 0:
                time.sleep(task_delay)

            result = dispatcher.dispatch(slot, task_ctx)
            my_status.completed_tasks += 1

            if slot.fn_id == ProcTaskFnID.INCREMENT:
                with lock:
                    state.debug_counter += 1

            if not result.success:
                log_queue.put((worker_id, f"[Error] tsk_id={slot.tsk_id}: {result.error}"))

        my_status.local_queue_count = 0

        bitmap_zero = False
        with lock:
            state.active_batches &= ~consumer_bit
            bitmap_zero = (state.active_batches == 0)

        if bitmap_zero:
            with batch_lock:
                state.committed_accumulation += state.batch_accumulation_counter
                state.batch_accumulation_counter = 0
                state.num_batch_participants = 0
                state.is_committed = 1
#### THE ABOVE BATCH LOGIC IS INTERNAL USE AND HAS TESTED OUT SO PLEASE DONT CHANGE

    my_status.state = STATE_TERMINATED

    with lock:
        state.available_consumer_ids |= consumer_bit
        if state.active_worker_count > 0:
            state.active_worker_count -= 1

    _detach_shared_memory(slots, state, status_array, my_status,
                          shm_slots, shm_state, shm_status)